package runtimepki

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const (
	runtimeCredentialScope = "agent:pull"
	maxCSRBytes            = 16 << 10
)

type RuntimeTokenValidator interface {
	ValidateRuntimeToken(context.Context, string, ...string) (db.AgentRuntimeToken, error)
}

type CredentialService struct {
	pool      *pgxpool.Pool
	authority *Manager
	tokens    RuntimeTokenValidator
}

type CredentialRequest struct {
	NodeID                string   `json:"node_id"`
	DisplayName           string   `json:"display_name"`
	NodeVersion           string   `json:"node_version"`
	ProtocolVersion       int32    `json:"protocol_version"`
	RuntimeContractID     string   `json:"runtime_contract_id"`
	RuntimeContractDigest string   `json:"runtime_contract_digest"`
	Features              []string `json:"features"`
	Capacity              int32    `json:"capacity"`
	CSRPEM                string   `json:"csr_pem"`
}

type CredentialResponse struct {
	NodeID                string    `json:"node_id"`
	AgentID               string    `json:"agent_id"`
	CertificatePEM        string    `json:"certificate_pem"`
	CertificateChainPEM   string    `json:"certificate_chain_pem"`
	TrustBundlePEM        string    `json:"trust_bundle_pem"`
	CertificateSerial     string    `json:"certificate_serial"`
	PublicKeyThumbprint   string    `json:"public_key_thumbprint"`
	NotBefore             time.Time `json:"not_before"`
	NotAfter              time.Time `json:"not_after"`
	RenewAfter            time.Time `json:"renew_after"`
	CertificateLifetimeHr int       `json:"certificate_lifetime_hours"`
}

func NewCredentialService(pool *pgxpool.Pool, authority *Manager, tokens RuntimeTokenValidator) *CredentialService {
	return &CredentialService{pool: pool, authority: authority, tokens: tokens}
}

func (s *CredentialService) Register(api *echo.Group) {
	if api == nil {
		return
	}
	api.POST("/runtime-credentials", s.issue)
	api.POST("/runtime-credentials/renew", s.issue)
}

func (s *CredentialService) RegisterTrustBundle(e *echo.Echo) {
	if e == nil {
		return
	}
	e.GET("/.well-known/openlinker-runtime-ca.pem", func(c echo.Context) error {
		if s == nil || s.authority == nil {
			return httpx.ServiceUnavailable("Runtime PKI 不可用")
		}
		c.Response().Header().Set(echo.HeaderContentType, "application/x-pem-file")
		c.Response().Header().Set(echo.HeaderCacheControl, "public, max-age=300")
		return c.String(http.StatusOK, s.authority.TrustBundlePEM())
	})
}

func (s *CredentialService) issue(c echo.Context) error {
	if s == nil || s.pool == nil || s.authority == nil || s.tokens == nil {
		return httpx.ServiceUnavailable("Runtime 自动签发不可用")
	}
	rawToken, err := bearerToken(c.Request().Header.Get(echo.HeaderAuthorization))
	if err != nil {
		return err
	}
	token, err := s.tokens.ValidateRuntimeToken(c.Request().Context(), rawToken, runtimeCredentialScope)
	if err != nil {
		return err
	}
	var request CredentialRequest
	if err = c.Bind(&request); err != nil {
		return httpx.BadRequest("请求体不是合法 JSON")
	}
	nodeID, csr, err := validateCredentialRequest(&request)
	if err != nil {
		return err
	}
	if nodeID == uuid.Nil {
		nodeID = uuid.New()
	}
	issued, err := s.authority.IssueClientCertificate(csr, nodeID)
	if err != nil {
		return httpx.BadRequest("CSR 无法签发")
	}
	if err = s.persistIssuedCredential(c.Request().Context(), token, nodeID, request, issued); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, CredentialResponse{
		NodeID:                nodeID.String(),
		AgentID:               token.AgentID.String(),
		CertificatePEM:        issued.CertificatePEM,
		CertificateChainPEM:   issued.CertificateChainPEM,
		TrustBundlePEM:        issued.TrustBundlePEM,
		CertificateSerial:     issued.Serial,
		PublicKeyThumbprint:   issued.PublicKeySHA256,
		NotBefore:             issued.NotBefore,
		NotAfter:              issued.NotAfter,
		RenewAfter:            issued.RenewAfter,
		CertificateLifetimeHr: int(ClientCertificateLifetime / time.Hour),
	})
}

func (s *CredentialService) persistIssuedCredential(
	ctx context.Context,
	token db.AgentRuntimeToken,
	nodeID uuid.UUID,
	request CredentialRequest,
	issued ClientCertificate,
) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return httpx.Internal("签发 Runtime 证书失败")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var lockedAgentID uuid.UUID
	var status string
	var revokedAt *time.Time
	err = tx.QueryRow(ctx, `
SELECT agent_id, status, revoked_at
FROM agent_tokens
WHERE id = $1
FOR UPDATE`, token.ID).Scan(&lockedAgentID, &status, &revokedAt)
	if err != nil || lockedAgentID != token.AgentID || status != "active_runtime" || revokedAt != nil {
		return httpx.Unauthorized("Agent Token 无效或已撤销")
	}

	var boundNodeID uuid.UUID
	var boundThumbprint, nodeStatus string
	err = tx.QueryRow(ctx, `
SELECT binding.node_id, binding.public_key_thumbprint, node.status
FROM runtime_node_bindings binding
JOIN runtime_nodes node ON node.node_id = binding.node_id
WHERE binding.credential_id = $1
FOR UPDATE OF binding, node`, token.ID).Scan(&boundNodeID, &boundThumbprint, &nodeStatus)
	switch {
	case err == nil:
		if boundNodeID != nodeID || boundThumbprint != issued.PublicKeySHA256 {
			return httpx.NewError(http.StatusConflict, "RUNTIME_CREDENTIAL_BOUND", "Agent Token 已绑定其他 Runtime Node 或公钥")
		}
		if nodeStatus == "revoked" {
			return httpx.NewError(http.StatusForbidden, "RUNTIME_NODE_REVOKED", "Runtime Node 已撤销")
		}
		if _, err = tx.Exec(ctx, `
UPDATE runtime_nodes
SET display_name = $2,
    node_version = $3,
    protocol_version = $4,
    runtime_contract_id = $5,
    runtime_contract_digest = $6,
    features = $7,
    capacity = GREATEST($8, inflight),
    updated_at = clock_timestamp()
WHERE node_id = $1`, nodeID, request.DisplayName, request.NodeVersion,
			request.ProtocolVersion, request.RuntimeContractID, request.RuntimeContractDigest,
			request.Features, request.Capacity); err != nil {
			return httpx.NewError(http.StatusConflict, "RUNTIME_NODE_UPDATE_REJECTED", "Runtime Node 状态不允许续期")
		}
	case errors.Is(err, pgx.ErrNoRows):
		_, err = tx.Exec(ctx, `
INSERT INTO runtime_nodes (
    node_id, display_name, device_certificate_serial,
    device_public_key_thumbprint, node_version, protocol_version,
    runtime_contract_id, runtime_contract_digest, features, capacity
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			nodeID, request.DisplayName, issued.Serial, issued.PublicKeySHA256,
			request.NodeVersion, request.ProtocolVersion, request.RuntimeContractID,
			request.RuntimeContractDigest, request.Features, request.Capacity)
		if err != nil {
			return httpx.NewError(http.StatusConflict, "RUNTIME_NODE_ENROLLMENT_CONFLICT", "Runtime Node 标识或公钥已被使用")
		}
		_, err = tx.Exec(ctx, `
INSERT INTO runtime_node_bindings (
    credential_id, node_id, agent_id, public_key_thumbprint
) VALUES ($1,$2,$3,$4)`, token.ID, nodeID, token.AgentID, issued.PublicKeySHA256)
		if err != nil {
			return httpx.NewError(http.StatusConflict, "RUNTIME_CREDENTIAL_BOUND", "Agent Token 已绑定其他 Runtime Node 或公钥")
		}
	default:
		return httpx.Internal("查询 Runtime 凭证绑定失败")
	}

	_, err = tx.Exec(ctx, `
INSERT INTO runtime_node_certificates (
    certificate_serial, node_id, public_key_thumbprint,
    certificate_fingerprint, not_before, not_after
) VALUES ($1,$2,$3,$4,$5,$6)`, issued.Serial, nodeID, issued.PublicKeySHA256,
		issued.FingerprintSHA256, issued.NotBefore, issued.NotAfter)
	if err != nil {
		return httpx.Internal("保存 Runtime 证书失败")
	}
	if err = tx.Commit(ctx); err != nil {
		return httpx.Internal("签发 Runtime 证书失败")
	}
	return nil
}

func validateCredentialRequest(request *CredentialRequest) (uuid.UUID, *x509.CertificateRequest, error) {
	if request == nil {
		return uuid.Nil, nil, httpx.BadRequest("请求体不能为空")
	}
	request.NodeID = strings.TrimSpace(request.NodeID)
	request.DisplayName = strings.TrimSpace(request.DisplayName)
	request.NodeVersion = strings.TrimSpace(request.NodeVersion)
	request.RuntimeContractID = strings.TrimSpace(request.RuntimeContractID)
	request.RuntimeContractDigest = strings.ToLower(strings.TrimSpace(request.RuntimeContractDigest))
	var nodeID uuid.UUID
	var err error
	if request.NodeID != "" {
		nodeID, err = uuid.Parse(request.NodeID)
		if err != nil || nodeID == uuid.Nil || nodeID.String() != request.NodeID {
			return uuid.Nil, nil, httpx.BadRequest("node_id 不是规范 UUID")
		}
	}
	if !utf8.ValidString(request.DisplayName) || utf8.RuneCountInString(request.DisplayName) < 1 || utf8.RuneCountInString(request.DisplayName) > 200 {
		return uuid.Nil, nil, httpx.BadRequest("display_name 长度必须为 1 到 200")
	}
	if !utf8.ValidString(request.NodeVersion) || utf8.RuneCountInString(request.NodeVersion) < 1 || utf8.RuneCountInString(request.NodeVersion) > 100 {
		return uuid.Nil, nil, httpx.BadRequest("node_version 长度必须为 1 到 100")
	}
	if request.ProtocolVersion != coreruntime.RuntimeProtocolVersion ||
		request.RuntimeContractID != coreruntime.RuntimeContractID ||
		request.RuntimeContractDigest != coreruntime.RuntimeContractDigest ||
		!sameStrings(request.Features, coreruntime.RuntimeRequiredFeatures()) {
		return uuid.Nil, nil, httpx.NewError(http.StatusConflict, "RUNTIME_CONTRACT_UNSUPPORTED", "Runtime contract 不受支持")
	}
	if request.Capacity < 1 || request.Capacity > 1024 {
		return uuid.Nil, nil, httpx.BadRequest("capacity 必须为 1 到 1024")
	}
	if len(request.CSRPEM) == 0 || len(request.CSRPEM) > maxCSRBytes {
		return uuid.Nil, nil, httpx.BadRequest("csr_pem 无效")
	}
	block, rest := pem.Decode([]byte(request.CSRPEM))
	if block == nil || block.Type != "CERTIFICATE REQUEST" || len(strings.TrimSpace(string(rest))) != 0 {
		return uuid.Nil, nil, httpx.BadRequest("csr_pem 无效")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil || csr.CheckSignature() != nil {
		return uuid.Nil, nil, httpx.BadRequest("csr_pem 签名无效")
	}
	spki, err := x509.MarshalPKIXPublicKey(csr.PublicKey)
	if err != nil {
		return uuid.Nil, nil, httpx.BadRequest("CSR 公钥无效")
	}
	if digest := sha256.Sum256(spki); hex.EncodeToString(digest[:]) == strings.Repeat("0", 64) {
		return uuid.Nil, nil, httpx.BadRequest("CSR 公钥无效")
	}
	request.Features = append([]string(nil), request.Features...)
	sort.Strings(request.Features)
	return nodeID, csr, nil
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftCopy := append([]string(nil), left...)
	rightCopy := append([]string(nil), right...)
	sort.Strings(leftCopy)
	sort.Strings(rightCopy)
	for index := range leftCopy {
		if leftCopy[index] != rightCopy[index] {
			return false
		}
	}
	return true
}

func bearerToken(value string) (string, error) {
	parts := strings.Fields(value)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.TrimSpace(parts[1]) == "" {
		return "", httpx.Unauthorized("缺少 Agent Token")
	}
	return parts[1], nil
}
