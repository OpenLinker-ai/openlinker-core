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
	response, err := s.issueOrReplayCredential(c.Request().Context(), token, nodeID, request, csr)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, response)
}

func (s *CredentialService) issueOrReplayCredential(
	ctx context.Context,
	token db.AgentRuntimeToken,
	nodeID uuid.UUID,
	request CredentialRequest,
	csr *x509.CertificateRequest,
) (CredentialResponse, error) {
	if csr == nil || len(csr.RawSubjectPublicKeyInfo) == 0 {
		return CredentialResponse{}, httpx.BadRequest("CSR 无法签发")
	}
	publicKeyDigest := sha256.Sum256(csr.RawSubjectPublicKeyInfo)
	publicKeyThumbprint := hex.EncodeToString(publicKeyDigest[:])

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return CredentialResponse{}, httpx.Internal("签发 Runtime 证书失败")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `
SELECT pg_advisory_xact_lock(hashtextextended('runtime-credential-issuance:' || $1::text, 0))`, token.ID); err != nil {
		return CredentialResponse{}, httpx.Internal("锁定 Runtime 凭证签发失败")
	}

	var boundNodeID uuid.UUID
	var boundThumbprint, bindingMode string
	bindingErr := tx.QueryRow(ctx, `
SELECT node_id, public_key_thumbprint, binding_mode
FROM runtime_node_bindings
WHERE credential_id = $1`, token.ID).Scan(&boundNodeID, &boundThumbprint, &bindingMode)
	if bindingErr != nil && !errors.Is(bindingErr, pgx.ErrNoRows) {
		return CredentialResponse{}, httpx.Internal("查询 Runtime 凭证绑定失败")
	}

	var nodeStatus string
	if bindingErr == nil {
		if err = tx.QueryRow(ctx, `
SELECT status
FROM runtime_nodes
WHERE node_id = $1
FOR UPDATE`, boundNodeID).Scan(&nodeStatus); err != nil {
			return CredentialResponse{}, httpx.Internal("查询 Runtime Node 失败")
		}
	} else {
		if _, err = tx.Exec(ctx, `
SELECT pg_advisory_xact_lock(hashtextextended('runtime-node-enrollment:' || $1::text, 0))`, nodeID); err != nil {
			return CredentialResponse{}, httpx.Internal("锁定 Runtime Node 登记失败")
		}
		var existingStatus string
		err = tx.QueryRow(ctx, `
SELECT status
FROM runtime_nodes
WHERE node_id = $1
FOR UPDATE`, nodeID).Scan(&existingStatus)
		if err == nil {
			return CredentialResponse{}, httpx.NewError(http.StatusConflict, "RUNTIME_NODE_ENROLLMENT_CONFLICT", "Runtime Node 标识或公钥已被使用")
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return CredentialResponse{}, httpx.Internal("查询 Runtime Node 失败")
		}
	}

	var lockedAgentID uuid.UUID
	err = tx.QueryRow(ctx, `
SELECT agent_id
FROM agent_tokens
WHERE id = $1
  AND agent_id = $2
  AND status = 'active_runtime'
  AND revoked_at IS NULL
  AND scopes @> ARRAY['agent:pull']::text[]
  AND (expires_at IS NULL OR expires_at > clock_timestamp())
FOR UPDATE`, token.ID, token.AgentID).Scan(&lockedAgentID)
	if err != nil || lockedAgentID != token.AgentID {
		return CredentialResponse{}, httpx.Unauthorized("Agent Token 无效、已撤销或已过期")
	}

	err = tx.QueryRow(ctx, `
SELECT node_id, public_key_thumbprint, binding_mode
FROM runtime_node_bindings
WHERE credential_id = $1
FOR UPDATE`, token.ID).Scan(&boundNodeID, &boundThumbprint, &bindingMode)
	switch {
	case err == nil:
		if bindingMode != "mtls" || boundNodeID != nodeID || boundThumbprint != publicKeyThumbprint {
			return CredentialResponse{}, httpx.NewError(http.StatusConflict, "RUNTIME_CREDENTIAL_BOUND", "Agent Token 已绑定其他 Runtime Node、公钥或认证模式")
		}
		if nodeStatus == "revoked" {
			return CredentialResponse{}, httpx.NewError(http.StatusForbidden, "RUNTIME_NODE_REVOKED", "Runtime Node 已撤销")
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
			return CredentialResponse{}, httpx.NewError(http.StatusConflict, "RUNTIME_NODE_UPDATE_REJECTED", "Runtime Node 状态不允许续期")
		}
	case errors.Is(err, pgx.ErrNoRows):
		if bindingErr == nil {
			return CredentialResponse{}, httpx.NewError(http.StatusConflict, "RUNTIME_CREDENTIAL_BOUND", "Agent Token 凭证绑定已发生变化")
		}
		issued, issueErr := s.authority.IssueClientCertificate(csr, nodeID)
		if issueErr != nil {
			return CredentialResponse{}, httpx.BadRequest("CSR 无法签发")
		}
		_, err = tx.Exec(ctx, `
INSERT INTO runtime_nodes (
    node_id, display_name, device_certificate_serial,
    device_public_key_thumbprint, node_version, protocol_version,
    runtime_contract_id, runtime_contract_digest, features, capacity
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			nodeID, request.DisplayName, issued.Serial, publicKeyThumbprint,
			request.NodeVersion, request.ProtocolVersion, request.RuntimeContractID,
			request.RuntimeContractDigest, request.Features, request.Capacity)
		if err != nil {
			return CredentialResponse{}, httpx.NewError(http.StatusConflict, "RUNTIME_NODE_ENROLLMENT_CONFLICT", "Runtime Node 标识或公钥已被使用")
		}
		_, err = tx.Exec(ctx, `
INSERT INTO runtime_node_bindings (
    credential_id, node_id, agent_id, public_key_thumbprint, binding_mode
) VALUES ($1,$2,$3,$4,'mtls')`, token.ID, nodeID, token.AgentID, publicKeyThumbprint)
		if err != nil {
			return CredentialResponse{}, httpx.NewError(http.StatusConflict, "RUNTIME_CREDENTIAL_BOUND", "Agent Token 已绑定其他 Runtime Node 或公钥")
		}
		if err = insertRuntimeNodeCertificate(ctx, tx, nodeID, issued); err != nil {
			return CredentialResponse{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return CredentialResponse{}, httpx.Internal("签发 Runtime 证书失败")
		}
		return credentialResponse(token.AgentID, nodeID, issued), nil
	default:
		return CredentialResponse{}, httpx.Internal("查询 Runtime 凭证绑定失败")
	}

	var replayed ClientCertificate
	err = tx.QueryRow(ctx, `
SELECT certificate_pem, certificate_chain_pem, trust_bundle_pem,
       certificate_serial, certificate_fingerprint, public_key_thumbprint,
       not_before, not_after, renew_after
FROM runtime_node_certificates
WHERE node_id = $1
  AND public_key_thumbprint = $2
  AND issued_at > clock_timestamp() - INTERVAL '12 hours'
  AND certificate_pem IS NOT NULL
  AND certificate_chain_pem IS NOT NULL
  AND trust_bundle_pem IS NOT NULL
  AND renew_after IS NOT NULL
ORDER BY issued_at DESC, certificate_serial DESC
LIMIT 1`, nodeID, publicKeyThumbprint).Scan(
		&replayed.CertificatePEM, &replayed.CertificateChainPEM, &replayed.TrustBundlePEM,
		&replayed.Serial, &replayed.FingerprintSHA256, &replayed.PublicKeySHA256,
		&replayed.NotBefore, &replayed.NotAfter, &replayed.RenewAfter,
	)
	if err == nil {
		if err = tx.Commit(ctx); err != nil {
			return CredentialResponse{}, httpx.Internal("重放 Runtime 证书失败")
		}
		return credentialResponse(token.AgentID, nodeID, replayed), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return CredentialResponse{}, httpx.Internal("查询 Runtime 证书重放记录失败")
	}

	issued, err := s.authority.IssueClientCertificate(csr, nodeID)
	if err != nil {
		return CredentialResponse{}, httpx.BadRequest("CSR 无法签发")
	}
	if err = insertRuntimeNodeCertificate(ctx, tx, nodeID, issued); err != nil {
		return CredentialResponse{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return CredentialResponse{}, httpx.Internal("签发 Runtime 证书失败")
	}
	return credentialResponse(token.AgentID, nodeID, issued), nil
}

func insertRuntimeNodeCertificate(ctx context.Context, tx pgx.Tx, nodeID uuid.UUID, issued ClientCertificate) error {
	_, err := tx.Exec(ctx, `
INSERT INTO runtime_node_certificates (
    certificate_serial, node_id, public_key_thumbprint,
    certificate_fingerprint, not_before, not_after,
    certificate_pem, certificate_chain_pem, trust_bundle_pem, renew_after
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, issued.Serial, nodeID, issued.PublicKeySHA256,
		issued.FingerprintSHA256, issued.NotBefore, issued.NotAfter,
		issued.CertificatePEM, issued.CertificateChainPEM, issued.TrustBundlePEM, issued.RenewAfter)
	if err != nil {
		return httpx.Internal("保存 Runtime 证书失败")
	}
	return nil
}

func credentialResponse(agentID, nodeID uuid.UUID, issued ClientCertificate) CredentialResponse {
	return CredentialResponse{
		NodeID:                nodeID.String(),
		AgentID:               agentID.String(),
		CertificatePEM:        issued.CertificatePEM,
		CertificateChainPEM:   issued.CertificateChainPEM,
		TrustBundlePEM:        issued.TrustBundlePEM,
		CertificateSerial:     issued.Serial,
		PublicKeyThumbprint:   issued.PublicKeySHA256,
		NotBefore:             issued.NotBefore.UTC(),
		NotAfter:              issued.NotAfter.UTC(),
		RenewAfter:            issued.RenewAfter.UTC(),
		CertificateLifetimeHr: int(ClientCertificateLifetime / time.Hour),
	}
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
	if request.NodeID == "" {
		return uuid.Nil, nil, httpx.BadRequest("node_id 必填")
	}
	nodeID, err := uuid.Parse(request.NodeID)
	if err != nil || nodeID == uuid.Nil || nodeID.String() != request.NodeID {
		return uuid.Nil, nil, httpx.BadRequest("node_id 不是规范 UUID")
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
