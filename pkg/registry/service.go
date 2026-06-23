package registry

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/bcrypt"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	nodeSecretPrefix              = "rn_live_"
	nodeSecretPrefixLen           = 12
	nodeSecretRandomSize          = 32
	federationTokenPrefix         = "rf_live_"
	federationTokenPrefixLen      = 12
	federationTokenRandomSize     = 32
	defaultFederationInviteTTL    = 15 * time.Minute
	defaultNodeType               = "bridge_proxy"
	defaultRoutingMode            = "pull_proxy"
	payloadPolicyMetadataOnly     = "metadata_only"
	payloadPolicyStoreRunSummary  = "store_run_summary"
	payloadPolicyStoreFullPayload = "store_full_payload"
	defaultPayloadPolicy          = payloadPolicyMetadataOnly
	maxProxyArtifactDownloadBytes = int64(32 << 20)
)

var defaultNodeScopes = []string{"heartbeat", "listing:sync", "proxy:pull", "proxy:result"}

var proxyArtifactHTTPClient = &http.Client{Timeout: 30 * time.Second}
var remoteRegistryHTTPClient = &http.Client{Timeout: 30 * time.Second}
var errFederationInviteExpired = errors.New("registry federation invite expired")

type Service struct {
	q    *db.Queries
	pool *pgxpool.Pool
}

type registryPeerRow struct {
	ID             uuid.UUID
	OwnerUserID    uuid.UUID
	Name           string
	APIBaseURL     string
	BearerToken    string
	CredentialHint string
	Status         string
	LastUsedAt     *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type registryFederationInviteRow struct {
	ID             uuid.UUID
	OwnerUserID    uuid.UUID
	Name           string
	APIBaseURL     string
	BearerToken    string
	TokenPrefix    string
	TokenHash      string
	CredentialHint string
	Status         string
	ExpiresAt      time.Time
	ConsumedAt     *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{q: db.New(pool), pool: pool}
}

func (s *Service) CreateNode(ctx context.Context, ownerID uuid.UUID, req *CreateNodeRequest) (*RegistryNodeResponse, error) {
	name := strings.TrimSpace(req.NodeName)
	if len([]rune(name)) < 2 || len([]rune(name)) > 120 {
		return nil, httpx.Unprocessable("node_name 长度需在 2-120 字符之间")
	}
	nodeType := strings.TrimSpace(req.NodeType)
	if nodeType == "" {
		nodeType = defaultNodeType
	}
	if nodeType != "self_hosted" && nodeType != "bridge_proxy" {
		return nil, httpx.Unprocessable("node_type 取值非法")
	}
	baseURL, err := normalizeBaseURL(req.BaseURL)
	if err != nil {
		return nil, err
	}
	scopes, err := normalizeScopes(req.Scopes)
	if err != nil {
		return nil, err
	}
	secret, prefix, err := generateNodeSecret()
	if err != nil {
		log.Error().Err(err).Msg("registry.CreateNode: generate secret")
		return nil, httpx.Internal("生成节点密钥失败")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(secret), bcrypt.DefaultCost)
	if err != nil {
		log.Error().Err(err).Msg("registry.CreateNode: hash secret")
		return nil, httpx.Internal("生成节点密钥失败")
	}
	node, err := s.q.CreateRegistryNode(ctx, db.CreateRegistryNodeParams{
		OwnerUserID:  ownerID,
		NodeName:     name,
		NodeType:     nodeType,
		BaseURL:      baseURL,
		SecretPrefix: prefix,
		SecretHash:   string(hash),
		Scopes:       scopes,
	})
	if err != nil {
		log.Error().Err(err).Str("owner_id", ownerID.String()).Msg("registry.CreateNode")
		return nil, httpx.Internal("创建 Registry Node 失败")
	}
	resp := registryNodeToResponse(node)
	resp.NodeSecret = secret
	return &resp, nil
}

func (s *Service) ListNodes(ctx context.Context, ownerID uuid.UUID) ([]RegistryNodeResponse, error) {
	nodes, err := s.q.ListRegistryNodesByOwner(ctx, ownerID)
	if err != nil {
		log.Error().Err(err).Str("owner_id", ownerID.String()).Msg("registry.ListNodes")
		return nil, httpx.Internal("查询 Registry Node 失败")
	}
	out := make([]RegistryNodeResponse, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, registryNodeToResponse(node))
	}
	return out, nil
}

func (s *Service) RevokeNode(ctx context.Context, ownerID, nodeID uuid.UUID) (*RegistryNodeResponse, error) {
	node, err := s.q.RevokeRegistryNodeForOwner(ctx, db.RevokeRegistryNodeForOwnerParams{
		ID:          nodeID,
		OwnerUserID: ownerID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Registry Node 不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("node_id", nodeID.String()).Msg("registry.RevokeNode")
		return nil, httpx.Internal("撤销 Registry Node 失败")
	}
	resp := registryNodeToResponse(node)
	return &resp, nil
}

func (s *Service) RotateNodeSecret(ctx context.Context, ownerID, nodeID uuid.UUID) (*RegistryNodeResponse, error) {
	secret, prefix, err := generateNodeSecret()
	if err != nil {
		log.Error().Err(err).Msg("registry.RotateNodeSecret: generate secret")
		return nil, httpx.Internal("生成节点密钥失败")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(secret), bcrypt.DefaultCost)
	if err != nil {
		log.Error().Err(err).Msg("registry.RotateNodeSecret: hash secret")
		return nil, httpx.Internal("生成节点密钥失败")
	}
	node, err := s.q.RotateRegistryNodeSecretForOwner(ctx, db.RotateRegistryNodeSecretForOwnerParams{
		ID:           nodeID,
		OwnerUserID:  ownerID,
		SecretPrefix: prefix,
		SecretHash:   string(hash),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Registry Node 不存在或已撤销")
	}
	if err != nil {
		log.Error().Err(err).Str("node_id", nodeID.String()).Msg("registry.RotateNodeSecret")
		return nil, httpx.Internal("轮换 Registry Node 密钥失败")
	}
	resp := registryNodeToResponse(node)
	resp.NodeSecret = secret
	return &resp, nil
}

func (s *Service) Heartbeat(ctx context.Context, plaintextSecret string) (*HeartbeatResponse, error) {
	node, err := s.verifyNodeSecret(ctx, plaintextSecret, "heartbeat")
	if err != nil {
		return nil, err
	}
	updated, err := s.q.MarkRegistryNodeHeartbeat(ctx, node.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.Unauthorized("Registry Node 已撤销")
	}
	if err != nil {
		log.Error().Err(err).Str("node_id", node.ID.String()).Msg("registry.Heartbeat")
		return nil, httpx.Internal("记录 Registry Node 心跳失败")
	}
	count, err := s.q.CountCloudListingLinksByNode(ctx, updated.ID)
	if err != nil {
		log.Warn().Err(err).Str("node_id", updated.ID.String()).Msg("registry.Heartbeat: CountCloudListingLinksByNode")
	}
	pendingRuns, err := s.q.CountPendingProxyRunsByNode(ctx, updated.ID)
	if err != nil {
		log.Warn().Err(err).Str("node_id", updated.ID.String()).Msg("registry.Heartbeat: CountPendingProxyRunsByNode")
	}
	return &HeartbeatResponse{
		NodeID:             updated.ID.String(),
		HeartbeatStatus:    updated.HeartbeatStatus,
		LastHeartbeatAt:    timePtrString(updated.LastHeartbeatAt),
		LinkedListingCount: count,
		PendingRunCount:    pendingRuns,
	}, nil
}

func (s *Service) CreateCloudListing(ctx context.Context, ownerID uuid.UUID, req *CreateCloudListingRequest) (*CloudListingLinkResponse, error) {
	nodeID, err := uuid.Parse(strings.TrimSpace(req.RegistryNodeID))
	if err != nil {
		return nil, httpx.BadRequest("registry_node_id 不是合法 uuid")
	}
	agentID, err := uuid.Parse(strings.TrimSpace(req.AgentID))
	if err != nil {
		return nil, httpx.BadRequest("agent_id 不是合法 uuid")
	}
	cloudListingID := uuid.New()
	rawListingID, err := resolveRegistryListingID(req.RegistryListingID, req.CloudListingID, false)
	if err != nil {
		return nil, err
	}
	if rawListingID != "" {
		cloudListingID, err = uuid.Parse(rawListingID)
		if err != nil {
			return nil, httpx.BadRequest("registry_listing_id 不是合法 uuid")
		}
		existing, err := s.q.GetCloudListingLinkForOwner(ctx, db.GetCloudListingLinkForOwnerParams{
			CloudListingID: cloudListingID,
			OwnerUserID:    ownerID,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Registry Listing 不存在")
		}
		if err != nil {
			log.Error().Err(err).Str("cloud_listing_id", cloudListingID.String()).Msg("registry.CreateCloudListing: existing listing")
			return nil, httpx.Internal("查询 Registry Listing 失败")
		}
		if existing.LocalAgentID != agentID {
			return nil, httpx.Conflict("registry_listing_id 已绑定到其它 Agent")
		}
	}
	node, err := s.q.GetRegistryNodeByIDForOwner(ctx, db.GetRegistryNodeByIDForOwnerParams{
		ID:          nodeID,
		OwnerUserID: ownerID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Registry Node 不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("node_id", nodeID.String()).Msg("registry.CreateCloudListing: node")
		return nil, httpx.Internal("查询 Registry Node 失败")
	}
	if node.RevokedAt != nil {
		return nil, httpx.Conflict("Registry Node 已撤销")
	}
	agent, err := s.q.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{
		ID:        agentID,
		CreatorID: ownerID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Agent 不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("registry.CreateCloudListing: agent")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	if agent.LifecycleStatus != "active" {
		return nil, httpx.Conflict("Agent 未启用，不能创建 Registry Listing")
	}
	routingMode := strings.TrimSpace(req.RoutingMode)
	if routingMode == "" {
		routingMode = defaultRoutingMode
	}
	if routingMode != "direct_endpoint" && routingMode != "pull_proxy" {
		return nil, httpx.Unprocessable("routing_mode 取值非法")
	}
	payloadPolicy := strings.TrimSpace(req.PayloadPolicy)
	if payloadPolicy == "" {
		payloadPolicy = defaultPayloadPolicy
	}
	if !validPayloadPolicy(payloadPolicy) {
		return nil, httpx.Unprocessable("payload_policy 取值非法")
	}
	redactionKeys, err := normalizePayloadRedactionKeys(req.PayloadRedactionKeys)
	if err != nil {
		return nil, err
	}
	link, err := s.q.UpsertCloudListingLink(ctx, db.UpsertCloudListingLinkParams{
		CloudListingID:       cloudListingID,
		RegistryNodeID:       nodeID,
		LocalAgentID:         agentID,
		RoutingMode:          routingMode,
		PayloadPolicy:        payloadPolicy,
		PayloadRedactionKeys: redactionKeys,
	})
	if err != nil {
		log.Error().Err(err).Str("node_id", nodeID.String()).Str("agent_id", agentID.String()).
			Msg("registry.CreateCloudListing: upsert")
		return nil, httpx.Internal("创建 Registry Listing 失败")
	}
	if _, err := s.q.SyncCloudListingMetadataForOwner(ctx, db.SyncCloudListingMetadataForOwnerParams{
		CloudListingID: link.CloudListingID,
		OwnerUserID:    ownerID,
	}); err != nil {
		log.Error().Err(err).Str("cloud_listing_id", link.CloudListingID.String()).Msg("registry.CreateCloudListing: sync metadata")
		return nil, httpx.Internal("同步 Registry Listing 元数据失败")
	}
	row, err := s.q.GetCloudListingLinkRowForOwner(ctx, db.GetCloudListingLinkRowForOwnerParams{
		ID:          link.ID,
		OwnerUserID: ownerID,
	})
	if err != nil {
		log.Error().Err(err).Str("cloud_listing_link_id", link.ID.String()).Msg("registry.CreateCloudListing: get row")
		return nil, httpx.Internal("查询 Registry Listing 失败")
	}
	resp := cloudListingRowToResponse(row)
	return &resp, nil
}

func resolveRegistryListingID(canonical, legacy string, required bool) (string, error) {
	canonical = strings.TrimSpace(canonical)
	legacy = strings.TrimSpace(legacy)
	if canonical != "" && legacy != "" && canonical != legacy {
		return "", httpx.BadRequest("registry_listing_id 与 cloud_listing_id 不一致")
	}
	if canonical != "" {
		return canonical, nil
	}
	if legacy != "" {
		return legacy, nil
	}
	if required {
		return "", httpx.BadRequest("registry_listing_id 不能为空")
	}
	return "", nil
}

func (s *Service) ListCloudListings(ctx context.Context, ownerID uuid.UUID) ([]CloudListingLinkResponse, error) {
	rows, err := s.q.ListCloudListingLinksByOwner(ctx, ownerID)
	if err != nil {
		log.Error().Err(err).Str("owner_id", ownerID.String()).Msg("registry.ListCloudListings")
		return nil, httpx.Internal("查询 Registry Listing 失败")
	}
	out := make([]CloudListingLinkResponse, 0, len(rows))
	for _, row := range rows {
		out = append(out, cloudListingRowToResponse(row))
	}
	return out, nil
}

func (s *Service) UpdateCloudListingStatus(ctx context.Context, ownerID, cloudListingID uuid.UUID, req *UpdateCloudListingStatusRequest) (*CloudListingLinkResponse, error) {
	status := strings.TrimSpace(req.SyncStatus)
	if status != "linked" && status != "paused" {
		return nil, httpx.Unprocessable("sync_status 只能是 linked 或 paused")
	}
	row, err := s.q.UpdateCloudListingLinkStatusForOwner(ctx, db.UpdateCloudListingLinkStatusForOwnerParams{
		CloudListingID: cloudListingID,
		OwnerUserID:    ownerID,
		SyncStatus:     status,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Registry Listing 不存在或对应 Registry Node 已撤销")
	}
	if err != nil {
		log.Error().Err(err).Str("cloud_listing_id", cloudListingID.String()).Msg("registry.UpdateCloudListingStatus")
		return nil, httpx.Internal("更新 Registry Listing 状态失败")
	}
	resp := cloudListingRowToResponse(row)
	return &resp, nil
}

func (s *Service) SyncCloudListingMetadata(ctx context.Context, ownerID, cloudListingID uuid.UUID) (*CloudListingLinkResponse, error) {
	row, err := s.q.SyncCloudListingMetadataForOwner(ctx, db.SyncCloudListingMetadataForOwnerParams{
		CloudListingID: cloudListingID,
		OwnerUserID:    ownerID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Registry Listing 不存在或对应 Registry Node 已撤销")
	}
	if err != nil {
		log.Error().Err(err).Str("cloud_listing_id", cloudListingID.String()).Msg("registry.SyncCloudListingMetadata")
		return nil, httpx.Internal("同步 Registry Listing 元数据失败")
	}
	resp := cloudListingRowToResponse(row)
	return &resp, nil
}

func (s *Service) SyncNodeMetadata(ctx context.Context, plaintextSecret string) (*NodeMetadataSyncResponse, error) {
	node, err := s.verifyNodeSecret(ctx, plaintextSecret, "listing:sync")
	if err != nil {
		return nil, err
	}
	count, err := s.q.SyncCloudListingMetadataByNode(ctx, node.ID)
	if err != nil {
		log.Error().Err(err).Str("node_id", node.ID.String()).Msg("registry.SyncNodeMetadata")
		return nil, httpx.Internal("同步 Registry Node 元数据失败")
	}
	return &NodeMetadataSyncResponse{
		RegistryNodeID:     node.ID.String(),
		SyncedListingCount: count,
		SyncedAt:           time.Now().UTC().Format(time.RFC3339),
	}, nil
}

func (s *Service) CreateRegistryPeer(ctx context.Context, ownerID uuid.UUID, req *CreateRegistryPeerRequest) (*RegistryPeerResponse, error) {
	if s.pool == nil {
		return nil, httpx.Internal("Registry Peer 存储未初始化")
	}
	name := strings.TrimSpace(req.Name)
	if len([]rune(name)) < 2 || len([]rune(name)) > 120 {
		return nil, httpx.Unprocessable("name 长度需在 2-120 字符之间")
	}
	apiBaseURL, err := normalizeRemoteAPIBaseURL(req.APIBaseURL)
	if err != nil {
		return nil, err
	}
	token := strings.TrimSpace(req.BearerToken)
	if len([]rune(token)) < 8 || len([]rune(token)) > 4096 {
		return nil, httpx.Unprocessable("bearer_token 长度需在 8-4096 字符之间")
	}
	status := strings.TrimSpace(req.InitialStatus)
	if status == "" {
		status = "active"
	}
	if status != "active" && status != "paused" {
		return nil, httpx.Unprocessable("initial_status 只能是 active 或 paused")
	}
	row, err := scanRegistryPeer(s.pool.QueryRow(ctx, `
		INSERT INTO registry_peers (owner_user_id, name, api_base_url, bearer_token, credential_hint, status)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, owner_user_id, name, api_base_url, bearer_token, credential_hint, status,
		          last_used_at, created_at, updated_at
	`, ownerID, name, apiBaseURL, token, registryCredentialHint(token), status))
	if err != nil {
		log.Error().Err(err).Str("owner_id", ownerID.String()).Str("api_base_url", apiBaseURL).Msg("registry.CreateRegistryPeer")
		return nil, httpx.Internal("创建 Registry Peer 失败")
	}
	resp := registryPeerToResponse(row)
	return &resp, nil
}

func (s *Service) ListRegistryPeers(ctx context.Context, ownerID uuid.UUID) ([]RegistryPeerResponse, error) {
	if s.pool == nil {
		return nil, httpx.Internal("Registry Peer 存储未初始化")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, owner_user_id, name, api_base_url, bearer_token, credential_hint, status,
		       last_used_at, created_at, updated_at
		FROM registry_peers
		WHERE owner_user_id = $1
		ORDER BY created_at DESC
	`, ownerID)
	if err != nil {
		log.Error().Err(err).Str("owner_id", ownerID.String()).Msg("registry.ListRegistryPeers")
		return nil, httpx.Internal("查询 Registry Peer 失败")
	}
	defer rows.Close()
	out := []RegistryPeerResponse{}
	for rows.Next() {
		row, err := scanRegistryPeer(rows)
		if err != nil {
			log.Error().Err(err).Str("owner_id", ownerID.String()).Msg("registry.ListRegistryPeers: scan")
			return nil, httpx.Internal("查询 Registry Peer 失败")
		}
		out = append(out, registryPeerToResponse(row))
	}
	if err := rows.Err(); err != nil {
		log.Error().Err(err).Str("owner_id", ownerID.String()).Msg("registry.ListRegistryPeers: rows")
		return nil, httpx.Internal("查询 Registry Peer 失败")
	}
	return out, nil
}

func (s *Service) DeleteRegistryPeer(ctx context.Context, ownerID, peerID uuid.UUID) error {
	if s.pool == nil {
		return httpx.Internal("Registry Peer 存储未初始化")
	}
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM registry_peers
		WHERE id = $1 AND owner_user_id = $2
	`, peerID, ownerID)
	if err != nil {
		log.Error().Err(err).Str("peer_id", peerID.String()).Msg("registry.DeleteRegistryPeer")
		return httpx.Internal("删除 Registry Peer 失败")
	}
	if tag.RowsAffected() == 0 {
		return httpx.NotFound("Registry Peer 不存在")
	}
	return nil
}

func (s *Service) CreateRegistryFederationInvite(ctx context.Context, ownerID uuid.UUID, req *CreateRegistryFederationInviteRequest) (*RegistryFederationInviteResponse, error) {
	if s.pool == nil {
		return nil, httpx.Internal("Registry Federation 存储未初始化")
	}
	name := strings.TrimSpace(req.Name)
	if len([]rune(name)) < 2 || len([]rune(name)) > 120 {
		return nil, httpx.Unprocessable("name 长度需在 2-120 字符之间")
	}
	apiBaseURL, err := normalizeRemoteAPIBaseURL(req.APIBaseURL)
	if err != nil {
		return nil, err
	}
	tokenForPeer := strings.TrimSpace(req.BearerToken)
	if len([]rune(tokenForPeer)) < 8 || len([]rune(tokenForPeer)) > 4096 {
		return nil, httpx.Unprocessable("bearer_token 长度需在 8-4096 字符之间")
	}
	ttl := defaultFederationInviteTTL
	if req.ExpiresInSeconds > 0 {
		ttl = time.Duration(req.ExpiresInSeconds) * time.Second
	}
	if ttl < time.Minute || ttl > 7*24*time.Hour {
		return nil, httpx.Unprocessable("expires_in_seconds 需在 60-604800 之间")
	}
	federationToken, tokenPrefix, err := generateFederationToken()
	if err != nil {
		log.Error().Err(err).Msg("registry.CreateRegistryFederationInvite: generate token")
		return nil, httpx.Internal("生成 Federation Token 失败")
	}
	tokenHash, err := bcrypt.GenerateFromPassword([]byte(federationToken), bcrypt.DefaultCost)
	if err != nil {
		log.Error().Err(err).Msg("registry.CreateRegistryFederationInvite: hash token")
		return nil, httpx.Internal("生成 Federation Token 失败")
	}
	row, err := scanRegistryFederationInvite(s.pool.QueryRow(ctx, `
		INSERT INTO registry_federation_invites (
		    owner_user_id, name, api_base_url, bearer_token,
		    token_prefix, token_hash, credential_hint, expires_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW() + ($8::int * INTERVAL '1 second'))
		RETURNING id, owner_user_id, name, api_base_url, bearer_token,
		          token_prefix, token_hash, credential_hint, status,
		          expires_at, consumed_at, created_at, updated_at
	`, ownerID, name, apiBaseURL, tokenForPeer, tokenPrefix, string(tokenHash), registryCredentialHint(tokenForPeer), int32(ttl.Seconds())))
	if err != nil {
		log.Error().Err(err).Str("owner_id", ownerID.String()).Str("api_base_url", apiBaseURL).Msg("registry.CreateRegistryFederationInvite")
		return nil, httpx.Internal("创建 Registry Federation Invite 失败")
	}
	resp := registryFederationInviteToResponse(row, true)
	resp.FederationToken = federationToken
	return &resp, nil
}

func (s *Service) ConsumeRegistryFederationInvite(ctx context.Context, req *ConsumeRegistryFederationInviteRequest) (*RegistryFederationExchangeMaterial, error) {
	if s.pool == nil {
		return nil, httpx.Internal("Registry Federation 存储未初始化")
	}
	token := strings.TrimSpace(req.FederationToken)
	if !strings.HasPrefix(token, federationTokenPrefix) || len(token) < federationTokenPrefixLen {
		return nil, httpx.Unauthorized("Federation Token 无效或已使用")
	}
	prefix := token[:federationTokenPrefixLen]
	var matched registryFederationInviteRow
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, owner_user_id, name, api_base_url, bearer_token,
			       token_prefix, token_hash, credential_hint, status,
			       expires_at, consumed_at, created_at, updated_at
			FROM registry_federation_invites
			WHERE token_prefix = $1 AND status = 'active'
			FOR UPDATE
		`, prefix)
		if err != nil {
			return err
		}
		candidates := []registryFederationInviteRow{}
		for rows.Next() {
			row, scanErr := scanRegistryFederationInvite(rows)
			if scanErr != nil {
				rows.Close()
				return scanErr
			}
			candidates = append(candidates, row)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for _, row := range candidates {
			if bcrypt.CompareHashAndPassword([]byte(row.TokenHash), []byte(token)) == nil {
				matched = row
				break
			}
		}
		if matched.ID == uuid.Nil {
			return pgx.ErrNoRows
		}
		if time.Now().After(matched.ExpiresAt) {
			_, _ = tx.Exec(ctx, `
				UPDATE registry_federation_invites
				SET status = 'expired'
				WHERE id = $1 AND status = 'active'
			`, matched.ID)
			return errFederationInviteExpired
		}
		tag, err := tx.Exec(ctx, `
			UPDATE registry_federation_invites
			SET status = 'consumed', consumed_at = NOW()
			WHERE id = $1 AND status = 'active'
		`, matched.ID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return pgx.ErrNoRows
		}
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.Unauthorized("Federation Token 无效或已使用")
	}
	if errors.Is(err, errFederationInviteExpired) {
		return nil, httpx.Unauthorized("Federation Token 已过期")
	}
	if err != nil {
		log.Error().Err(err).Str("token_prefix", prefix).Msg("registry.ConsumeRegistryFederationInvite")
		return nil, httpx.Internal("交换 Registry Federation Token 失败")
	}
	return &RegistryFederationExchangeMaterial{
		Name:           matched.Name,
		APIBaseURL:     matched.APIBaseURL,
		BearerToken:    matched.BearerToken,
		CredentialHint: matched.CredentialHint,
		ExpiresAt:      matched.ExpiresAt.UTC().Format(time.RFC3339),
	}, nil
}

func (s *Service) ExchangeRegistryFederationInvite(ctx context.Context, ownerID uuid.UUID, req *ExchangeRegistryFederationInviteRequest) (*RegistryFederationExchangeResponse, error) {
	exchangeURL, err := normalizeFederationExchangeURL(req.ExchangeURL)
	if err != nil {
		return nil, err
	}
	token := strings.TrimSpace(req.FederationToken)
	if len([]rune(token)) < 8 || len([]rune(token)) > 4096 {
		return nil, httpx.Unprocessable("federation_token 长度需在 8-4096 字符之间")
	}
	body, err := json.Marshal(ConsumeRegistryFederationInviteRequest{FederationToken: token})
	if err != nil {
		return nil, httpx.Internal("序列化 federation exchange 请求失败")
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, exchangeURL, bytes.NewReader(body))
	if err != nil {
		return nil, httpx.Unprocessable("exchange_url 不是合法 URL")
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := remoteRegistryHTTPClient.Do(httpReq)
	if err != nil {
		log.Warn().Err(err).Str("exchange_url", exchangeURL).Msg("registry.ExchangeRegistryFederationInvite: request")
		return nil, httpx.ServiceUnavailable("远端 Registry Federation Exchange 不可用")
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Warn().Int("status", resp.StatusCode).Str("exchange_url", exchangeURL).Str("body", string(raw)).Msg("registry.ExchangeRegistryFederationInvite: status")
		return nil, httpx.ServiceUnavailable("远端 Registry Federation Exchange 失败")
	}
	var material RegistryFederationExchangeMaterial
	if err := json.Unmarshal(raw, &material); err != nil {
		log.Warn().Err(err).Str("exchange_url", exchangeURL).Msg("registry.ExchangeRegistryFederationInvite: decode")
		return nil, httpx.ServiceUnavailable("远端 Registry Federation Exchange 响应格式错误")
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = material.Name
	}
	peer, err := s.CreateRegistryPeer(ctx, ownerID, &CreateRegistryPeerRequest{
		Name:          name,
		APIBaseURL:    material.APIBaseURL,
		BearerToken:   material.BearerToken,
		InitialStatus: req.InitialStatus,
	})
	if err != nil {
		return nil, err
	}
	return &RegistryFederationExchangeResponse{
		Peer:                 *peer,
		ExchangeURL:          exchangeURL,
		RemoteCredentialHint: material.CredentialHint,
	}, nil
}

func (s *Service) CreateProxyRun(ctx context.Context, requestingUserID uuid.UUID, req *CreateProxyRunRequest) (*ProxyRunResponse, error) {
	rawListingID, err := resolveRegistryListingID(req.RegistryListingID, req.CloudListingID, true)
	if err != nil {
		return nil, err
	}
	cloudListingID, err := uuid.Parse(rawListingID)
	if err != nil {
		return nil, httpx.BadRequest("registry_listing_id 不是合法 uuid")
	}
	idempotencyKey := strings.TrimSpace(req.IdempotencyKey)
	if idempotencyKey == "" {
		idempotencyKey = uuid.NewString()
	}
	if len([]rune(idempotencyKey)) < 8 || len([]rune(idempotencyKey)) > 160 {
		return nil, httpx.Unprocessable("idempotency_key 长度需在 8-160 字符之间")
	}
	input, err := marshalJSONObj(req.Input)
	if err != nil {
		return nil, httpx.BadRequest("input 必须是合法 JSON 对象")
	}
	inputSummary, err := optionalText(req.InputSummary, 500, "input_summary")
	if err != nil {
		return nil, err
	}
	link, err := s.q.GetCloudListingLinkForProxyRun(ctx, cloudListingID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Registry Listing 不存在、未 linked 或对应 Registry Node 已撤销")
	}
	if err != nil {
		log.Error().Err(err).Str("cloud_listing_id", cloudListingID.String()).Msg("registry.CreateProxyRun: link")
		return nil, httpx.Internal("查询 Registry Listing 失败")
	}
	storedInput, storedInputSummary := applyInputPayloadPolicy(link.PayloadPolicy, input, inputSummary, link.PayloadRedactionKeys)
	run, err := s.q.CreateProxyRun(ctx, db.CreateProxyRunParams{
		CloudListingID:     cloudListingID,
		CloudListingLinkID: link.ID,
		RequestingUserID:   requestingUserID,
		IdempotencyKey:     idempotencyKey,
		Input:              storedInput,
		InputSummary:       storedInputSummary,
		NodeInput:          input,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Registry Listing 不存在、未 linked 或对应 Registry Node 已撤销")
	}
	if err != nil {
		log.Error().Err(err).Str("cloud_listing_id", cloudListingID.String()).Msg("registry.CreateProxyRun")
		return nil, httpx.Internal("创建 Proxy Run 失败")
	}
	resp := proxyRunToResponse(run)
	return &resp, nil
}

func (s *Service) CreateRemoteProxyRun(ctx context.Context, requestingUserID uuid.UUID, req *CreateRemoteProxyRunRequest) (*RemoteProxyRunResponse, error) {
	remoteRoot, token, registryPeerID, routeMode, err := s.resolveRemoteRegistryCredentials(ctx, requestingUserID, req)
	if err != nil {
		return nil, err
	}
	rawRemoteListingID, err := resolveRegistryListingID(req.RemoteRegistryListingID, req.RemoteCloudListingID, true)
	if err != nil {
		return nil, err
	}
	remoteCloudListingID, err := uuid.Parse(rawRemoteListingID)
	if err != nil {
		return nil, httpx.BadRequest("remote_registry_listing_id 不是合法 uuid")
	}
	idempotencyKey := strings.TrimSpace(req.IdempotencyKey)
	if idempotencyKey == "" {
		idempotencyKey = uuid.NewString()
	}
	if len([]rune(idempotencyKey)) < 8 || len([]rune(idempotencyKey)) > 160 {
		return nil, httpx.Unprocessable("idempotency_key 长度需在 8-160 字符之间")
	}
	inputSummary, err := optionalText(req.InputSummary, 500, "input_summary")
	if err != nil {
		return nil, err
	}

	payload := map[string]any{
		"registry_listing_id": remoteCloudListingID.String(),
		"cloud_listing_id":    remoteCloudListingID.String(),
		"idempotency_key":     idempotencyKey,
		"input":               req.Input,
	}
	if inputSummary != nil {
		payload["input_summary"] = *inputSummary
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, httpx.Internal("创建跨 Registry 路由请求失败")
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, remoteRoot+"/proxy/runs", bytes.NewReader(body))
	if err != nil {
		return nil, httpx.Internal("创建跨 Registry 路由请求失败")
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("User-Agent", "OpenLinker-Cross-Registry-Router/1.0")

	httpResp, err := remoteRegistryHTTPClient.Do(httpReq)
	if err != nil {
		log.Warn().Err(err).Str("remote_api_base_url", remoteRoot).Msg("registry.CreateRemoteProxyRun: request")
		return nil, httpx.ServiceUnavailable("远端 Registry 暂不可用")
	}
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(httpResp.Body, 1<<20))
	if err != nil {
		return nil, httpx.ServiceUnavailable("读取远端 Registry 响应失败")
	}
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		msg := strings.TrimSpace(string(respBody))
		if len(msg) > 300 {
			msg = msg[:300]
		}
		if msg == "" {
			msg = http.StatusText(httpResp.StatusCode)
		}
		return nil, httpx.NewError(http.StatusBadGateway, httpx.CodeServiceUnavailable, "远端 Registry 创建 Proxy Run 失败: "+msg)
	}
	var remoteRun ProxyRunResponse
	if err := json.Unmarshal(respBody, &remoteRun); err != nil {
		log.Warn().Err(err).Str("remote_api_base_url", remoteRoot).Msg("registry.CreateRemoteProxyRun: decode")
		return nil, httpx.ServiceUnavailable("远端 Registry 响应格式无法识别")
	}
	remoteListingID := remoteRun.RegistryListingID
	if remoteListingID == "" {
		remoteListingID = remoteRun.CloudListingID
	}
	if remoteRun.ID == "" || remoteListingID == "" {
		return nil, httpx.ServiceUnavailable("远端 Registry 响应缺少 Proxy Run 标识")
	}
	return &RemoteProxyRunResponse{
		RemoteAPIBaseURL: remoteRoot,
		RegistryPeerID:   registryPeerID,
		RouteMode:        routeMode,
		RemoteRun:        remoteRun,
	}, nil
}

func (s *Service) resolveRemoteRegistryCredentials(ctx context.Context, requestingUserID uuid.UUID, req *CreateRemoteProxyRunRequest) (string, string, string, string, error) {
	peerIDRaw := strings.TrimSpace(req.RegistryPeerID)
	if peerIDRaw == "" {
		if strings.TrimSpace(req.RemoteAPIBaseURL) == "" && strings.TrimSpace(req.RemoteBearerToken) == "" {
			row, err := s.resolveSingleActiveRegistryPeer(ctx, requestingUserID)
			if err != nil {
				return "", "", "", "", err
			}
			if err := s.markRegistryPeerUsed(ctx, row.ID, requestingUserID); err != nil {
				log.Warn().Err(err).Str("peer_id", row.ID.String()).Msg("registry.resolveRemoteRegistryCredentials: mark auto peer used")
			}
			return row.APIBaseURL, row.BearerToken, row.ID.String(), "registry_peer_auto", nil
		}
		remoteRoot, err := normalizeRemoteAPIBaseURL(req.RemoteAPIBaseURL)
		if err != nil {
			return "", "", "", "", err
		}
		token := strings.TrimSpace(req.RemoteBearerToken)
		if len([]rune(token)) < 8 || len([]rune(token)) > 4096 {
			return "", "", "", "", httpx.Unprocessable("remote_bearer_token 长度需在 8-4096 字符之间")
		}
		return remoteRoot, token, "", "explicit", nil
	}
	if strings.TrimSpace(req.RemoteAPIBaseURL) != "" || strings.TrimSpace(req.RemoteBearerToken) != "" {
		return "", "", "", "", httpx.Unprocessable("使用 registry_peer_id 时不要同时传 remote_api_base_url 或 remote_bearer_token")
	}
	if s.pool == nil {
		return "", "", "", "", httpx.Internal("Registry Peer 存储未初始化")
	}
	peerID, err := uuid.Parse(peerIDRaw)
	if err != nil {
		return "", "", "", "", httpx.BadRequest("registry_peer_id 不是合法 uuid")
	}
	row, err := scanRegistryPeer(s.pool.QueryRow(ctx, `
		SELECT id, owner_user_id, name, api_base_url, bearer_token, credential_hint, status,
		       last_used_at, created_at, updated_at
		FROM registry_peers
		WHERE id = $1 AND owner_user_id = $2 AND status = 'active'
	`, peerID, requestingUserID))
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", "", "", httpx.NotFound("Registry Peer 不存在或已暂停")
	}
	if err != nil {
		log.Error().Err(err).Str("peer_id", peerID.String()).Msg("registry.resolveRemoteRegistryCredentials")
		return "", "", "", "", httpx.Internal("查询 Registry Peer 失败")
	}
	if err := s.markRegistryPeerUsed(ctx, peerID, requestingUserID); err != nil {
		log.Warn().Err(err).Str("peer_id", peerID.String()).Msg("registry.resolveRemoteRegistryCredentials: mark used")
	}
	return row.APIBaseURL, row.BearerToken, row.ID.String(), "registry_peer", nil
}

func (s *Service) resolveSingleActiveRegistryPeer(ctx context.Context, ownerID uuid.UUID) (registryPeerRow, error) {
	if s.pool == nil {
		return registryPeerRow{}, httpx.Internal("Registry Peer 存储未初始化")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, owner_user_id, name, api_base_url, bearer_token, credential_hint, status,
		       last_used_at, created_at, updated_at
		FROM registry_peers
		WHERE owner_user_id = $1 AND status = 'active'
		ORDER BY last_used_at ASC NULLS FIRST, created_at ASC
		LIMIT 2
	`, ownerID)
	if err != nil {
		log.Error().Err(err).Str("owner_id", ownerID.String()).Msg("registry.resolveSingleActiveRegistryPeer")
		return registryPeerRow{}, httpx.Internal("查询 Registry Peer 失败")
	}
	defer rows.Close()
	peers := []registryPeerRow{}
	for rows.Next() {
		row, err := scanRegistryPeer(rows)
		if err != nil {
			log.Error().Err(err).Str("owner_id", ownerID.String()).Msg("registry.resolveSingleActiveRegistryPeer: scan")
			return registryPeerRow{}, httpx.Internal("查询 Registry Peer 失败")
		}
		peers = append(peers, row)
	}
	if err := rows.Err(); err != nil {
		log.Error().Err(err).Str("owner_id", ownerID.String()).Msg("registry.resolveSingleActiveRegistryPeer: rows")
		return registryPeerRow{}, httpx.Internal("查询 Registry Peer 失败")
	}
	switch len(peers) {
	case 0:
		return registryPeerRow{}, httpx.Unprocessable("未提供 registry_peer_id 或远端凭证，且没有 active Registry Peer 可用于自动路由")
	case 1:
		return peers[0], nil
	default:
		return registryPeerRow{}, httpx.Conflict("存在多个 active Registry Peer，请传 registry_peer_id 明确选择")
	}
}

func (s *Service) markRegistryPeerUsed(ctx context.Context, peerID, ownerID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE registry_peers
		SET last_used_at = NOW()
		WHERE id = $1 AND owner_user_id = $2
	`, peerID, ownerID)
	return err
}

func (s *Service) ExpireStaleProxyRuns(ctx context.Context, staleAfter time.Duration) (int32, error) {
	if staleAfter < 0 {
		staleAfter = 0
	}
	total, err := s.q.TimeoutStaleProxyRuns(ctx, time.Now().Add(-staleAfter))
	if err != nil {
		log.Error().Err(err).Dur("stale_after", staleAfter).Msg("registry.ExpireStaleProxyRuns")
		return 0, httpx.Internal("处理超时 Proxy Run 失败")
	}
	return total, nil
}

func (s *Service) GetProxyRun(ctx context.Context, requestingUserID, runID uuid.UUID) (*ProxyRunResponse, error) {
	run, err := s.q.GetProxyRunForRequester(ctx, db.GetProxyRunForRequesterParams{
		ID:               runID,
		RequestingUserID: requestingUserID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Proxy Run 不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("registry.GetProxyRun")
		return nil, httpx.Internal("查询 Proxy Run 失败")
	}
	resp := proxyRunToResponse(run)
	return &resp, nil
}

func (s *Service) ClaimProxyRun(ctx context.Context, plaintextSecret string) (*ProxyRunResponse, error) {
	node, err := s.verifyNodeSecret(ctx, plaintextSecret, "proxy:pull")
	if err != nil {
		return nil, err
	}
	run, err := s.q.ClaimPendingProxyRun(ctx, node.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		log.Error().Err(err).Str("node_id", node.ID.String()).Msg("registry.ClaimProxyRun")
		return nil, httpx.Internal("认领 Proxy Run 失败")
	}
	resp := proxyRunToResponse(run)
	return &resp, nil
}

func (s *Service) CompleteProxyRun(ctx context.Context, plaintextSecret string, runID uuid.UUID, req *CompleteProxyRunRequest) (*ProxyRunResponse, error) {
	node, err := s.verifyNodeSecret(ctx, plaintextSecret, "proxy:result")
	if err != nil {
		return nil, err
	}
	status := strings.TrimSpace(req.Status)
	if status != "success" && status != "failed" && status != "timeout" {
		return nil, httpx.Unprocessable("status 只能是 success、failed 或 timeout")
	}
	output, err := marshalJSONObj(req.Output)
	if err != nil {
		return nil, httpx.BadRequest("output 必须是合法 JSON 对象")
	}
	outputSummary, err := optionalText(req.OutputSummary, 1000, "output_summary")
	if err != nil {
		return nil, err
	}
	errorCode, err := optionalText(req.ErrorCode, 80, "error_code")
	if err != nil {
		return nil, err
	}
	errorMessage, err := optionalText(req.ErrorMessage, 1000, "error_message")
	if err != nil {
		return nil, err
	}
	if status == "success" {
		errorCode = nil
		errorMessage = nil
	}
	retryAfter := req.RetryAfterSec
	if retryAfter < 0 || retryAfter > 3600 {
		return nil, httpx.Unprocessable("retry_after_seconds 需在 0-3600 之间")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("registry.CompleteProxyRun: begin")
		return nil, httpx.Internal("回写 Proxy Run 结果失败")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := s.q.WithTx(tx)

	existing, err := q.GetProxyRunForNode(ctx, db.GetProxyRunForNodeParams{
		ID:             runID,
		RegistryNodeID: node.ID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Proxy Run 不存在、已完成或不属于该 Registry Node")
	}
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Str("node_id", node.ID.String()).Msg("registry.CompleteProxyRun: get policy")
		return nil, httpx.Internal("查询 Proxy Run 失败")
	}
	storedOutput, storedOutputSummary := applyOutputPayloadPolicy(existing.PayloadPolicy, output, outputSummary, status, existing.PayloadRedactionKeys)
	run, err := q.CompleteProxyRun(ctx, db.CompleteProxyRunParams{
		ID:             runID,
		RegistryNodeID: node.ID,
		Status:         status,
		Output:         storedOutput,
		OutputSummary:  storedOutputSummary,
		ErrorCode:      errorCode,
		ErrorMessage:   errorMessage,
		Retryable:      req.Retryable,
		RetryAfterSecs: retryAfter,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Proxy Run 不存在、已完成或不属于该 Registry Node")
	}
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Str("node_id", node.ID.String()).Msg("registry.CompleteProxyRun")
		return nil, httpx.Internal("回写 Proxy Run 结果失败")
	}
	if err := q.DeleteProxyRunArtifacts(ctx, run.ID); err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("registry.CompleteProxyRun: delete artifacts")
		return nil, httpx.Internal("更新 Proxy Run 产物失败")
	}
	if run.Status == "success" {
		for _, artifact := range extractProxyRunArtifacts(output, run.PayloadPolicy) {
			if _, err := q.CreateProxyRunArtifact(ctx, db.CreateProxyRunArtifactParams{
				ProxyRunID:       run.ID,
				CloudRunID:       run.CloudRunID,
				SourceArtifactID: artifact.SourceArtifactID,
				ArtifactType:     artifact.ArtifactType,
				Title:            artifact.Title,
				Content:          artifact.Content,
				MimeType:         artifact.MimeType,
				FileURI:          artifact.FileURI,
				FileName:         artifact.FileName,
				FileSHA256:       artifact.FileSHA256,
				FileSizeBytes:    artifact.FileSizeBytes,
			}); err != nil {
				log.Error().Err(err).Str("run_id", runID.String()).Str("source_artifact_id", artifact.SourceArtifactID).
					Msg("registry.CompleteProxyRun: create artifact")
				return nil, httpx.Internal("更新 Proxy Run 产物失败")
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("registry.CompleteProxyRun: commit")
		return nil, httpx.Internal("回写 Proxy Run 结果失败")
	}
	resp := proxyRunToResponse(run)
	return &resp, nil
}

func (s *Service) ListProxyRunArtifacts(ctx context.Context, requestingUserID, runID uuid.UUID) ([]ProxyRunArtifactResponse, error) {
	if _, err := s.q.GetProxyRunForRequester(ctx, db.GetProxyRunForRequesterParams{
		ID:               runID,
		RequestingUserID: requestingUserID,
	}); errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Proxy Run 不存在")
	} else if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("registry.ListProxyRunArtifacts: run")
		return nil, httpx.Internal("查询 Proxy Run 失败")
	}
	rows, err := s.q.ListProxyRunArtifactsForRequester(ctx, db.ListProxyRunArtifactsForRequesterParams{
		ProxyRunID:       runID,
		RequestingUserID: requestingUserID,
	})
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("registry.ListProxyRunArtifacts")
		return nil, httpx.Internal("查询 Proxy Run 产物失败")
	}
	items := make([]ProxyRunArtifactResponse, 0, len(rows))
	for _, row := range rows {
		items = append(items, proxyRunArtifactToResponse(row))
	}
	return items, nil
}

func (s *Service) DownloadProxyRunArtifact(ctx context.Context, requestingUserID, runID, artifactID uuid.UUID) (*ProxyRunArtifactDownload, error) {
	artifact, err := s.q.GetProxyRunArtifactForRequester(ctx, db.GetProxyRunArtifactForRequesterParams{
		ID:               artifactID,
		ProxyRunID:       runID,
		RequestingUserID: requestingUserID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Proxy Run Artifact 不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Str("artifact_id", artifactID.String()).
			Msg("registry.DownloadProxyRunArtifact: artifact")
		return nil, httpx.Internal("查询 Proxy Run 产物失败")
	}
	if artifact.FileURI == nil || strings.TrimSpace(*artifact.FileURI) == "" {
		return nil, httpx.Unprocessable("该产物没有可代理下载的 file_uri")
	}
	fileURL, err := url.Parse(strings.TrimSpace(*artifact.FileURI))
	if err != nil || fileURL.Scheme == "" || fileURL.Host == "" {
		return nil, httpx.Unprocessable("该产物 file_uri 不是合法 URL")
	}
	if fileURL.Scheme != "http" && fileURL.Scheme != "https" {
		return nil, httpx.Unprocessable("该产物 file_uri 仅支持 http/https")
	}
	if artifact.FileSizeBytes != nil && *artifact.FileSizeBytes > maxProxyArtifactDownloadBytes {
		return nil, httpx.NewError(http.StatusRequestEntityTooLarge, httpx.CodeUnprocessable, "产物超过代理下载大小限制")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL.String(), nil)
	if err != nil {
		return nil, httpx.Internal("创建产物下载请求失败")
	}
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", "OpenLinker-Artifact-Proxy/1.0")
	resp, err := proxyArtifactHTTPClient.Do(req)
	if err != nil {
		log.Warn().Err(err).Str("artifact_id", artifactID.String()).Str("file_uri", fileURL.String()).
			Msg("registry.DownloadProxyRunArtifact: fetch")
		return nil, httpx.ServiceUnavailable("产物源文件暂不可用")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, httpx.ServiceUnavailable("产物源文件返回非成功状态")
	}
	if resp.ContentLength > maxProxyArtifactDownloadBytes {
		return nil, httpx.NewError(http.StatusRequestEntityTooLarge, httpx.CodeUnprocessable, "产物超过代理下载大小限制")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxProxyArtifactDownloadBytes+1))
	if err != nil {
		log.Warn().Err(err).Str("artifact_id", artifactID.String()).Msg("registry.DownloadProxyRunArtifact: read")
		return nil, httpx.ServiceUnavailable("读取产物源文件失败")
	}
	if int64(len(body)) > maxProxyArtifactDownloadBytes {
		return nil, httpx.NewError(http.StatusRequestEntityTooLarge, httpx.CodeUnprocessable, "产物超过代理下载大小限制")
	}
	if artifact.FileSizeBytes != nil && int64(len(body)) != *artifact.FileSizeBytes {
		return nil, httpx.ServiceUnavailable("产物源文件大小与声明不一致")
	}
	sum := sha256.Sum256(body)
	actualSHA := hex.EncodeToString(sum[:])
	if artifact.FileSHA256 != nil && *artifact.FileSHA256 != "" && !strings.EqualFold(*artifact.FileSHA256, actualSHA) {
		return nil, httpx.ServiceUnavailable("产物源文件 SHA256 与声明不一致")
	}
	contentType := "application/octet-stream"
	if artifact.MimeType != nil && strings.TrimSpace(*artifact.MimeType) != "" {
		contentType = strings.TrimSpace(*artifact.MimeType)
	} else if upstreamType := strings.TrimSpace(resp.Header.Get("Content-Type")); upstreamType != "" {
		contentType = upstreamType
	}
	fileName := artifact.SourceArtifactID
	if artifact.FileName != nil && strings.TrimSpace(*artifact.FileName) != "" {
		fileName = strings.TrimSpace(*artifact.FileName)
	}
	return &ProxyRunArtifactDownload{
		ArtifactID:  artifact.ID.String(),
		FileName:    fileName,
		ContentType: contentType,
		SizeBytes:   int64(len(body)),
		SHA256:      actualSHA,
		Body:        body,
	}, nil
}

func (s *Service) verifyNodeSecret(ctx context.Context, plaintext, requiredScope string) (db.RegistryNode, error) {
	plaintext = strings.TrimSpace(plaintext)
	if !strings.HasPrefix(plaintext, nodeSecretPrefix) || len(plaintext) != len(nodeSecretPrefix)+nodeSecretRandomSize*2 {
		return db.RegistryNode{}, httpx.Unauthorized("Registry Node secret 无效或已撤销")
	}
	nodes, err := s.q.ListActiveRegistryNodesBySecretPrefix(ctx, plaintext[:nodeSecretPrefixLen])
	if err != nil {
		return db.RegistryNode{}, httpx.Unauthorized("Registry Node secret 无效或已撤销")
	}
	for _, node := range nodes {
		if bcrypt.CompareHashAndPassword([]byte(node.SecretHash), []byte(plaintext)) == nil && hasScope(node.Scopes, requiredScope) {
			return node, nil
		}
	}
	return db.RegistryNode{}, httpx.Unauthorized("Registry Node secret 无效或已撤销")
}

func normalizeBaseURL(raw string) (*string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, httpx.Unprocessable("base_url 不是合法 URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, httpx.Unprocessable("base_url 仅支持 http/https")
	}
	if len(raw) > 500 {
		return nil, httpx.Unprocessable("base_url 最多 500 字符")
	}
	return &raw, nil
}

func normalizeRemoteAPIBaseURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", httpx.Unprocessable("remote_api_base_url 不能为空")
	}
	if len(raw) > 500 {
		return "", httpx.Unprocessable("remote_api_base_url 最多 500 字符")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", httpx.Unprocessable("remote_api_base_url 不是合法 URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", httpx.Unprocessable("remote_api_base_url 仅支持 http/https")
	}
	u.RawQuery = ""
	u.Fragment = ""
	u.Path = strings.TrimRight(u.Path, "/")
	out := strings.TrimRight(u.String(), "/")
	if !strings.HasSuffix(out, "/api/v1") {
		out += "/api/v1"
	}
	return out, nil
}

func normalizeFederationExchangeURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", httpx.Unprocessable("exchange_url 不能为空")
	}
	if len(raw) > 600 {
		return "", httpx.Unprocessable("exchange_url 最多 600 字符")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", httpx.Unprocessable("exchange_url 不是合法 URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", httpx.Unprocessable("exchange_url 仅支持 http/https")
	}
	u.RawQuery = ""
	u.Fragment = ""
	return strings.TrimRight(u.String(), "/"), nil
}

func normalizeScopes(scopes []string) ([]string, error) {
	if len(scopes) == 0 {
		return append([]string{}, defaultNodeScopes...), nil
	}
	allowed := map[string]struct{}{
		"heartbeat":    {},
		"listing:sync": {},
		"proxy:pull":   {},
		"proxy:result": {},
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(scopes))
	for _, raw := range scopes {
		scope := strings.TrimSpace(raw)
		if _, ok := allowed[scope]; !ok {
			return nil, httpx.Unprocessable("未知 Registry Node scope: " + scope)
		}
		if _, ok := seen[scope]; ok {
			continue
		}
		seen[scope] = struct{}{}
		out = append(out, scope)
	}
	if !hasScope(out, "heartbeat") {
		out = append([]string{"heartbeat"}, out...)
	}
	return out, nil
}

func hasScope(scopes []string, expected string) bool {
	for _, scope := range scopes {
		if scope == expected {
			return true
		}
	}
	return false
}

func validPayloadPolicy(policy string) bool {
	switch policy {
	case payloadPolicyMetadataOnly, payloadPolicyStoreRunSummary, payloadPolicyStoreFullPayload:
		return true
	default:
		return false
	}
}

func normalizePayloadRedactionKeys(keys []string) ([]string, error) {
	if len(keys) > 20 {
		return nil, httpx.Unprocessable("payload_redaction_keys 最多 20 个")
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(keys))
	for _, raw := range keys {
		key := strings.TrimSpace(raw)
		if key == "" {
			return nil, httpx.Unprocessable("payload_redaction_keys 不能包含空字段")
		}
		if len([]rune(key)) > 80 {
			return nil, httpx.Unprocessable("payload_redaction_keys 单个字段最多 80 字符")
		}
		normalized := strings.ToLower(key)
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, key)
	}
	return out, nil
}

func applyInputPayloadPolicy(policy string, fullInput []byte, summary *string, redactionKeys []string) ([]byte, *string) {
	switch policy {
	case payloadPolicyStoreFullPayload:
		return redactPayload(fullInput, redactionKeys), summary
	case payloadPolicyStoreRunSummary:
		return emptyJSONObject(), summary
	default:
		return emptyJSONObject(), nil
	}
}

func applyOutputPayloadPolicy(policy string, fullOutput []byte, summary *string, status string, redactionKeys []string) ([]byte, *string) {
	switch policy {
	case payloadPolicyStoreFullPayload:
		return redactPayload(fullOutput, redactionKeys), summary
	case payloadPolicyStoreRunSummary:
		return emptyJSONObject(), summary
	default:
		if status == "failed" || status == "timeout" {
			return emptyJSONObject(), summary
		}
		return emptyJSONObject(), nil
	}
}

func redactPayload(raw []byte, keys []string) []byte {
	if len(raw) == 0 || len(keys) == 0 {
		return raw
	}
	redactSet := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		redactSet[strings.ToLower(strings.TrimSpace(key))] = struct{}{}
	}
	var value interface{}
	if err := json.Unmarshal(raw, &value); err != nil {
		return raw
	}
	redacted := redactJSONValue(value, redactSet)
	out, err := json.Marshal(redacted)
	if err != nil {
		return raw
	}
	return out
}

func redactJSONValue(value interface{}, redactSet map[string]struct{}) interface{} {
	switch v := value.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(v))
		for key, child := range v {
			if _, ok := redactSet[strings.ToLower(key)]; ok {
				out[key] = "[redacted]"
				continue
			}
			out[key] = redactJSONValue(child, redactSet)
		}
		return out
	case []interface{}:
		out := make([]interface{}, 0, len(v))
		for _, item := range v {
			out = append(out, redactJSONValue(item, redactSet))
		}
		return out
	default:
		return value
	}
}

func emptyJSONObject() []byte {
	return []byte("{}")
}

func generateNodeSecret() (plaintext, prefix string, err error) {
	buf := make([]byte, nodeSecretRandomSize)
	if _, err := rand.Read(buf); err != nil {
		return "", "", err
	}
	plaintext = nodeSecretPrefix + hex.EncodeToString(buf)
	return plaintext, plaintext[:nodeSecretPrefixLen], nil
}

func generateFederationToken() (plaintext, prefix string, err error) {
	buf := make([]byte, federationTokenRandomSize)
	if _, err := rand.Read(buf); err != nil {
		return "", "", err
	}
	plaintext = federationTokenPrefix + hex.EncodeToString(buf)
	return plaintext, plaintext[:federationTokenPrefixLen], nil
}

type registryPeerScanner interface {
	Scan(dest ...any) error
}

func scanRegistryPeer(scanner registryPeerScanner) (registryPeerRow, error) {
	var row registryPeerRow
	err := scanner.Scan(
		&row.ID,
		&row.OwnerUserID,
		&row.Name,
		&row.APIBaseURL,
		&row.BearerToken,
		&row.CredentialHint,
		&row.Status,
		&row.LastUsedAt,
		&row.CreatedAt,
		&row.UpdatedAt,
	)
	return row, err
}

type registryFederationInviteScanner interface {
	Scan(dest ...any) error
}

func scanRegistryFederationInvite(scanner registryFederationInviteScanner) (registryFederationInviteRow, error) {
	var row registryFederationInviteRow
	err := scanner.Scan(
		&row.ID,
		&row.OwnerUserID,
		&row.Name,
		&row.APIBaseURL,
		&row.BearerToken,
		&row.TokenPrefix,
		&row.TokenHash,
		&row.CredentialHint,
		&row.Status,
		&row.ExpiresAt,
		&row.ConsumedAt,
		&row.CreatedAt,
		&row.UpdatedAt,
	)
	return row, err
}

func registryCredentialHint(token string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return "sha256:" + hex.EncodeToString(sum[:])[:12]
}

func registryPeerToResponse(row registryPeerRow) RegistryPeerResponse {
	return RegistryPeerResponse{
		ID:             row.ID.String(),
		Name:           row.Name,
		APIBaseURL:     row.APIBaseURL,
		CredentialHint: row.CredentialHint,
		Status:         row.Status,
		LastUsedAt:     timePtrString(row.LastUsedAt),
		CreatedAt:      row.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:      row.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func registryFederationInviteToResponse(row registryFederationInviteRow, includeTokenPrefix bool) RegistryFederationInviteResponse {
	resp := RegistryFederationInviteResponse{
		ID:             row.ID.String(),
		Name:           row.Name,
		APIBaseURL:     row.APIBaseURL,
		CredentialHint: row.CredentialHint,
		Status:         row.Status,
		ExchangeURL:    row.APIBaseURL + "/registry-peers/federation-invitations/exchange",
		ExpiresAt:      row.ExpiresAt.UTC().Format(time.RFC3339),
		ConsumedAt:     timePtrString(row.ConsumedAt),
		CreatedAt:      row.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:      row.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if includeTokenPrefix {
		resp.TokenPrefix = row.TokenPrefix
	}
	return resp
}

func registryNodeToResponse(node db.RegistryNode) RegistryNodeResponse {
	resp := RegistryNodeResponse{
		ID:              node.ID.String(),
		NodeName:        node.NodeName,
		NodeType:        node.NodeType,
		SecretPrefix:    node.SecretPrefix,
		Scopes:          append([]string{}, node.Scopes...),
		HeartbeatStatus: node.HeartbeatStatus,
		LastHeartbeatAt: timePtrString(node.LastHeartbeatAt),
		RevokedAt:       timePtrString(node.RevokedAt),
		CreatedAt:       node.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:       node.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if node.BaseURL != nil {
		resp.BaseURL = *node.BaseURL
	}
	return resp
}

func cloudListingLinkToResponse(link db.CloudListingLink, nodeName, agentSlug, agentName string) *CloudListingLinkResponse {
	if link.SyncedAgentSlug != "" {
		agentSlug = link.SyncedAgentSlug
	}
	if link.SyncedAgentName != "" {
		agentName = link.SyncedAgentName
	}
	return &CloudListingLinkResponse{
		ID:                   link.ID.String(),
		RegistryListingID:    link.CloudListingID.String(),
		CloudListingID:       link.CloudListingID.String(),
		RegistryNodeID:       link.RegistryNodeID.String(),
		NodeName:             nodeName,
		AgentID:              link.LocalAgentID.String(),
		AgentSlug:            agentSlug,
		AgentName:            agentName,
		AgentDescription:     link.SyncedAgentDescription,
		AgentTags:            append([]string{}, link.SyncedAgentTags...),
		AvailabilityStatus:   link.SyncedAvailabilityStatus,
		MetadataSyncedAt:     timePtrString(link.MetadataSyncedAt),
		MetadataSyncError:    stringPtrValue(link.MetadataSyncError),
		RoutingMode:          link.RoutingMode,
		PayloadPolicy:        link.PayloadPolicy,
		PayloadRedactionKeys: append([]string{}, link.PayloadRedactionKeys...),
		SyncStatus:           link.SyncStatus,
		LastSyncAt:           link.LastSyncAt.UTC().Format(time.RFC3339),
		CreatedAt:            link.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:            link.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func cloudListingRowToResponse(row db.ListCloudListingLinksByOwnerRow) CloudListingLinkResponse {
	return CloudListingLinkResponse{
		ID:                   row.ID.String(),
		RegistryListingID:    row.CloudListingID.String(),
		CloudListingID:       row.CloudListingID.String(),
		RegistryNodeID:       row.RegistryNodeID.String(),
		NodeName:             row.NodeName,
		AgentID:              row.LocalAgentID.String(),
		AgentSlug:            row.AgentSlug,
		AgentName:            row.AgentName,
		AgentDescription:     row.AgentDescription,
		AgentTags:            append([]string{}, row.AgentTags...),
		AvailabilityStatus:   row.AvailabilityStatus,
		MetadataSyncedAt:     timePtrString(row.MetadataSyncedAt),
		MetadataSyncError:    stringPtrValue(row.MetadataSyncError),
		RoutingMode:          row.RoutingMode,
		PayloadPolicy:        row.PayloadPolicy,
		PayloadRedactionKeys: append([]string{}, row.PayloadRedactionKeys...),
		SyncStatus:           row.SyncStatus,
		LastSyncAt:           row.LastSyncAt.UTC().Format(time.RFC3339),
		CreatedAt:            row.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:            row.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func proxyRunToResponse(run db.ProxyRun) ProxyRunResponse {
	resp := ProxyRunResponse{
		ID:                    run.ID.String(),
		CloudRunID:            run.CloudRunID.String(),
		RegistryListingLinkID: run.CloudListingLinkID.String(),
		CloudListingLinkID:    run.CloudListingLinkID.String(),
		RegistryListingID:     run.CloudListingID.String(),
		CloudListingID:        run.CloudListingID.String(),
		RegistryNodeID:        run.RegistryNodeID.String(),
		LocalAgentID:          run.LocalAgentID.String(),
		RequestingUserID:      run.RequestingUserID.String(),
		IdempotencyKey:        run.IdempotencyKey,
		Status:                run.Status,
		PayloadPolicy:         run.PayloadPolicy,
		Input:                 jsonObjFromBytes(run.Input),
		Output:                jsonObjFromBytes(run.Output),
		ClaimedAt:             timePtrString(run.ClaimedAt),
		FinishedAt:            timePtrString(run.FinishedAt),
		AttemptCount:          run.AttemptCount,
		MaxAttempts:           run.MaxAttempts,
		NextRetryAt:           timePtrString(run.NextRetryAt),
		CreatedAt:             run.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:             run.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if run.InputSummary != nil {
		resp.InputSummary = *run.InputSummary
	}
	if run.OutputSummary != nil {
		resp.OutputSummary = *run.OutputSummary
	}
	if run.ErrorCode != nil {
		resp.ErrorCode = *run.ErrorCode
	}
	if run.ErrorMessage != nil {
		resp.ErrorMessage = *run.ErrorMessage
	}
	return resp
}

type proxyRunArtifactDraft struct {
	SourceArtifactID string
	ArtifactType     string
	Title            string
	Content          []byte
	MimeType         *string
	FileURI          *string
	FileName         *string
	FileSHA256       *string
	FileSizeBytes    *int64
}

func extractProxyRunArtifacts(rawOutput []byte, payloadPolicy string) []proxyRunArtifactDraft {
	var output map[string]interface{}
	if len(rawOutput) == 0 || json.Unmarshal(rawOutput, &output) != nil {
		return nil
	}
	values := artifactValuesFromOutput(output)
	items := make([]proxyRunArtifactDraft, 0, len(values))
	seen := map[string]int{}
	for i, value := range values {
		m, ok := value.(map[string]interface{})
		if !ok {
			continue
		}
		sourceID := normalizeArtifactText(firstString(m, "source_artifact_id", "sourceArtifactID", "artifact_id", "artifactId", "id"), 160)
		if sourceID == "" {
			sourceID = "artifact-" + strconv.Itoa(i+1)
		}
		if count := seen[sourceID]; count > 0 {
			sourceID = sourceID + "-" + uuid.NewString()[:8]
		}
		seen[sourceID]++
		title := normalizeArtifactText(firstString(m, "title", "name", "filename", "file_name", "fileName"), 300)
		if title == "" {
			title = "Artifact " + strconv.Itoa(i+1)
		}
		artifactType := normalizeArtifactText(firstString(m, "artifact_type", "artifactType", "type", "kind"), 80)
		if artifactType == "" {
			artifactType = "data"
		}
		content := emptyJSONObject()
		if payloadPolicy == payloadPolicyStoreFullPayload {
			content = artifactContent(m)
		}
		meta := artifactFileMetadataFromMap(m)
		items = append(items, proxyRunArtifactDraft{
			SourceArtifactID: sourceID,
			ArtifactType:     artifactType,
			Title:            title,
			Content:          content,
			MimeType:         stringPtr(meta.MimeType),
			FileURI:          stringPtr(meta.FileURI),
			FileName:         stringPtr(meta.FileName),
			FileSHA256:       stringPtr(meta.FileSHA256),
			FileSizeBytes:    meta.FileSizeBytes,
		})
	}
	return items
}

func artifactValuesFromOutput(output map[string]interface{}) []interface{} {
	var values []interface{}
	if artifacts, ok := output["artifacts"].([]interface{}); ok {
		values = append(values, artifacts...)
	}
	if artifact, ok := output["artifact"].(map[string]interface{}); ok {
		values = append(values, artifact)
	}
	return values
}

func artifactContent(raw map[string]interface{}) []byte {
	for _, key := range []string{"content", "data"} {
		if value, ok := raw[key].(map[string]interface{}); ok {
			if out, err := json.Marshal(value); err == nil {
				return out
			}
		}
	}
	return emptyJSONObject()
}

type artifactFileMetadata struct {
	MimeType      string
	FileURI       string
	FileName      string
	FileSHA256    string
	FileSizeBytes *int64
}

func artifactFileMetadataFromMap(raw map[string]interface{}) artifactFileMetadata {
	meta := artifactFileMetadata{
		MimeType:   normalizeArtifactText(firstString(raw, "mime_type", "mimeType", "media_type", "mediaType", "content_type", "contentType"), 200),
		FileURI:    normalizeArtifactText(firstString(raw, "file_uri", "fileUri", "uri", "url"), 2000),
		FileName:   normalizeArtifactText(firstString(raw, "file_name", "fileName", "filename", "name"), 500),
		FileSHA256: normalizeSHA256(firstString(raw, "file_sha256", "fileSha256", "sha256", "checksum")),
	}
	if size, ok := firstInt64(raw, "file_size_bytes", "fileSizeBytes", "size_bytes", "sizeBytes", "size"); ok {
		meta.FileSizeBytes = &size
	}
	for _, key := range []string{"file", "file_ref", "fileRef", "binary"} {
		if nested, ok := raw[key].(map[string]interface{}); ok {
			meta = mergeArtifactFileMetadata(meta, artifactFileMetadataFromMap(nested))
		}
	}
	return meta
}

func mergeArtifactFileMetadata(base, next artifactFileMetadata) artifactFileMetadata {
	if base.MimeType == "" {
		base.MimeType = next.MimeType
	}
	if base.FileURI == "" {
		base.FileURI = next.FileURI
	}
	if base.FileName == "" {
		base.FileName = next.FileName
	}
	if base.FileSHA256 == "" {
		base.FileSHA256 = next.FileSHA256
	}
	if base.FileSizeBytes == nil {
		base.FileSizeBytes = next.FileSizeBytes
	}
	return base
}

func firstString(raw map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := raw[key].(string); ok && strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func firstInt64(raw map[string]interface{}, keys ...string) (int64, bool) {
	for _, key := range keys {
		switch value := raw[key].(type) {
		case int64:
			if value >= 0 {
				return value, true
			}
		case int:
			if value >= 0 {
				return int64(value), true
			}
		case int32:
			if value >= 0 {
				return int64(value), true
			}
		case float64:
			if value >= 0 {
				return int64(value), true
			}
		case float32:
			if value >= 0 {
				return int64(value), true
			}
		}
	}
	return 0, false
}

func normalizeArtifactText(value string, max int) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) > max {
		return string(runes[:max])
	}
	return value
}

func normalizeSHA256(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) != 64 {
		return ""
	}
	for _, r := range value {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return ""
		}
	}
	return value
}

func stringPtr(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return &value
}

func proxyRunArtifactToResponse(artifact db.ProxyRunArtifact) ProxyRunArtifactResponse {
	resp := ProxyRunArtifactResponse{
		ID:               artifact.ID.String(),
		ProxyRunID:       artifact.ProxyRunID.String(),
		CloudRunID:       artifact.CloudRunID.String(),
		SourceArtifactID: artifact.SourceArtifactID,
		ArtifactType:     artifact.ArtifactType,
		Title:            artifact.Title,
		Content:          jsonObjFromBytes(artifact.Content),
		FileSizeBytes:    artifact.FileSizeBytes,
		CreatedAt:        artifact.CreatedAt.UTC().Format(time.RFC3339),
	}
	if artifact.MimeType != nil {
		resp.MimeType = *artifact.MimeType
	}
	if artifact.FileURI != nil {
		resp.FileURI = *artifact.FileURI
	}
	if artifact.FileName != nil {
		resp.FileName = *artifact.FileName
	}
	if artifact.FileSHA256 != nil {
		resp.FileSHA256 = *artifact.FileSHA256
	}
	return resp
}

func marshalJSONObj(value map[string]any) ([]byte, error) {
	if value == nil {
		value = map[string]any{}
	}
	return json.Marshal(value)
}

func jsonObjFromBytes(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil
	}
	if len(value) == 0 {
		return nil
	}
	return value
}

func optionalText(raw string, maxLen int, field string) (*string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil, nil
	}
	if len([]rune(value)) > maxLen {
		return nil, httpx.Unprocessable(field + " 超过长度限制")
	}
	return &value, nil
}

func timePtrString(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func stringPtrValue(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
