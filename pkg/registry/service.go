package registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/bcrypt"

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

const (
	nodeSecretPrefix     = "rn_live_"
	nodeSecretPrefixLen  = 12
	nodeSecretRandomSize = 32
	defaultNodeType      = "bridge_proxy"
	defaultRoutingMode   = "pull_proxy"
	defaultPayloadPolicy = "metadata_only"
)

var defaultNodeScopes = []string{"heartbeat", "listing:sync", "proxy:pull", "proxy:result"}

type Service struct {
	q *db.Queries
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{q: db.New(pool)}
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
		return nil, httpx.Conflict("Agent 未启用，不能创建 Cloud Listing")
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
	if payloadPolicy != "metadata_only" && payloadPolicy != "store_run_summary" && payloadPolicy != "store_full_payload" {
		return nil, httpx.Unprocessable("payload_policy 取值非法")
	}
	link, err := s.q.UpsertCloudListingLink(ctx, db.UpsertCloudListingLinkParams{
		RegistryNodeID: nodeID,
		LocalAgentID:   agentID,
		RoutingMode:    routingMode,
		PayloadPolicy:  payloadPolicy,
	})
	if err != nil {
		log.Error().Err(err).Str("node_id", nodeID.String()).Str("agent_id", agentID.String()).
			Msg("registry.CreateCloudListing: upsert")
		return nil, httpx.Internal("创建 Cloud Listing 失败")
	}
	return cloudListingLinkToResponse(link, node.NodeName, agent.Slug, agent.Name), nil
}

func (s *Service) ListCloudListings(ctx context.Context, ownerID uuid.UUID) ([]CloudListingLinkResponse, error) {
	rows, err := s.q.ListCloudListingLinksByOwner(ctx, ownerID)
	if err != nil {
		log.Error().Err(err).Str("owner_id", ownerID.String()).Msg("registry.ListCloudListings")
		return nil, httpx.Internal("查询 Cloud Listing 失败")
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
		return nil, httpx.NotFound("Cloud Listing 不存在或对应 Registry Node 已撤销")
	}
	if err != nil {
		log.Error().Err(err).Str("cloud_listing_id", cloudListingID.String()).Msg("registry.UpdateCloudListingStatus")
		return nil, httpx.Internal("更新 Cloud Listing 状态失败")
	}
	resp := cloudListingRowToResponse(row)
	return &resp, nil
}

func (s *Service) CreateProxyRun(ctx context.Context, requestingUserID uuid.UUID, req *CreateProxyRunRequest) (*ProxyRunResponse, error) {
	cloudListingID, err := uuid.Parse(strings.TrimSpace(req.CloudListingID))
	if err != nil {
		return nil, httpx.BadRequest("cloud_listing_id 不是合法 uuid")
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
	run, err := s.q.CreateProxyRun(ctx, db.CreateProxyRunParams{
		CloudListingID:   cloudListingID,
		RequestingUserID: requestingUserID,
		IdempotencyKey:   idempotencyKey,
		Input:            input,
		InputSummary:     inputSummary,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Cloud Listing 不存在、未 linked 或对应 Registry Node 已撤销")
	}
	if err != nil {
		log.Error().Err(err).Str("cloud_listing_id", cloudListingID.String()).Msg("registry.CreateProxyRun")
		return nil, httpx.Internal("创建 Proxy Run 失败")
	}
	resp := proxyRunToResponse(run)
	return &resp, nil
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
	run, err := s.q.CompleteProxyRun(ctx, db.CompleteProxyRunParams{
		ID:             runID,
		RegistryNodeID: node.ID,
		Status:         status,
		Output:         output,
		OutputSummary:  outputSummary,
		ErrorCode:      errorCode,
		ErrorMessage:   errorMessage,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Proxy Run 不存在、已完成或不属于该 Registry Node")
	}
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Str("node_id", node.ID.String()).Msg("registry.CompleteProxyRun")
		return nil, httpx.Internal("回写 Proxy Run 结果失败")
	}
	resp := proxyRunToResponse(run)
	return &resp, nil
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

func generateNodeSecret() (plaintext, prefix string, err error) {
	buf := make([]byte, nodeSecretRandomSize)
	if _, err := rand.Read(buf); err != nil {
		return "", "", err
	}
	plaintext = nodeSecretPrefix + hex.EncodeToString(buf)
	return plaintext, plaintext[:nodeSecretPrefixLen], nil
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
	return &CloudListingLinkResponse{
		ID:             link.ID.String(),
		CloudListingID: link.CloudListingID.String(),
		RegistryNodeID: link.RegistryNodeID.String(),
		NodeName:       nodeName,
		AgentID:        link.LocalAgentID.String(),
		AgentSlug:      agentSlug,
		AgentName:      agentName,
		RoutingMode:    link.RoutingMode,
		PayloadPolicy:  link.PayloadPolicy,
		SyncStatus:     link.SyncStatus,
		LastSyncAt:     link.LastSyncAt.UTC().Format(time.RFC3339),
		CreatedAt:      link.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:      link.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func cloudListingRowToResponse(row db.ListCloudListingLinksByOwnerRow) CloudListingLinkResponse {
	return CloudListingLinkResponse{
		ID:             row.ID.String(),
		CloudListingID: row.CloudListingID.String(),
		RegistryNodeID: row.RegistryNodeID.String(),
		NodeName:       row.NodeName,
		AgentID:        row.LocalAgentID.String(),
		AgentSlug:      row.AgentSlug,
		AgentName:      row.AgentName,
		RoutingMode:    row.RoutingMode,
		PayloadPolicy:  row.PayloadPolicy,
		SyncStatus:     row.SyncStatus,
		LastSyncAt:     row.LastSyncAt.UTC().Format(time.RFC3339),
		CreatedAt:      row.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:      row.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func proxyRunToResponse(run db.ProxyRun) ProxyRunResponse {
	resp := ProxyRunResponse{
		ID:                 run.ID.String(),
		CloudRunID:         run.CloudRunID.String(),
		CloudListingLinkID: run.CloudListingLinkID.String(),
		CloudListingID:     run.CloudListingID.String(),
		RegistryNodeID:     run.RegistryNodeID.String(),
		LocalAgentID:       run.LocalAgentID.String(),
		RequestingUserID:   run.RequestingUserID.String(),
		IdempotencyKey:     run.IdempotencyKey,
		Status:             run.Status,
		PayloadPolicy:      run.PayloadPolicy,
		Input:              jsonObjFromBytes(run.Input),
		Output:             jsonObjFromBytes(run.Output),
		ClaimedAt:          timePtrString(run.ClaimedAt),
		FinishedAt:         timePtrString(run.FinishedAt),
		CreatedAt:          run.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:          run.UpdatedAt.UTC().Format(time.RFC3339),
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
