package a2a

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/bcrypt"

	"github.com/OpenLinker-ai/openlinker-core/pkg/credential"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const (
	runtimeTokenPrefixLen = credential.PrefixLen
	maxRuntimeTokens      = 10
	maxDelegationDepth    = 8
	maxRunningDelegations = 500
	defaultParentPage     = 1
	defaultParentPageSize = 10
	maxParentPageSize     = 50
	maxParentSearchLen    = 120
)

type Service struct {
	queries               *db.Queries
	pool                  *pgxpool.Pool
	runtime               *runtime.Service
	taskCallbackManager   taskCallbackManager
	maxDelegationDepth    int
	maxRunningDelegations int
}

func NewService(pool *pgxpool.Pool, runtimeSvc *runtime.Service) *Service {
	return &Service{
		queries:               db.New(pool),
		pool:                  pool,
		runtime:               runtimeSvc,
		maxDelegationDepth:    maxDelegationDepth,
		maxRunningDelegations: maxRunningDelegations,
	}
}

func (s *Service) SetDelegationLimits(maxDepth, maxRunning int) {
	s.maxDelegationDepth = maxDepth
	s.maxRunningDelegations = maxRunning
}

func (s *Service) CreateRuntimeToken(ctx context.Context, userID, agentID uuid.UUID, req *CreateRuntimeTokenRequest) (*RuntimeTokenResponse, error) {
	agent, err := s.ownerAgent(ctx, userID, agentID)
	if err != nil {
		return nil, err
	}
	count, err := s.queries.CountActiveAgentRuntimeTokens(ctx, agentID)
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("a2a.CreateRuntimeToken: count")
		return nil, httpx.Internal("查询访问令牌失败")
	}
	if count >= maxRuntimeTokens {
		return nil, httpx.BadRequest("访问令牌数量已达上限（10 个），请先撤销旧令牌")
	}

	plaintext, prefix, err := credential.GenerateAccessToken()
	if err != nil {
		return nil, httpx.Internal("生成访问令牌失败")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(plaintext), credential.BcryptCost)
	if err != nil {
		return nil, httpx.Internal("加密访问令牌失败")
	}
	token, err := s.queries.CreateAgentRuntimeToken(ctx, db.CreateAgentRuntimeTokenParams{
		AgentID:         agentID,
		CreatedByUserID: userID,
		Name:            strings.TrimSpace(req.Name),
		Prefix:          prefix,
		TokenHash:       string(hash),
		Scopes:          runtimeTokenScopesForAgent(agent),
	})
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("a2a.CreateRuntimeToken: insert")
		return nil, httpx.Internal("创建访问令牌失败")
	}
	resp := tokenResponse(token)
	resp.PlaintextToken = plaintext
	return &resp, nil
}

func runtimeTokenScopesForAgent(agent db.Agent) []string {
	scopes := []string{"agent:call"}
	if agent.ConnectionMode == "runtime_pull" || agent.ConnectionMode == "runtime_ws" {
		scopes = append(scopes, "agent:pull")
	}
	return scopes
}

func isQueuedRuntimeConnectionMode(mode string) bool {
	return mode == "runtime_pull" || mode == "runtime_ws"
}

func (s *Service) ListRuntimeTokens(ctx context.Context, userID, agentID uuid.UUID) ([]RuntimeTokenResponse, error) {
	if _, err := s.ownerAgent(ctx, userID, agentID); err != nil {
		return nil, err
	}
	tokens, err := s.queries.ListAgentRuntimeTokensForOwner(ctx, db.ListAgentRuntimeTokensForOwnerParams{
		AgentID: agentID,
		UserID:  userID,
	})
	if err != nil {
		return nil, httpx.Internal("查询访问令牌失败")
	}
	items := make([]RuntimeTokenResponse, 0, len(tokens))
	for _, token := range tokens {
		items = append(items, tokenResponse(token))
	}
	return items, nil
}

func (s *Service) GetRuntimeWorkbench(ctx context.Context, userID, agentID uuid.UUID) (*RuntimeWorkbenchResponse, error) {
	agent, err := s.ownerAgent(ctx, userID, agentID)
	if err != nil {
		return nil, err
	}
	tokens, err := s.ListRuntimeTokens(ctx, userID, agentID)
	if err != nil {
		return nil, err
	}
	pendingCount := int32(0)
	if agent.ConnectionMode == "runtime_pull" || agent.ConnectionMode == "runtime_ws" {
		count, countErr := s.queries.CountClaimableRuntimePullRuns(ctx, agentID)
		if countErr != nil {
			log.Warn().Err(countErr).Str("agent_id", agentID.String()).Msg("a2a.GetRuntimeWorkbench: CountClaimableRuntimePullRuns")
		} else {
			pendingCount = count
		}
	}
	runs, err := s.queries.ListRunsByCreatorAgentWithAgent(ctx, db.ListRunsByCreatorAgentWithAgentParams{
		CreatorID: userID,
		AgentID:   agentID,
		Limit:     10,
		Offset:    0,
	})
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("a2a.GetRuntimeWorkbench: ListRunsByCreatorAgentWithAgent")
		return nil, httpx.Internal("查询 Agent 运行记录失败")
	}

	recentRuns := make([]RuntimeWorkbenchRun, 0, len(runs))
	var lastClaimedAt *string
	var lastResultAt *string
	for _, run := range runs {
		item := RuntimeWorkbenchRun{
			RunID:        run.ID.String(),
			Status:       run.Status,
			Source:       run.Source,
			StartedAt:    run.StartedAt.UTC().Format(time.RFC3339),
			ErrorCode:    run.ErrorCode,
			ErrorMessage: run.ErrorMessage,
			DetailURL:    "/run/" + run.ID.String(),
		}
		if run.ClaimedAt != nil {
			value := run.ClaimedAt.UTC().Format(time.RFC3339)
			item.ClaimedAt = &value
			if lastClaimedAt == nil {
				lastClaimedAt = &value
			}
		}
		if run.FinishedAt != nil {
			value := run.FinishedAt.UTC().Format(time.RFC3339)
			item.FinishedAt = &value
			if lastResultAt == nil {
				lastResultAt = &value
			}
		}
		recentRuns = append(recentRuns, item)
	}

	var lastActivity *string
	for _, token := range tokens {
		if token.RevokedAt != nil || token.LastUsedAt == nil {
			continue
		}
		if lastActivity == nil || *token.LastUsedAt > *lastActivity {
			value := *token.LastUsedAt
			lastActivity = &value
		}
	}

	availability := runtimeWorkbenchAvailability(agent, tokens, recentRuns)
	resp := &RuntimeWorkbenchResponse{
		Agent: RuntimeWorkbenchAgent{
			ID:                  agent.ID.String(),
			Slug:                agent.Slug,
			Name:                agent.Name,
			ConnectionMode:      agent.ConnectionMode,
			LifecycleStatus:     agent.LifecycleStatus,
			Visibility:          agent.Visibility,
			CertificationStatus: agent.CertificationStatus,
			ReadinessCallable:   availability == "healthy",
			AvailabilityStatus:  availability,
		},
		Runtime: RuntimeWorkbenchRuntime{
			ActiveTokenCount:                 activeRuntimeTokenCount(tokens),
			PendingRunCount:                  pendingCount,
			ClaimNow:                         pendingCount > 0,
			LastRuntimeActivityAt:            lastActivity,
			LastClaimedAt:                    lastClaimedAt,
			LastResultAt:                     lastResultAt,
			RecommendedHeartbeatAfterSeconds: 60,
			MaxClaimWaitSeconds:              30,
		},
		Tokens:      tokens,
		RecentRuns:  recentRuns,
		Diagnostics: runtimeWorkbenchDiagnostics(agent, tokens, pendingCount, recentRuns, lastActivity),
	}
	return resp, nil
}

func (s *Service) RevokeRuntimeToken(ctx context.Context, userID, tokenID uuid.UUID) error {
	affected, err := s.queries.RevokeAgentRuntimeTokenForOwner(ctx, db.RevokeAgentRuntimeTokenForOwnerParams{
		ID: tokenID, UserID: userID,
	})
	if err != nil {
		return httpx.Internal("撤销访问令牌失败")
	}
	if affected == 0 {
		return httpx.NotFound("访问令牌不存在或已撤销")
	}
	return nil
}

func activeRuntimeTokenCount(tokens []RuntimeTokenResponse) int32 {
	var count int32
	for _, token := range tokens {
		if token.RevokedAt == nil {
			count++
		}
	}
	return count
}

func runtimeWorkbenchAvailability(agent db.Agent, tokens []RuntimeTokenResponse, runs []RuntimeWorkbenchRun) string {
	if agent.LifecycleStatus != "active" {
		return "disabled"
	}
	for _, run := range runs {
		if run.Status == "success" {
			return "healthy"
		}
	}
	for _, token := range tokens {
		if token.RevokedAt == nil && token.LastUsedAt != nil &&
			(!isQueuedRuntimeConnectionMode(agent.ConnectionMode) || hasScope(token.Scopes, "agent:pull")) {
			return "active"
		}
	}
	return "unknown"
}

func runtimeWorkbenchDiagnostics(
	agent db.Agent,
	tokens []RuntimeTokenResponse,
	pendingCount int32,
	runs []RuntimeWorkbenchRun,
	lastActivity *string,
) []RuntimeWorkbenchDiagnostic {
	diagnostics := []RuntimeWorkbenchDiagnostic{}
	if !isQueuedRuntimeConnectionMode(agent.ConnectionMode) {
		return append(diagnostics, RuntimeWorkbenchDiagnostic{
			Code:       "not_runtime_pull",
			Severity:   "info",
			Message:    "Agent 不是队列型 runtime 接入模式，使用 endpoint 或 MCP 健康检查维护可用性。",
			NextAction: "run_health_check",
		})
	}
	if activeRuntimeTokenCount(tokens) == 0 {
		diagnostics = append(diagnostics, RuntimeWorkbenchDiagnostic{
			Code:       "no_runtime_token",
			Severity:   "warning",
			Message:    "当前没有可用的 Agent runtime token，worker 无法 heartbeat、claim 或 result。",
			NextAction: "create_runtime_token",
		})
	}
	if activeRuntimeTokenCount(tokens) > 0 && !hasActiveRuntimePullToken(tokens) {
		diagnostics = append(diagnostics, RuntimeWorkbenchDiagnostic{
			Code:       "scope_missing",
			Severity:   "error",
			Message:    "当前 active runtime token 缺少 agent:pull scope，worker 无法建立 WebSocket 或领取任务。",
			NextAction: "create_runtime_token",
		})
	}
	if lastActivity == nil {
		diagnostics = append(diagnostics, RuntimeWorkbenchDiagnostic{
			Code:       "no_recent_runtime_activity",
			Severity:   "warning",
			Message:    "还没有看到 runtime token 活动。启动 worker 后应先 heartbeat。",
			NextAction: "start_worker",
		})
	}
	if pendingCount > 0 {
		diagnostics = append(diagnostics, RuntimeWorkbenchDiagnostic{
			Code:       "pending_claimable_runs",
			Severity:   "warning",
			Message:    "存在待派发 run。确认 worker 已建立 WebSocket，或正在使用 claim?wait=25 长轮询。",
			NextAction: "check_claim_loop",
		})
	}
	for _, run := range runs {
		if run.ErrorCode == nil {
			continue
		}
		switch *run.ErrorCode {
		case "RUNTIME_PULL_NOT_CLAIMED":
			diagnostics = append(diagnostics, RuntimeWorkbenchDiagnostic{
				Code:       "pending_not_claimed",
				Severity:   "error",
				Message:    "最近有 runtime run 超时未被派发或领取。",
				NextAction: "start_worker",
			})
		case "RUNTIME_PULL_RESULT_TIMEOUT":
			diagnostics = append(diagnostics, RuntimeWorkbenchDiagnostic{
				Code:       "result_timeout",
				Severity:   "error",
				Message:    "最近有 run 已领取但未在超时时间内回传结果。",
				NextAction: "inspect_worker_result",
			})
		}
	}
	if len(diagnostics) == 0 {
		diagnostics = append(diagnostics, RuntimeWorkbenchDiagnostic{
			Code:       "runtime_ready",
			Severity:   "success",
			Message:    "runtime 供给当前没有明显阻断项。",
			NextAction: "keep_worker_supervised",
		})
	}
	return diagnostics
}

func hasActiveRuntimePullToken(tokens []RuntimeTokenResponse) bool {
	for _, token := range tokens {
		if token.RevokedAt == nil && hasScope(token.Scopes, "agent:pull") {
			return true
		}
	}
	return false
}

func (s *Service) GetCallPolicy(ctx context.Context, userID, agentID uuid.UUID) (*CallPolicyResponse, error) {
	if _, err := s.ownerAgent(ctx, userID, agentID); err != nil {
		return nil, err
	}
	policy, err := s.queries.GetAgentCallPolicy(ctx, agentID)
	if err != nil {
		return nil, httpx.Internal("查询 A2A 策略失败")
	}
	return &CallPolicyResponse{AgentID: agentID.String(), CallableBy: policy}, nil
}

func (s *Service) UpdateCallPolicy(ctx context.Context, userID, agentID uuid.UUID, req *UpdateCallPolicyRequest) (*CallPolicyResponse, error) {
	policy, err := s.queries.UpsertAgentCallPolicyForOwner(ctx, db.UpsertAgentCallPolicyForOwnerParams{
		AgentID: agentID, UserID: userID, CallableBy: req.CallableBy,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Agent 不存在")
	}
	if err != nil {
		return nil, httpx.Internal("更新 A2A 策略失败")
	}
	return &CallPolicyResponse{
		AgentID:    policy.AgentID.String(),
		CallableBy: policy.CallableBy,
		UpdatedAt:  policy.UpdatedAt.UTC().Format(time.RFC3339),
	}, nil
}

func (s *Service) CallAgent(ctx context.Context, plaintextToken string, req *CallAgentRequest) (*runtime.RunResponse, error) {
	callerToken, err := s.verifyRuntimeToken(ctx, plaintextToken)
	if err != nil {
		return nil, err
	}
	parentRunID, err := currentRunIDFromRequest(req)
	if err != nil {
		return nil, err
	}
	targetAgentID, err := uuid.Parse(req.TargetAgentID)
	if err != nil {
		return nil, httpx.Unprocessable("target_agent_id 不是合法 uuid")
	}
	if targetAgentID == callerToken.AgentID {
		return nil, httpx.Unprocessable("Agent 不能通过 A2A 调用自己")
	}

	parent, err := s.queries.GetRunByID(ctx, parentRunID)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && parent.AgentID != callerToken.AgentID) {
		return nil, httpx.NotFound("父运行不存在或不属于调用 Agent")
	}
	if err != nil {
		return nil, httpx.Internal("查询父运行失败")
	}
	if parent.Status != "running" {
		return nil, httpx.Conflict("父运行已结束，不能发起新的 Agent 委派")
	}

	caller, err := s.queries.GetAgentByID(ctx, callerToken.AgentID)
	if err != nil {
		return nil, httpx.NotFound("调用 Agent 不存在")
	}
	target, err := s.queries.GetAgentByID(ctx, targetAgentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("目标 Agent 不存在")
	}
	if err != nil {
		return nil, httpx.Internal("查询目标 Agent 失败")
	}
	policy, err := s.queries.GetAgentCallPolicy(ctx, targetAgentID)
	if err != nil {
		return nil, httpx.Internal("查询目标 Agent 调用策略失败")
	}
	if policy == "private" || (policy == "same_creator" && caller.CreatorID != target.CreatorID) {
		return nil, httpx.Forbidden("目标 Agent 不接受当前 Agent 的调用")
	}
	if err := s.enforceDelegationLimits(ctx, parentRunID, targetAgentID); err != nil {
		return nil, err
	}
	taskCallbackConfig := taskCallbackConfigFromCallRequest(req)
	if err := s.validateCallerTaskCallbackConfig(taskCallbackConfig); err != nil {
		return nil, err
	}
	childContext, err := s.childRunA2AContext(ctx, parentRunID, parent, callerToken.AgentID, targetAgentID, req)
	if err != nil {
		return nil, err
	}
	metadata := copyMetadata(req.Metadata)
	if childContext != nil {
		metadata["a2a"] = map[string]interface{}{
			"protocol_context_id": childContext.ProtocolContextID,
			"root_context_id":     childContext.RootContextID,
			"parent_context_id":   childContext.ParentContextID,
			"parent_task_id":      childContext.ParentTaskID,
			"trace_id":            childContext.TraceID,
			"reference_task_ids":  childContext.ReferenceTaskIDs,
		}
	}

	_ = s.queries.TouchAgentRuntimeToken(ctx, callerToken.ID)
	resp, err := s.runtime.RunDelegated(ctx, parent.UserID, runtime.Delegation{
		ParentRunID:   parentRunID,
		CallerAgentID: callerToken.AgentID,
		Reason:        strings.TrimSpace(req.Reason),
	}, &runtime.RunRequest{
		AgentID:    targetAgentID.String(),
		Input:      req.Input,
		Metadata:   metadata,
		A2AContext: childContext,
	})
	if err != nil {
		return nil, err
	}
	callback, err := s.createCallerTaskCallback(ctx, parent.UserID, resp.RunID, taskCallbackConfig)
	if err != nil {
		return nil, err
	}
	resp.TaskCallback = callback
	return resp, nil
}

func (s *Service) enforceDelegationLimits(ctx context.Context, parentRunID, targetAgentID uuid.UUID) error {
	if s.maxDelegationDepth > 0 {
		lineage, err := s.delegationLineage(ctx, parentRunID)
		if err != nil {
			log.Error().Err(err).Str("parent_run_id", parentRunID.String()).Msg("a2a.enforceDelegationLimits: lineage")
			return httpx.Internal("检查 Agent 调用链失败")
		}
		for _, agentID := range lineage {
			if agentID == targetAgentID {
				return httpx.Unprocessable("A2A 调用链不能形成循环")
			}
		}
		if len(lineage) > s.maxDelegationDepth {
			return httpx.Unprocessable("A2A 调用链深度超过限制")
		}
	}
	if s.maxRunningDelegations > 0 {
		count, err := s.runningDelegationCount(ctx)
		if err != nil {
			log.Error().Err(err).Msg("a2a.enforceDelegationLimits: running count")
			return httpx.Internal("检查 Agent 委派并发失败")
		}
		if count >= s.maxRunningDelegations {
			return httpx.NewError(http.StatusTooManyRequests, httpx.CodeRateLimited, "A2A 委派并发已达上限，请稍后再试")
		}
	}
	return nil
}

func (s *Service) delegationLineage(ctx context.Context, runID uuid.UUID) ([]uuid.UUID, error) {
	return s.queries.ListDelegationLineage(ctx, db.ListDelegationLineageParams{
		RunID:    runID,
		MaxDepth: int32(s.maxDelegationDepth + 1),
	})
}

func (s *Service) runningDelegationCount(ctx context.Context) (int, error) {
	count, err := s.queries.CountRunningDelegations(ctx)
	return int(count), err
}

func currentRunIDFromRequest(req *CallAgentRequest) (uuid.UUID, error) {
	current := strings.TrimSpace(req.CurrentRunID)
	legacyParent := strings.TrimSpace(req.ParentRunID)
	if current == "" {
		current = legacyParent
	}
	if current == "" {
		return uuid.Nil, httpx.Unprocessable("current_run_id 或 parent_run_id 必填")
	}
	if legacyParent != "" && req.CurrentRunID != "" && legacyParent != current {
		return uuid.Nil, httpx.Unprocessable("current_run_id 与 parent_run_id 不一致")
	}
	parsed, err := uuid.Parse(current)
	if err != nil {
		return uuid.Nil, httpx.Unprocessable("current_run_id 不是合法 uuid")
	}
	return parsed, nil
}

func (s *Service) childRunA2AContext(
	ctx context.Context,
	parentRunID uuid.UUID,
	parent db.Run,
	callerAgentID uuid.UUID,
	targetAgentID uuid.UUID,
	req *CallAgentRequest,
) (*runtime.RunA2AContextRequest, error) {
	explicitContextID := strings.TrimSpace(req.ContextID)
	if explicitContextID == "" {
		explicitContextID = strings.TrimSpace(req.ContextIDAlias)
	}
	explicitTraceID := strings.TrimSpace(req.TraceID)
	if explicitTraceID == "" {
		explicitTraceID = strings.TrimSpace(req.TraceIDAlias)
	}
	referenceTaskIDs := callRequestReferenceTaskIDs(req)

	parentContextID := a2aContextIDFromRunInput(parent.Input)
	parentTaskID := parentRunID.String()
	rootContextID := parentContextID
	traceID := ""
	mapping, err := s.queries.GetA2AContextMappingByRun(ctx, parentRunID)
	if err == nil {
		parentContextID = mapping.ProtocolContextID
		parentTaskID = mapping.ProtocolTaskID
		rootContextID = mapping.RootContextID
		traceID = mapping.TraceID
	} else if !errors.Is(err, pgx.ErrNoRows) {
		log.Warn().Err(err).Str("parent_run_id", parentRunID.String()).Msg("a2a.childRunA2AContext: parent mapping")
	}
	if parentTaskID == "" {
		parentTaskID = parentRunID.String()
	}
	contextID := explicitContextID
	if contextID == "" {
		if parentContextID != "" {
			contextID = parentContextID
		} else if rootContextID != "" {
			contextID = rootContextID
		} else {
			contextID = "ctx-" + parentRunID.String()
		}
	}
	if rootContextID == "" {
		rootContextID = contextID
	}
	if explicitTraceID != "" {
		traceID = explicitTraceID
	}
	if traceID == "" {
		traceID = rootContextID
	}
	referenceTaskIDs = appendUniqueString(referenceTaskIDs, parentTaskID)

	return &runtime.RunA2AContextRequest{
		ProtocolContextID: contextID,
		RootContextID:     rootContextID,
		ParentContextID:   parentContextID,
		ParentTaskID:      parentTaskID,
		ParentRunID:       parentRunID.String(),
		CallerAgentID:     callerAgentID.String(),
		TargetAgentID:     targetAgentID.String(),
		TraceID:           traceID,
		ReferenceTaskIDs:  referenceTaskIDs,
		Source:            "agent_delegation",
	}, nil
}

func callRequestReferenceTaskIDs(req *CallAgentRequest) []string {
	if req == nil {
		return nil
	}
	values := req.ReferenceTaskIDs
	if len(values) == 0 {
		values = req.ReferenceTaskIDsAlias
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = appendUniqueString(out, strings.TrimSpace(value))
	}
	return out
}

func appendUniqueString(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func copyMetadata(metadata map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(metadata)+1)
	for k, v := range metadata {
		out[k] = v
	}
	return out
}

func (s *Service) ListChildren(ctx context.Context, userID, parentRunID uuid.UUID) ([]ChildRunResponse, error) {
	rows, err := s.queries.ListChildRunsByParentAndUser(ctx, db.ListChildRunsByParentAndUserParams{
		ParentRunID: parentRunID, UserID: userID,
	})
	if err != nil {
		return nil, httpx.Internal("查询 Agent 协作运行失败")
	}
	items := make([]ChildRunResponse, 0, len(rows))
	for _, row := range rows {
		item := ChildRunResponse{
			ChildRunID: row.ChildRunID.String(), ParentRunID: row.ParentRunID.String(),
			CallerAgentID: row.CallerAgentID.String(), TargetAgentID: row.TargetAgentID.String(),
			CallerAgentSlug: row.CallerAgentSlug, CallerAgentName: row.CallerAgentName,
			CallerAgentTags: row.CallerAgentTags, CallerSkills: skillRefs(row.CallerSkillIDs, row.CallerSkillNames),
			TargetAgentSlug: row.TargetAgentSlug, TargetAgentName: row.TargetAgentName,
			TargetAgentTags: row.TargetAgentTags, TargetSkills: skillRefs(row.TargetSkillIDs, row.TargetSkillNames),
			Reason: row.Reason, Status: row.Status, CostCents: row.CostCents,
			DurationMs: row.DurationMs, StartedAt: row.StartedAt.UTC().Format(time.RFC3339),
			Source: row.Source, BillingMode: "free_delegation",
			A2AContext: contextRefFromChildRow(row),
		}
		if row.FinishedAt != nil {
			formatted := row.FinishedAt.UTC().Format(time.RFC3339)
			item.FinishedAt = &formatted
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *Service) ListParentRuns(ctx context.Context, userID uuid.UUID, page, size int32, search string) (*ParentRunListResponse, error) {
	if page < 1 {
		page = defaultParentPage
	}
	if size < 1 {
		size = defaultParentPageSize
	}
	if size > maxParentPageSize {
		size = maxParentPageSize
	}
	search = normalizeParentSearch(search)
	rows, err := s.queries.ListParentRunsWithDelegationsByUser(ctx, db.ListParentRunsWithDelegationsByUserParams{
		UserID: userID,
		Search: search,
		Limit:  size,
		Offset: (page - 1) * size,
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("a2a.ListParentRuns: list")
		return nil, httpx.Internal("查询 Parent 调用链失败")
	}
	total, err := s.queries.CountParentRunsWithDelegationsByUser(ctx, db.CountParentRunsWithDelegationsByUserParams{
		UserID: userID,
		Search: search,
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("a2a.ListParentRuns: count")
		return nil, httpx.Internal("查询 Parent 调用链失败")
	}
	items := make([]ParentRunSummary, 0, len(rows))
	for _, row := range rows {
		item := ParentRunSummary{
			ParentRunID: row.ParentRunID.String(), CallerAgentID: row.CallerAgentID.String(),
			CallerAgentSlug: row.CallerAgentSlug, CallerAgentName: row.CallerAgentName,
			CallerAgentTags: row.CallerAgentTags, CallerSkills: skillRefs(row.CallerSkillIDs, row.CallerSkillNames),
			Source: row.ParentSource, ActiveRuntimeTokenCount: row.ActiveRuntimeTokenCount,
			Status: row.Status, DurationMs: row.DurationMs, StartedAt: row.StartedAt.UTC().Format(time.RFC3339),
			ChildCount: row.ChildCount, SuccessfulChildCount: row.SuccessfulChildCount,
			RunningChildCount: row.RunningChildCount,
			A2AContext:        contextRefFromParentRow(row),
		}
		if row.FinishedAt != nil {
			formatted := row.FinishedAt.UTC().Format(time.RFC3339)
			item.FinishedAt = &formatted
		}
		if row.LastRuntimeTokenUsedAt != nil {
			formatted := row.LastRuntimeTokenUsedAt.UTC().Format(time.RFC3339)
			item.LastRuntimeTokenUsedAt = &formatted
		}
		items = append(items, item)
	}
	return &ParentRunListResponse{Items: items, Total: total, Page: page, Size: size}, nil
}

func normalizeParentSearch(search string) string {
	search = strings.TrimSpace(search)
	runes := []rune(search)
	if len(runes) > maxParentSearchLen {
		return string(runes[:maxParentSearchLen])
	}
	return search
}

func skillRefs(ids, names []string) []SkillRef {
	if len(ids) == 0 {
		return []SkillRef{}
	}
	items := make([]SkillRef, 0, len(ids))
	for i, id := range ids {
		name := id
		if i < len(names) && strings.TrimSpace(names[i]) != "" {
			name = names[i]
		}
		items = append(items, SkillRef{ID: id, Name: name})
	}
	return items
}

func contextRefFromChildRow(row db.ListChildRunsByParentAndUserRow) *A2AContextRef {
	if row.ProtocolContextID == "" && row.RootContextID == "" && row.TraceID == "" {
		return nil
	}
	return &A2AContextRef{
		ProtocolContextID: row.ProtocolContextID,
		ProtocolTaskID:    row.ProtocolTaskID,
		RootContextID:     row.RootContextID,
		ParentContextID:   row.ParentContextID,
		ParentTaskID:      row.ParentTaskID,
		TraceID:           row.TraceID,
		ReferenceTaskIDs:  row.ReferenceTaskIDs,
		Source:            row.ContextSource,
	}
}

func contextRefFromParentRow(row db.ListParentRunsWithDelegationsByUserRow) *A2AContextRef {
	if row.ProtocolContextID == "" && row.RootContextID == "" && row.TraceID == "" {
		return nil
	}
	return &A2AContextRef{
		ProtocolContextID: row.ProtocolContextID,
		ProtocolTaskID:    row.ProtocolTaskID,
		RootContextID:     row.RootContextID,
		TraceID:           row.TraceID,
	}
}

func (s *Service) ownerAgent(ctx context.Context, userID, agentID uuid.UUID) (db.Agent, error) {
	agent, err := s.queries.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{ID: agentID, CreatorID: userID})
	if errors.Is(err, pgx.ErrNoRows) {
		return db.Agent{}, httpx.NotFound("Agent 不存在")
	}
	if err != nil {
		return db.Agent{}, httpx.Internal("查询 Agent 失败")
	}
	return agent, nil
}

func (s *Service) verifyRuntimeToken(ctx context.Context, plaintext string) (db.AgentRuntimeToken, error) {
	plaintext = strings.TrimSpace(plaintext)
	if !credential.HasAnyPrefix(plaintext, credential.AccessTokenPrefix, credential.LegacyAgentPrefix) ||
		!credential.ValidLength(plaintext) {
		return db.AgentRuntimeToken{}, httpx.Unauthorized("访问令牌无效或已撤销")
	}
	tokens, err := s.queries.ListActiveAgentRuntimeTokensByPrefix(ctx, plaintext[:runtimeTokenPrefixLen])
	if err != nil {
		return db.AgentRuntimeToken{}, httpx.Unauthorized("访问令牌无效或已撤销")
	}
	for _, token := range tokens {
		if bcrypt.CompareHashAndPassword([]byte(token.TokenHash), []byte(plaintext)) == nil &&
			hasScope(token.Scopes, "agent:call") {
			return token, nil
		}
	}
	return db.AgentRuntimeToken{}, httpx.Unauthorized("访问令牌无效或已撤销")
}

func hasScope(scopes []string, expected string) bool {
	for _, scope := range scopes {
		if scope == expected {
			return true
		}
	}
	return false
}

func tokenResponse(token db.AgentRuntimeToken) RuntimeTokenResponse {
	resp := RuntimeTokenResponse{
		ID: token.ID.String(), AgentID: token.AgentID.String(), Name: token.Name,
		Prefix: token.Prefix, Scopes: token.Scopes,
		CreatedAt: token.CreatedAt.UTC().Format(time.RFC3339),
	}
	if token.LastUsedAt != nil {
		value := token.LastUsedAt.UTC().Format(time.RFC3339)
		resp.LastUsedAt = &value
	}
	if token.RevokedAt != nil {
		value := token.RevokedAt.UTC().Format(time.RFC3339)
		resp.RevokedAt = &value
	}
	return resp
}
