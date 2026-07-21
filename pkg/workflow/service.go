package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/executioncontract"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// Service persists workflows and executes Agent nodes through the runtime service.
type Service struct {
	queries    *db.Queries
	pool       *pgxpool.Pool
	runtime    workflowRuntime
	observer   runtime.WorkerObserver
	runUpdates runtime.RunUpdateSource
}

type workflowRuntime interface {
	Run(context.Context, uuid.UUID, *runtime.RunRequest, string) (*runtime.RunResponse, error)
	GetRun(context.Context, uuid.UUID, uuid.UUID) (*runtime.RunResponse, error)
}

const defaultWorkflowRunMaxAttempts int32 = 3

const (
	workflowInputMappingInvalidCode     = httpx.ErrorCode("WORKFLOW_INPUT_MAPPING_INVALID")
	workflowNodeInputSchemaMismatchCode = httpx.ErrorCode("WORKFLOW_NODE_INPUT_SCHEMA_MISMATCH")
)

const (
	workflowRunStatusPending  = "pending"
	workflowRunStatusRunning  = "running"
	workflowRunStatusPaused   = "paused"
	workflowRunStatusCanceled = "canceled"
	workflowRunStatusSuccess  = "success"
	workflowRunStatusFailed   = "failed"
)

const (
	runtimeRunStatusRunning  = "running"
	runtimeRunStatusPending  = "pending"
	runtimeRunStatusSuccess  = "success"
	runtimeRunStatusFailed   = "failed"
	runtimeRunStatusTimeout  = "timeout"
	runtimeRunStatusCanceled = "canceled"
)

const workflowNodeRunPollInterval = 250 * time.Millisecond
const workflowNodeRunPollMaxLoops = 240
const workflowRunClaimReleaseTimeout = 5 * time.Second
const workflowStepEvidenceWriteTimeout = 5 * time.Second

func normalizeRunStatus(status string) string {
	return strings.ToLower(strings.TrimSpace(status))
}

func NewService(pool *pgxpool.Pool, runtimeSvc *runtime.Service) *Service {
	return &Service{queries: db.New(pool), pool: pool, runtime: runtimeSvc}
}

// SetWorkerObserver installs payload-free test instrumentation only.
func (s *Service) SetWorkerObserver(observer runtime.WorkerObserver) {
	if s != nil {
		s.observer = observer
	}
}

// SetRunUpdateSource enables advisory event-driven child Run waits. The Run
// row remains authoritative and the legacy polling path remains the degraded
// fallback when the shared listener is unavailable.
func (s *Service) SetRunUpdateSource(source runtime.RunUpdateSource) {
	if s != nil {
		s.runUpdates = source
	}
}

func (s *Service) CreateWorkflow(ctx context.Context, userID uuid.UUID, req *CreateWorkflowRequest) (*WorkflowResponse, error) {
	if req == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	if len(req.Nodes) == 0 {
		return nil, httpx.BadRequest("workflow 至少需要一个 Agent 节点")
	}
	if len(req.Nodes) > 10 {
		return nil, httpx.BadRequest("workflow 节点不能超过 10 个")
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, httpx.BadRequest("workflow 名称不能为空")
	}
	description := strings.TrimSpace(req.Description)
	edges, err := normalizeWorkflowEdgesFromRequest(req.Nodes, req.Edges)
	if err != nil {
		return nil, err
	}
	if err := validateWorkflowGraphFromRequest(req.Nodes, edges); err != nil {
		return nil, err
	}
	if err := s.validateWorkflowRequestAgentsAvailable(ctx, userID, req.Nodes, false); err != nil {
		return nil, err
	}
	if err := s.validateWorkflowRequestInputMappings(ctx, req.Nodes, edges); err != nil {
		return nil, err
	}
	edgesJSON, err := json.Marshal(edges)
	if err != nil {
		return nil, httpx.BadRequest("edges 不是合法 JSON")
	}

	var workflow db.Workflow
	var nodes []db.WorkflowNode
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.queries.WithTx(tx)
		created, err := q.CreateWorkflow(ctx, db.CreateWorkflowParams{
			UserID:      userID,
			Name:        name,
			Description: description,
			Edges:       edgesJSON,
		})
		if err != nil {
			return err
		}
		workflow = created
		seen := map[string]struct{}{}
		for i, node := range req.Nodes {
			key := strings.TrimSpace(node.Key)
			if key == "" {
				return httpx.BadRequest("workflow node key 不能为空")
			}
			if _, ok := seen[key]; ok {
				return httpx.BadRequest("workflow node key 不能重复: " + key)
			}
			if node.AgentID == uuid.Nil {
				return httpx.BadRequest("workflow node agent_id 不能为空")
			}
			seen[key] = struct{}{}
			title := strings.TrimSpace(node.Title)
			if title == "" {
				title = "Agent " + key
			}
			config := node.Config
			if config == nil {
				config = map[string]interface{}{}
			}
			configJSON, err := json.Marshal(config)
			if err != nil {
				return httpx.BadRequest("node config 不是合法 JSON")
			}
			createdNode, err := q.CreateWorkflowNode(ctx, db.CreateWorkflowNodeParams{
				WorkflowID: workflow.ID,
				NodeKey:    key,
				NodeType:   "agent",
				AgentID:    node.AgentID,
				Title:      title,
				Config:     configJSON,
				Position:   int32(i),
			})
			if err != nil {
				return err
			}
			nodes = append(nodes, createdNode)
		}
		return nil
	})
	if err != nil {
		var he *httpx.HTTPError
		if errors.As(err, &he) {
			return nil, err
		}
		log.Error().Err(err).Str("user_id", userID.String()).Msg("workflow.CreateWorkflow")
		return nil, httpx.Internal("创建 workflow 失败")
	}
	resp := workflowToResponse(workflow, nodes)
	return &resp, nil
}

func (s *Service) GetWorkflow(ctx context.Context, userID, workflowID uuid.UUID) (*WorkflowResponse, error) {
	w, nodes, err := s.getWorkflowForOwner(ctx, userID, workflowID)
	if err != nil {
		return nil, err
	}
	resp := workflowToResponse(w, nodes)
	return &resp, nil
}

func (s *Service) ListWorkflows(ctx context.Context, userID uuid.UUID, limit int32) (*WorkflowListResponse, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	return s.ListWorkflowsPage(ctx, userID, "", "", "updated_desc", 1, limit)
}

// ListWorkflowsPage returns workflows with server-side search, status filter, sort, and pagination.
func (s *Service) ListWorkflowsPage(ctx context.Context, userID uuid.UUID, query, status, sort string, page, size int32) (*WorkflowListResponse, error) {
	page, size = normalizeWorkflowListPage(page, size)
	query = normalizeWorkflowListQuery(query)
	status = normalizeWorkflowListStatus(status)
	sort = normalizeWorkflowListSort(sort)
	offset := (page - 1) * size

	rows, err := s.queries.ListWorkflowsByUser(ctx, db.ListWorkflowsByUserParams{
		UserID: userID,
		Query:  query,
		Status: status,
		Sort:   sort,
		Limit:  size,
		Offset: offset,
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("workflow.ListWorkflows")
		return nil, httpx.Internal("查询 workflows 失败")
	}
	total, err := s.queries.CountWorkflowsByUser(ctx, db.CountWorkflowsByUserParams{
		UserID: userID,
		Query:  query,
		Status: status,
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("workflow.ListWorkflows: count")
		return nil, httpx.Internal("查询 workflow 数量失败")
	}
	items := make([]WorkflowResponse, 0, len(rows))
	workflowIDs := make([]uuid.UUID, 0, len(rows))
	for _, w := range rows {
		workflowIDs = append(workflowIDs, w.ID)
	}
	nodesByWorkflowID, err := s.listWorkflowNodesByWorkflowIDs(ctx, workflowIDs)
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("workflow.ListWorkflows: nodes")
		return nil, httpx.Internal("查询 workflow 节点失败")
	}
	for _, w := range rows {
		items = append(items, workflowToResponse(w, nodesByWorkflowID[w.ID]))
	}
	return &WorkflowListResponse{
		Items:        items,
		Total:        total,
		Page:         page,
		Size:         size,
		Query:        query,
		Sort:         sort,
		StatusFilter: status,
	}, nil
}

func normalizeWorkflowListPage(page, size int32) (int32, int32) {
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 20
	}
	if size > 50 {
		size = 50
	}
	return page, size
}

func normalizeWorkflowListQuery(query string) string {
	query = strings.TrimSpace(query)
	if len([]rune(query)) > 200 {
		query = string([]rune(query)[:200])
	}
	return query
}

func normalizeWorkflowListStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "active":
		return "active"
	case "archived":
		return "archived"
	default:
		return ""
	}
}

func normalizeWorkflowListSort(sort string) string {
	switch strings.ToLower(strings.TrimSpace(sort)) {
	case "updated_asc":
		return "updated_asc"
	case "created_desc":
		return "created_desc"
	case "created_asc":
		return "created_asc"
	case "name_asc":
		return "name_asc"
	case "name_desc":
		return "name_desc"
	default:
		return "updated_desc"
	}
}

func (s *Service) RunWorkflow(ctx context.Context, userID, workflowID uuid.UUID, req *RunWorkflowRequest) (*WorkflowRunResponse, error) {
	if s.runtime == nil {
		return nil, httpx.Internal("workflow runtime 未配置")
	}
	w, nodes, graph, input, inputJSON, _, err := s.prepareWorkflowExecution(ctx, userID, workflowID, req)
	if err != nil {
		return nil, err
	}
	run, err := s.queries.CreateWorkflowRun(ctx, db.CreateWorkflowRunParams{
		WorkflowID: workflowID,
		UserID:     userID,
		Input:      inputJSON,
	})
	if err != nil {
		log.Error().Err(err).Str("workflow_id", workflowID.String()).Msg("workflow.RunWorkflow: CreateWorkflowRun")
		return nil, httpx.Internal("创建 workflow_run 失败")
	}
	return s.executeWorkflowRun(ctx, w, nodes, graph, run, input)
}

// StartWorkflowRun creates a durable pending workflow run. A background worker
// will claim and execute it, so HTTP clients do not have to keep long requests open.
func (s *Service) StartWorkflowRun(ctx context.Context, userID, workflowID uuid.UUID, req *RunWorkflowRequest) (*WorkflowRunResponse, error) {
	if s.runtime == nil {
		return nil, httpx.Internal("workflow runtime 未配置")
	}
	_, _, _, _, inputJSON, maxAttempts, err := s.prepareWorkflowExecution(ctx, userID, workflowID, req)
	if err != nil {
		return nil, err
	}
	run, err := s.queries.CreatePendingWorkflowRun(ctx, db.CreatePendingWorkflowRunParams{
		WorkflowID:  workflowID,
		UserID:      userID,
		Input:       inputJSON,
		MaxAttempts: maxAttempts,
	})
	if err != nil {
		log.Error().Err(err).Str("workflow_id", workflowID.String()).Msg("workflow.StartWorkflowRun: CreatePendingWorkflowRun")
		return nil, httpx.Internal("创建异步 workflow_run 失败")
	}
	resp := workflowRunToResponse(run, nil)
	return &resp, nil
}

// ValidateExternalExecutionTarget verifies that an actor-owned workflow is safe
// for external execution. Passing uuid.Nil to the existing Agent validator
// intentionally requires every node to be public and callable.
func (s *Service) ValidateExternalExecutionTarget(ctx context.Context, targetOwnerID, workflowID uuid.UUID) (*ExternalExecutionTargetValidation, error) {
	result := &ExternalExecutionTargetValidation{UnavailableReason: "not_found"}
	w, nodes, err := s.getWorkflowForOwner(ctx, targetOwnerID, workflowID)
	if err != nil {
		var he *httpx.HTTPError
		if errors.As(err, &he) && he.Status < 500 {
			return result, nil
		}
		return nil, err
	}
	result.TargetName = w.Name
	if w.Status != "active" {
		result.UnavailableReason = "not_active"
		return result, nil
	}
	if len(nodes) == 0 {
		result.UnavailableReason = "no_nodes"
		return result, nil
	}
	if _, err := workflowGraphFromDefinition(w, nodes); err != nil {
		var he *httpx.HTTPError
		if errors.As(err, &he) && he.Status < 500 {
			result.UnavailableReason = "invalid_definition"
			return result, nil
		}
		return nil, err
	}
	if err := s.validateWorkflowStoredAgentsAvailable(ctx, uuid.Nil, nodes, true); err != nil {
		var he *httpx.HTTPError
		if errors.As(err, &he) && he.Status < 500 {
			result.UnavailableReason = "nodes_unavailable"
			return result, nil
		}
		return nil, err
	}
	contractHash, err := s.externalExecutionWorkflowContractHash(ctx, w, nodes)
	if err != nil {
		var he *httpx.HTTPError
		if errors.As(err, &he) && he.Status < 500 {
			result.UnavailableReason = "nodes_unavailable"
			return result, nil
		}
		return nil, err
	}
	result.Executable = true
	result.UnavailableReason = ""
	result.ContractHash = contractHash
	return result, nil
}

func (s *Service) externalExecutionWorkflowContractHash(ctx context.Context, w db.Workflow, nodes []db.WorkflowNode) (string, error) {
	rawEdges := []map[string]interface{}{}
	if len(w.Edges) > 0 {
		if err := json.Unmarshal(w.Edges, &rawEdges); err != nil {
			return "", httpx.BadRequest("workflow edges 不是合法 JSON")
		}
	}
	nodeKeys := make([]string, 0, len(nodes))
	for _, node := range nodes {
		nodeKeys = append(nodeKeys, node.NodeKey)
	}
	edges, err := normalizeWorkflowEdges(nodeKeys, rawEdges)
	if err != nil {
		return "", err
	}
	agentHashes := map[uuid.UUID]string{}
	contractNodes := make([]executioncontract.WorkflowNode, 0, len(nodes))
	for _, node := range nodes {
		agentHash, ok := agentHashes[node.AgentID]
		if !ok {
			agentRow, err := s.queries.GetAgentByID(ctx, node.AgentID)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return "", httpx.Conflict("workflow node Agent 当前不可用于外部执行")
				}
				return "", err
			}
			capability, err := s.queries.GetAgentCapabilityByAgentID(ctx, node.AgentID)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return "", httpx.Conflict("workflow node Agent 缺少 capability")
				}
				return "", err
			}
			inputSchema := map[string]interface{}{}
			outputSchema := map[string]interface{}{}
			if json.Unmarshal(capability.InputSchema, &inputSchema) != nil || json.Unmarshal(capability.OutputSchema, &outputSchema) != nil {
				return "", httpx.Conflict("workflow node Agent capability 无效")
			}
			mcpToolName := ""
			if agentRow.MCPToolName != nil {
				mcpToolName = *agentRow.MCPToolName
			}
			agentHash, err = executioncontract.AgentHash(executioncontract.Agent{
				ID: node.AgentID.String(), ConnectionMode: agentRow.ConnectionMode,
				EndpointURL: agentRow.EndpointURL, MCPToolName: mcpToolName,
				CapabilityVersion: capability.Version, InputSchema: inputSchema, OutputSchema: outputSchema,
			})
			if err != nil {
				return "", httpx.Conflict("workflow node Agent capability 无效")
			}
			agentHashes[node.AgentID] = agentHash
		}
		config := map[string]interface{}{}
		if len(node.Config) > 0 && json.Unmarshal(node.Config, &config) != nil {
			return "", httpx.BadRequest("workflow node config 不是合法 JSON")
		}
		contractNodes = append(contractNodes, executioncontract.WorkflowNode{
			ID: node.ID.String(), Key: node.NodeKey, Type: node.NodeType, AgentID: node.AgentID.String(),
			Config: config, Position: node.Position, AgentContractHash: agentHash,
		})
	}
	hash, err := executioncontract.WorkflowHash(executioncontract.Workflow{ID: w.ID.String(), Edges: edges, Nodes: contractNodes})
	if err != nil {
		return "", httpx.Conflict("workflow execution contract 无效")
	}
	return hash, nil
}

// StartExternalExecutionWorkflowRun keeps target ownership and result ownership
// separate. The run ID is derived from the verified caller plus its request ID,
// so retries remain idempotent without allowing cross-service collisions.
func (s *Service) StartExternalExecutionWorkflowRun(
	ctx context.Context,
	callerServiceID string,
	targetOwnerID, actorUserID, workflowID, externalRequestID uuid.UUID,
	input map[string]interface{},
) (*WorkflowRunResponse, error) {
	callerServiceID = strings.TrimSpace(callerServiceID)
	if callerServiceID == "" || targetOwnerID == uuid.Nil || actorUserID == uuid.Nil || workflowID == uuid.Nil || externalRequestID == uuid.Nil {
		return nil, httpx.BadRequest("external workflow 执行参数无效")
	}
	input, inputJSON, err := normalizeExternalExecutionWorkflowInput(input)
	if err != nil {
		return nil, httpx.BadRequest("input 不是合法 JSON")
	}
	if replay, found, err := s.lookupExternalExecutionWorkflowRun(
		ctx,
		callerServiceID,
		actorUserID,
		workflowID,
		externalRequestID,
		input,
		defaultWorkflowRunMaxAttempts,
	); err != nil || found {
		return replay, err
	}

	if s.runtime == nil {
		return nil, httpx.Internal("workflow runtime 未配置")
	}
	w, nodes, err := s.getWorkflowForOwner(ctx, targetOwnerID, workflowID)
	if err != nil {
		return nil, err
	}
	if w.Status != "active" {
		return nil, httpx.Conflict("workflow 当前不可执行")
	}
	if len(nodes) == 0 {
		return nil, httpx.Conflict("workflow 没有可执行节点")
	}
	if _, err := workflowGraphFromDefinition(w, nodes); err != nil {
		return nil, err
	}
	if err := s.validateWorkflowStoredAgentsAvailable(ctx, uuid.Nil, nodes, true); err != nil {
		return nil, err
	}
	runID := externalExecutionWorkflowRunID(callerServiceID, externalRequestID)
	run, err := s.queries.CreatePendingExternalExecutionWorkflowRun(ctx, db.CreatePendingExternalExecutionWorkflowRunParams{
		ID:          runID,
		WorkflowID:  workflowID,
		UserID:      actorUserID,
		Input:       inputJSON,
		MaxAttempts: defaultWorkflowRunMaxAttempts,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		replay, found, lookupErr := s.lookupExternalExecutionWorkflowRun(
			ctx,
			callerServiceID,
			actorUserID,
			workflowID,
			externalRequestID,
			input,
			defaultWorkflowRunMaxAttempts,
		)
		if lookupErr != nil {
			return nil, lookupErr
		}
		if found {
			return replay, nil
		}
	}
	if err != nil {
		log.Error().Err(err).Str("workflow_id", workflowID.String()).Str("external_request_id", externalRequestID.String()).Msg("workflow.StartExternalExecutionWorkflowRun")
		return nil, httpx.Internal("创建 external workflow_run 失败")
	}
	resp := workflowRunToResponse(run, nil)
	return &resp, nil
}

func externalExecutionWorkflowRunID(callerServiceID string, externalRequestID uuid.UUID) uuid.UUID {
	return uuid.NewHash(sha256.New(), uuid.NameSpaceOID, []byte(callerServiceID+"\x00"+externalRequestID.String()), 5)
}

// LookupExternalExecutionWorkflowRun performs the read-only half of external
// workflow execution idempotency. The deterministic caller/request identity is
// resolved without consulting the mutable Workflow definition. A committed
// row must still match every execution semantic; a missing row is returned as
// a clean miss and never creates a workflow run.
func (s *Service) LookupExternalExecutionWorkflowRun(
	ctx context.Context,
	callerServiceID string,
	actorUserID, workflowID, externalRequestID uuid.UUID,
	input map[string]interface{},
) (*WorkflowRunResponse, bool, error) {
	callerServiceID = strings.TrimSpace(callerServiceID)
	if callerServiceID == "" || actorUserID == uuid.Nil || workflowID == uuid.Nil || externalRequestID == uuid.Nil {
		return nil, false, httpx.BadRequest("external workflow 执行参数无效")
	}
	normalizedInput, _, err := normalizeExternalExecutionWorkflowInput(input)
	if err != nil {
		return nil, false, httpx.BadRequest("input 不是合法 JSON")
	}
	return s.lookupExternalExecutionWorkflowRun(
		ctx,
		callerServiceID,
		actorUserID,
		workflowID,
		externalRequestID,
		normalizedInput,
		defaultWorkflowRunMaxAttempts,
	)
}

func (s *Service) lookupExternalExecutionWorkflowRun(
	ctx context.Context,
	callerServiceID string,
	actorUserID, workflowID, externalRequestID uuid.UUID,
	input map[string]interface{},
	maxAttempts int32,
) (*WorkflowRunResponse, bool, error) {
	runID := externalExecutionWorkflowRunID(callerServiceID, externalRequestID)
	run, err := s.queries.GetWorkflowRunByID(ctx, runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		log.Error().Err(err).
			Str("workflow_id", workflowID.String()).
			Str("external_request_id", externalRequestID.String()).
			Msg("workflow.LookupExternalExecutionWorkflowRun")
		return nil, false, httpx.Internal("查询 external workflow_run 失败")
	}
	if !externalExecutionWorkflowRunMatches(run, workflowID, actorUserID, input, maxAttempts) {
		return nil, false, httpx.Conflict("external_request_id 已用于其他 workflow 执行")
	}

	resp := workflowRunToResponse(run, nil)
	return &resp, true, nil
}

func normalizeExternalExecutionWorkflowInput(input map[string]interface{}) (map[string]interface{}, []byte, error) {
	if input == nil {
		input = map[string]interface{}{}
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, nil, err
	}
	normalized := map[string]interface{}{}
	if err := json.Unmarshal(encoded, &normalized); err != nil {
		return nil, nil, err
	}
	return normalized, encoded, nil
}

func externalExecutionWorkflowRunMatches(run db.WorkflowRun, workflowID, actorUserID uuid.UUID, input map[string]interface{}, maxAttempts int32) bool {
	if run.WorkflowID != workflowID || run.UserID != actorUserID || run.MaxAttempts != maxAttempts {
		return false
	}
	existing := map[string]interface{}{}
	if len(run.Input) > 0 && json.Unmarshal(run.Input, &existing) != nil {
		return false
	}
	normalizedInput, _, err := normalizeExternalExecutionWorkflowInput(input)
	if err != nil {
		return false
	}
	return reflect.DeepEqual(existing, normalizedInput)
}

func (s *Service) RetryWorkflowRun(ctx context.Context, userID, workflowRunID uuid.UUID) (*WorkflowRunResponse, error) {
	run, err := s.getWorkflowRunForOwner(ctx, userID, workflowRunID)
	if err != nil {
		return nil, err
	}
	if run.Status != workflowRunStatusFailed {
		return nil, httpx.Conflict("只有 failed workflow_run 可以重试")
	}
	input := map[string]interface{}{}
	if len(run.Input) > 0 {
		if err := json.Unmarshal(run.Input, &input); err != nil {
			return nil, httpx.Internal("workflow_run input 不是合法 JSON")
		}
	}
	return s.StartWorkflowRun(ctx, userID, run.WorkflowID, &RunWorkflowRequest{
		Input:       input,
		MaxAttempts: run.MaxAttempts,
	})
}

func (s *Service) RerunWorkflowStep(ctx context.Context, userID, workflowRunID uuid.UUID, req *RerunWorkflowStepRequest) (*WorkflowStepRerunResponse, error) {
	if s.runtime == nil {
		return nil, httpx.Internal("workflow runtime 未配置")
	}
	if req == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	nodeKey := strings.TrimSpace(req.NodeKey)
	if nodeKey == "" {
		return nil, httpx.BadRequest("node_key 不能为空")
	}
	sourceRun, err := s.getWorkflowRunForOwner(ctx, userID, workflowRunID)
	if err != nil {
		return nil, err
	}
	switch sourceRun.Status {
	case workflowRunStatusSuccess, workflowRunStatusFailed:
	default:
		return nil, httpx.Conflict("只有 success / failed workflow_run 可以 step 级重跑")
	}
	w, nodes, err := s.getWorkflowForOwner(ctx, userID, sourceRun.WorkflowID)
	if err != nil {
		return nil, err
	}
	graph, err := workflowGraphFromDefinition(w, nodes)
	if err != nil {
		return nil, err
	}
	nodeByKey := map[string]db.WorkflowNode{}
	for _, node := range nodes {
		nodeByKey[node.NodeKey] = node
	}
	if _, ok := nodeByKey[nodeKey]; !ok {
		return nil, httpx.NotFound("workflow step 不存在")
	}

	sourceSteps, err := s.queries.ListWorkflowRunSteps(ctx, sourceRun.ID)
	if err != nil {
		log.Error().Err(err).Str("workflow_run_id", sourceRun.ID.String()).Msg("workflow.RerunWorkflowStep: source steps")
		return nil, httpx.Internal("查询 workflow_run_steps 失败")
	}
	sourceStepByKey := latestWorkflowStepByNodeKey(sourceSteps)
	if _, ok := sourceStepByKey[nodeKey]; !ok {
		return nil, httpx.Conflict("source workflow_run 尚未执行该 step，请重跑已执行过的 step 或其失败父节点")
	}
	affected := workflowAffectedNodeKeys(graph, nodeKey)
	for _, node := range nodes {
		if _, shouldRerun := affected[node.NodeKey]; shouldRerun {
			continue
		}
		step, ok := sourceStepByKey[node.NodeKey]
		if !ok || step.Status != workflowRunStatusSuccess {
			return nil, httpx.Conflict("非重跑路径上的 step 必须已成功，无法复用: " + node.NodeKey)
		}
	}

	input := map[string]interface{}{}
	if len(sourceRun.Input) > 0 {
		if err := json.Unmarshal(sourceRun.Input, &input); err != nil {
			return nil, httpx.Internal("workflow_run input 不是合法 JSON")
		}
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return nil, httpx.BadRequest("input 不是合法 JSON")
	}
	rerun, err := s.queries.CreateWorkflowRun(ctx, db.CreateWorkflowRunParams{
		WorkflowID: sourceRun.WorkflowID,
		UserID:     userID,
		Input:      inputJSON,
	})
	if err != nil {
		log.Error().Err(err).Str("workflow_run_id", sourceRun.ID.String()).Msg("workflow.RerunWorkflowStep: CreateWorkflowRun")
		return nil, httpx.Internal("创建 step rerun workflow_run 失败")
	}

	outputsByNode := map[string]map[string]interface{}{}
	reusedKeys := []string{}
	rerunKeys := []string{}
	for _, level := range graph.Levels {
		for _, node := range level {
			if _, shouldRerun := affected[node.NodeKey]; !shouldRerun {
				sourceStep := sourceStepByKey[node.NodeKey]
				output, err := workflowStepOutputMap(sourceStep)
				if err != nil {
					_ = s.markWorkflowRunFailedStatus(ctx, rerun.ID, err.Error())
					return nil, httpx.Internal(err.Error())
				}
				if err := s.copyWorkflowRunStep(ctx, rerun.ID, sourceStep); err != nil {
					_ = s.markWorkflowRunFailedStatus(ctx, rerun.ID, err.Error())
					return nil, err
				}
				outputsByNode[node.NodeKey] = output
				reusedKeys = append(reusedKeys, node.NodeKey)
				continue
			}

			stepInput := workflowStepInput(input, outputsByNode, graph.Parents[node.NodeKey], node)
			result := s.runWorkflowNode(ctx, userID, w, rerun, node, stepInput, graph.Sequence[node.NodeKey])
			rerunKeys = append(rerunKeys, node.NodeKey)
			if result.Err != nil {
				if err := s.markWorkflowRunFailedStatus(ctx, rerun.ID, result.Err.Error()); err != nil {
					return nil, err
				}
				runResp, err := s.GetWorkflowRun(ctx, userID, rerun.ID)
				if err != nil {
					return nil, err
				}
				comparison, err := s.CompareWorkflowRuns(ctx, userID, sourceRun.ID, rerun.ID)
				if err != nil {
					return nil, err
				}
				return &WorkflowStepRerunResponse{
					SourceRunID:    sourceRun.ID.String(),
					RerunRunID:     rerun.ID.String(),
					NodeKey:        nodeKey,
					ReusedNodeKeys: reusedKeys,
					RerunNodeKeys:  rerunKeys,
					Run:            *runResp,
					Comparison:     *comparison,
				}, nil
			}
			outputsByNode[result.NodeKey] = result.Output
		}
	}

	finalOutput := workflowFinalOutput(outputsByNode, graph.Sinks)
	finalJSON, _ := json.Marshal(finalOutput)
	if _, err := s.queries.MarkWorkflowRunSuccess(ctx, db.MarkWorkflowRunSuccessParams{
		ID:     rerun.ID,
		Output: finalJSON,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.Conflict("step rerun workflow_run 已不是 running 状态")
		}
		log.Error().Err(err).Str("workflow_run_id", rerun.ID.String()).Msg("workflow.RerunWorkflowStep: MarkWorkflowRunSuccess")
		return nil, httpx.Internal("更新 step rerun workflow_run 失败")
	}
	runResp, err := s.GetWorkflowRun(ctx, userID, rerun.ID)
	if err != nil {
		return nil, err
	}
	comparison, err := s.CompareWorkflowRuns(ctx, userID, sourceRun.ID, rerun.ID)
	if err != nil {
		return nil, err
	}
	return &WorkflowStepRerunResponse{
		SourceRunID:    sourceRun.ID.String(),
		RerunRunID:     rerun.ID.String(),
		NodeKey:        nodeKey,
		ReusedNodeKeys: reusedKeys,
		RerunNodeKeys:  rerunKeys,
		Run:            *runResp,
		Comparison:     *comparison,
	}, nil
}

func (s *Service) CompareWorkflowRuns(ctx context.Context, userID, baseRunID, candidateRunID uuid.UUID) (*WorkflowRunComparisonResponse, error) {
	baseRun, err := s.getWorkflowRunForOwner(ctx, userID, baseRunID)
	if err != nil {
		return nil, err
	}
	candidateRun, err := s.getWorkflowRunForOwner(ctx, userID, candidateRunID)
	if err != nil {
		return nil, err
	}
	if baseRun.WorkflowID != candidateRun.WorkflowID {
		return nil, httpx.Conflict("只能对比同一个 workflow 的运行")
	}
	baseSteps, err := s.queries.ListWorkflowRunSteps(ctx, baseRun.ID)
	if err != nil {
		log.Error().Err(err).Str("workflow_run_id", baseRun.ID.String()).Msg("workflow.CompareWorkflowRuns: base steps")
		return nil, httpx.Internal("查询 workflow_run_steps 失败")
	}
	candidateSteps, err := s.queries.ListWorkflowRunSteps(ctx, candidateRun.ID)
	if err != nil {
		log.Error().Err(err).Str("workflow_run_id", candidateRun.ID.String()).Msg("workflow.CompareWorkflowRuns: candidate steps")
		return nil, httpx.Internal("查询 workflow_run_steps 失败")
	}
	resp := compareWorkflowRuns(baseRun, candidateRun, baseSteps, candidateSteps)
	return &resp, nil
}

func (s *Service) PauseWorkflowRun(ctx context.Context, userID, workflowRunID uuid.UUID) (*WorkflowRunResponse, error) {
	run, err := s.getWorkflowRunForOwner(ctx, userID, workflowRunID)
	if err != nil {
		return nil, err
	}
	switch run.Status {
	case workflowRunStatusPaused:
		return s.GetWorkflowRun(ctx, userID, workflowRunID)
	case workflowRunStatusPending:
	case workflowRunStatusRunning:
		if run.ClaimedAt == nil {
			return nil, httpx.Conflict("同步执行中的 workflow_run 不支持暂停")
		}
	default:
		return nil, httpx.Conflict("只有 pending / running workflow_run 可以暂停")
	}
	if _, err := s.queries.PauseWorkflowRun(ctx, workflowRunID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return s.GetWorkflowRun(ctx, userID, workflowRunID)
		}
		log.Error().Err(err).Str("workflow_run_id", workflowRunID.String()).Msg("workflow.PauseWorkflowRun")
		return nil, httpx.Internal("暂停 workflow_run 失败")
	}
	return s.GetWorkflowRun(ctx, userID, workflowRunID)
}

func (s *Service) ResumeWorkflowRun(ctx context.Context, userID, workflowRunID uuid.UUID) (*WorkflowRunResponse, error) {
	run, err := s.getWorkflowRunForOwner(ctx, userID, workflowRunID)
	if err != nil {
		return nil, err
	}
	if run.Status != workflowRunStatusPaused {
		return nil, httpx.Conflict("只有 paused workflow_run 可以恢复")
	}
	if run.ClaimedAt != nil {
		return nil, httpx.Conflict("workflow_run 仍在完成暂停，请稍后再恢复")
	}
	if _, err := s.queries.ResumeWorkflowRun(ctx, workflowRunID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			current, getErr := s.GetWorkflowRun(ctx, userID, workflowRunID)
			if getErr != nil {
				return nil, getErr
			}
			if current.Status == workflowRunStatusPaused && current.ClaimedAt != "" {
				return nil, httpx.Conflict("workflow_run 仍在完成暂停，请稍后再恢复")
			}
			return current, nil
		}
		log.Error().Err(err).Str("workflow_run_id", workflowRunID.String()).Msg("workflow.ResumeWorkflowRun")
		return nil, httpx.Internal("恢复 workflow_run 失败")
	}
	return s.GetWorkflowRun(ctx, userID, workflowRunID)
}

func (s *Service) CancelWorkflowRun(ctx context.Context, userID, workflowRunID uuid.UUID) (*WorkflowRunResponse, error) {
	// Lightweight query fakes used by the pure service tests do not expose a
	// transaction pool. Production always takes the durable evidence path below.
	if s.pool == nil {
		return s.cancelWorkflowRunCompatibility(ctx, userID, workflowRunID)
	}
	evidence, err := s.requestWorkflowCancellation(
		ctx, userID, workflowRunID, "OWNER_CANCEL_REQUESTED",
	)
	if err != nil {
		return nil, err
	}
	if evidence.State == "not_applied" {
		return nil, httpx.Conflict("只有 pending / running / paused workflow_run 可以取消")
	}
	if evidence.State == "requested" || evidence.State == "stopping" {
		if err := s.reconcileWorkflowCancellation(ctx, userID, workflowRunID); err != nil {
			log.Error().Err(err).Str("workflow_run_id", workflowRunID.String()).Msg("workflow.CancelWorkflowRun: reconcile")
			return nil, httpx.Internal("workflow child 取消状态正在确认")
		}
	}
	return s.GetWorkflowRun(ctx, userID, workflowRunID)
}

func (s *Service) cancelWorkflowRunCompatibility(ctx context.Context, userID, workflowRunID uuid.UUID) (*WorkflowRunResponse, error) {
	run, err := s.getWorkflowRunForOwner(ctx, userID, workflowRunID)
	if err != nil {
		return nil, err
	}
	switch run.Status {
	case workflowRunStatusCanceled:
		return s.GetWorkflowRun(ctx, userID, workflowRunID)
	case workflowRunStatusPending, workflowRunStatusRunning, workflowRunStatusPaused:
	default:
		return nil, httpx.Conflict("只有 pending / running / paused workflow_run 可以取消")
	}
	if _, err := s.queries.CancelWorkflowRun(ctx, workflowRunID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return s.GetWorkflowRun(ctx, userID, workflowRunID)
		}
		log.Error().Err(err).Str("workflow_run_id", workflowRunID.String()).Msg("workflow.CancelWorkflowRun")
		return nil, httpx.Internal("取消 workflow_run 失败")
	}
	return s.GetWorkflowRun(ctx, userID, workflowRunID)
}

func (s *Service) ListWorkflowRuns(ctx context.Context, userID, workflowID uuid.UUID, limit int32) (*WorkflowRunListResponse, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	return s.ListWorkflowRunsPage(ctx, userID, workflowID, "", "", "created_desc", 1, limit)
}

// ListWorkflowRunsPage returns workflow run history with server-side search,
// status filtering, sorting, and pagination.
func (s *Service) ListWorkflowRunsPage(ctx context.Context, userID, workflowID uuid.UUID, query, status, sort string, page, size int32) (*WorkflowRunListResponse, error) {
	page, size = normalizeWorkflowListPage(page, size)
	query = normalizeWorkflowListQuery(query)
	status = normalizeWorkflowRunStatusFilter(status)
	sort = normalizeWorkflowRunListSort(sort)
	offset := (page - 1) * size
	if _, _, err := s.getWorkflowForOwner(ctx, userID, workflowID); err != nil {
		return nil, err
	}
	rows, err := s.queries.ListWorkflowRunsByWorkflow(ctx, db.ListWorkflowRunsByWorkflowParams{
		WorkflowID: workflowID,
		Query:      query,
		Status:     status,
		Sort:       sort,
		Limit:      size,
		Offset:     offset,
		UserID:     userID,
	})
	if err != nil {
		log.Error().Err(err).Str("workflow_id", workflowID.String()).Msg("workflow.ListWorkflowRuns")
		return nil, httpx.Internal("查询 workflow_runs 失败")
	}
	total, err := s.queries.CountWorkflowRunsByWorkflow(ctx, db.CountWorkflowRunsByWorkflowParams{
		WorkflowID: workflowID,
		Query:      query,
		Status:     status,
		UserID:     userID,
	})
	if err != nil {
		log.Error().Err(err).Str("workflow_id", workflowID.String()).Msg("workflow.ListWorkflowRuns: count")
		return nil, httpx.Internal("查询 workflow_run 数量失败")
	}
	items := make([]WorkflowRunResponse, 0, len(rows))
	runIDs := make([]uuid.UUID, 0, len(rows))
	for _, run := range rows {
		runIDs = append(runIDs, run.ID)
	}
	stepsByRunID, err := s.listWorkflowRunStepsByRunIDs(ctx, runIDs)
	if err != nil {
		log.Error().Err(err).Str("workflow_id", workflowID.String()).Msg("workflow.ListWorkflowRuns: steps")
		return nil, httpx.Internal("查询 workflow_run_steps 失败")
	}
	for _, run := range rows {
		items = append(items, workflowRunToResponse(run, stepsByRunID[run.ID]))
	}
	return &WorkflowRunListResponse{
		Items:        items,
		Total:        total,
		Page:         page,
		Size:         size,
		Query:        query,
		Sort:         sort,
		StatusFilter: status,
	}, nil
}

func normalizeWorkflowRunStatusFilter(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case workflowRunStatusPending:
		return workflowRunStatusPending
	case workflowRunStatusRunning:
		return workflowRunStatusRunning
	case workflowRunStatusPaused:
		return workflowRunStatusPaused
	case workflowRunStatusCanceled:
		return workflowRunStatusCanceled
	case workflowRunStatusSuccess:
		return workflowRunStatusSuccess
	case workflowRunStatusFailed:
		return workflowRunStatusFailed
	default:
		return ""
	}
}

func normalizeWorkflowRunListSort(sort string) string {
	switch strings.ToLower(strings.TrimSpace(sort)) {
	case "created_asc":
		return "created_asc"
	case "updated_desc":
		return "updated_desc"
	case "updated_asc":
		return "updated_asc"
	case "finished_desc":
		return "finished_desc"
	case "finished_asc":
		return "finished_asc"
	case "status_asc":
		return "status_asc"
	case "status_desc":
		return "status_desc"
	default:
		return "created_desc"
	}
}

func (s *Service) prepareWorkflowExecution(
	ctx context.Context,
	userID, workflowID uuid.UUID,
	req *RunWorkflowRequest,
) (db.Workflow, []db.WorkflowNode, *workflowGraph, map[string]interface{}, []byte, int32, error) {
	w, nodes, err := s.getWorkflowForOwner(ctx, userID, workflowID)
	if err != nil {
		return db.Workflow{}, nil, nil, nil, nil, 0, err
	}
	if len(nodes) == 0 {
		return db.Workflow{}, nil, nil, nil, nil, 0, httpx.BadRequest("workflow 没有可执行节点")
	}
	graph, err := workflowGraphFromDefinition(w, nodes)
	if err != nil {
		return db.Workflow{}, nil, nil, nil, nil, 0, err
	}
	input := map[string]interface{}{}
	maxAttempts := defaultWorkflowRunMaxAttempts
	if req != nil {
		if req.Input != nil {
			input = req.Input
		}
		if req.MaxAttempts > 0 {
			if req.MaxAttempts > 10 {
				return db.Workflow{}, nil, nil, nil, nil, 0, httpx.Unprocessable("max_attempts 不能超过 10")
			}
			maxAttempts = req.MaxAttempts
		}
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return db.Workflow{}, nil, nil, nil, nil, 0, httpx.BadRequest("input 不是合法 JSON")
	}
	if err := s.validateWorkflowStoredAgentsAvailable(ctx, userID, nodes, true); err != nil {
		return db.Workflow{}, nil, nil, nil, nil, 0, err
	}
	return w, nodes, graph, input, inputJSON, maxAttempts, nil
}

func (s *Service) executeWorkflowRun(
	ctx context.Context,
	w db.Workflow,
	nodes []db.WorkflowNode,
	graph *workflowGraph,
	run db.WorkflowRun,
	input map[string]interface{},
) (*WorkflowRunResponse, error) {
	outputsByNode := map[string]map[string]interface{}{}
	for _, level := range graph.Levels {
		if stopped, resp, err := s.workflowRunStopped(ctx, run.UserID, run.ID); stopped || err != nil {
			return resp, err
		}
		levelCtx, cancelLevel := context.WithCancel(ctx)
		results := make(chan workflowNodeRunResult, len(level))
		var wg sync.WaitGroup
		for _, node := range level {
			node := node
			stepInput := workflowStepInput(input, outputsByNode, graph.Parents[node.NodeKey], node)
			sequence := graph.Sequence[node.NodeKey]
			wg.Add(1)
			go func() {
				defer wg.Done()
				result := s.runWorkflowNode(levelCtx, run.UserID, w, run, node, stepInput, sequence)
				if result.Err != nil {
					cancelLevel()
				}
				results <- result
			}()
		}
		wg.Wait()
		cancelLevel()
		close(results)
		var levelErr error
		for result := range results {
			if result.Err != nil {
				levelErr = preferWorkflowNodeError(levelErr, result.Err)
				continue
			}
			outputsByNode[result.NodeKey] = result.Output
		}
		if levelErr != nil {
			if stopped, resp, err := s.workflowRunStopped(ctx, run.UserID, run.ID); stopped || err != nil {
				return resp, err
			}
			return nil, s.failWorkflowRun(ctx, run.ID, levelErr.Error())
		}
		if stopped, resp, err := s.workflowRunStopped(ctx, run.UserID, run.ID); stopped || err != nil {
			return resp, err
		}
	}
	finalOutput := workflowFinalOutput(outputsByNode, graph.Sinks)
	finalJSON, _ := json.Marshal(finalOutput)
	if _, err := s.queries.MarkWorkflowRunSuccess(ctx, db.MarkWorkflowRunSuccessParams{
		ID:     run.ID,
		Output: finalJSON,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return s.GetWorkflowRun(ctx, run.UserID, run.ID)
		}
		return nil, httpx.Internal("更新 workflow_run 失败")
	}
	return s.GetWorkflowRun(ctx, run.UserID, run.ID)
}

type workflowNodeRunResult struct {
	NodeKey string
	Output  map[string]interface{}
	Err     error
}

func (s *Service) runWorkflowNode(
	ctx context.Context,
	userID uuid.UUID,
	w db.Workflow,
	run db.WorkflowRun,
	node db.WorkflowNode,
	stepInput workflowStepInputResult,
	sequence int32,
) workflowNodeRunResult {
	if stepInput.Value == nil {
		stepInput.Value = map[string]interface{}{}
	}
	stepInputJSON, _ := json.Marshal(stepInput.Value)
	step, err := s.queries.CreateWorkflowRunStep(ctx, db.CreateWorkflowRunStepParams{
		WorkflowRunID:  run.ID,
		WorkflowNodeID: node.ID,
		NodeKey:        node.NodeKey,
		AgentID:        node.AgentID,
		Input:          stepInputJSON,
		Sequence:       sequence,
	})
	if err != nil {
		return workflowNodeRunResult{NodeKey: node.NodeKey, Err: fmt.Errorf("创建 step 失败: %w", err)}
	}
	if stepInput.Err != nil {
		msg := stepInput.Err.Error()
		_ = s.markWorkflowRunStepFailed(ctx, db.MarkWorkflowRunStepFailedParams{
			ID:           step.ID,
			ErrorMessage: &msg,
		})
		return workflowNodeRunResult{NodeKey: node.NodeKey, Err: errors.New(msg)}
	}
	if stepInput.Mapped {
		if err := s.validateMappedWorkflowNodeInput(ctx, node, stepInput.Value); err != nil {
			msg := err.Error()
			_ = s.markWorkflowRunStepFailed(ctx, db.MarkWorkflowRunStepFailedParams{
				ID:           step.ID,
				ErrorMessage: &msg,
			})
			return workflowNodeRunResult{NodeKey: node.NodeKey, Err: errors.New(msg)}
		}
	}
	runRequest := &runtime.RunRequest{
		AgentID:          node.AgentID.String(),
		Input:            stepInput.Value,
		IdempotencyKey:   workflowNodeRunIdempotencyKey(run.ID, node.NodeKey),
		CreationProtocol: "workflow",
		CreationMethod:   "node.run",
		Metadata: map[string]interface{}{
			"workflow_id":       w.ID.String(),
			"workflow_run_id":   run.ID.String(),
			"workflow_node_id":  node.ID.String(),
			"workflow_node_key": node.NodeKey,
		},
	}
	var resp *runtime.RunResponse
	if fencedRuntime, ok := s.runtime.(workflowLaunchRuntime); ok {
		identity, identityErr := fencedRuntime.PrepareRunCreationIdentity(runRequest, "api")
		if identityErr != nil {
			return workflowNodeRunResult{NodeKey: node.NodeKey, Err: identityErr}
		}
		fence, fenceErr := s.claimWorkflowChildLaunch(
			ctx, dbWorkflowRunIdentity{ID: run.ID, UserID: run.UserID},
			node.ID, step.ID, node.NodeKey, identity,
		)
		if fenceErr != nil {
			return workflowNodeRunResult{NodeKey: node.NodeKey, Err: fenceErr}
		}
		resp, err = fencedRuntime.RunWorkflowChild(ctx, userID, runRequest, "api", fence)
		if err == nil {
			childRunID, parseErr := workflowChildRunIDFromResponse(resp)
			if parseErr == nil {
				attachCtx, cancelAttach := context.WithTimeout(context.WithoutCancel(ctx), workflowStepEvidenceWriteTimeout)
				parseErr = s.attachWorkflowChildLaunch(attachCtx, fence, step.ID, childRunID)
				cancelAttach()
			}
			if parseErr != nil {
				err = parseErr
			}
		}
	} else {
		resp, err = s.runtime.Run(ctx, userID, runRequest, "api")
	}
	if err != nil {
		msg := err.Error()
		_ = s.markWorkflowRunStepFailed(ctx, db.MarkWorkflowRunStepFailedParams{
			ID:           step.ID,
			ErrorMessage: &msg,
		})
		return workflowNodeRunResult{NodeKey: node.NodeKey, Err: errors.New(msg)}
	}
	childRunID, err := workflowChildRunIDFromResponse(resp)
	if err != nil {
		msg := err.Error()
		_ = s.markWorkflowRunStepFailed(ctx, db.MarkWorkflowRunStepFailedParams{
			ID:           step.ID,
			ErrorMessage: &msg,
		})
		return workflowNodeRunResult{NodeKey: node.NodeKey, Err: errors.New(msg)}
	}
	childRunIDPtr := &childRunID
	if _, fenced := s.runtime.(workflowLaunchRuntime); !fenced {
		attachCtx, cancelAttach := context.WithTimeout(context.WithoutCancel(ctx), workflowStepEvidenceWriteTimeout)
		attached, attachErr := s.queries.AttachWorkflowRunStepRun(attachCtx, db.AttachWorkflowRunStepRunParams{
			ID:    step.ID,
			RunID: childRunID,
		})
		cancelAttach()
		if attachErr != nil {
			msg := fmt.Sprintf("关联 step 子 run 失败: %v", attachErr)
			_ = s.markWorkflowRunStepFailed(ctx, db.MarkWorkflowRunStepFailedParams{
				ID:           step.ID,
				RunID:        childRunIDPtr,
				ErrorMessage: &msg,
			})
			return workflowNodeRunResult{NodeKey: node.NodeKey, Err: errors.New(msg)}
		}
		if attached != 1 {
			msg := "workflow step 不再接受子 run 关联"
			return workflowNodeRunResult{NodeKey: node.NodeKey, Err: errors.New(msg)}
		}
	}
	resp.Status = normalizeRunStatus(resp.Status)
	if resp.Status == runtimeRunStatusRunning || resp.Status == runtimeRunStatusPending {
		completed, err := s.waitForRuntimeRunCompletion(ctx, userID, childRunID)
		if err != nil {
			msg := err.Error()
			_ = s.markWorkflowRunStepFailed(ctx, db.MarkWorkflowRunStepFailedParams{
				ID:           step.ID,
				RunID:        childRunIDPtr,
				ErrorMessage: &msg,
			})
			return workflowNodeRunResult{NodeKey: node.NodeKey, Err: errors.New(msg)}
		}
		resp = completed
	}
	if resp.Status != "success" {
		msg := strings.TrimSpace(resp.ErrorMsg)
		if msg == "" {
			msg = "workflow step " + node.NodeKey + " returned status " + resp.Status
		}
		_ = s.markWorkflowRunStepFailed(ctx, db.MarkWorkflowRunStepFailedParams{
			ID:           step.ID,
			RunID:        childRunIDPtr,
			ErrorMessage: &msg,
		})
		return workflowNodeRunResult{NodeKey: node.NodeKey, Err: errors.New(msg)}
	}
	output := resp.Output
	if output == nil {
		output = map[string]interface{}{}
	}
	outputJSON, _ := json.Marshal(output)
	if err := s.markWorkflowRunStepSuccess(ctx, db.MarkWorkflowRunStepSuccessParams{
		ID:     step.ID,
		RunID:  childRunIDPtr,
		Output: outputJSON,
	}); err != nil {
		return workflowNodeRunResult{NodeKey: node.NodeKey, Err: fmt.Errorf("更新 step 失败: %w", err)}
	}
	return workflowNodeRunResult{NodeKey: node.NodeKey, Output: output}
}

func (s *Service) markWorkflowRunStepFailed(
	ctx context.Context,
	params db.MarkWorkflowRunStepFailedParams,
) error {
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), workflowStepEvidenceWriteTimeout)
	defer cancel()
	_, err := s.queries.MarkWorkflowRunStepFailed(writeCtx, params)
	return err
}

func (s *Service) markWorkflowRunStepSuccess(
	ctx context.Context,
	params db.MarkWorkflowRunStepSuccessParams,
) error {
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), workflowStepEvidenceWriteTimeout)
	defer cancel()
	_, err := s.queries.MarkWorkflowRunStepSuccess(writeCtx, params)
	return err
}

func workflowChildRunIDFromResponse(resp *runtime.RunResponse) (uuid.UUID, error) {
	if resp == nil {
		return uuid.Nil, errors.New("workflow runtime 返回空响应")
	}
	raw := strings.TrimSpace(resp.RunID)
	if raw == "" {
		return uuid.Nil, errors.New("workflow node runID 为空")
	}
	runID, err := uuid.Parse(raw)
	if err != nil || runID == uuid.Nil {
		return uuid.Nil, errors.New("workflow node runID 无效")
	}
	return runID, nil
}

// workflowNodeRunIdempotencyKey keeps the human node key out of the wire key.
// Node keys may contain any valid Unicode text, while Run idempotency keys are
// deliberately restricted to printable ASCII. Hashing the normalized semantic
// key preserves deterministic retries without narrowing the workflow schema.
func workflowNodeRunIdempotencyKey(workflowRunID uuid.UUID, nodeKey string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(nodeKey)))
	return fmt.Sprintf("workflow/%s/node/%x", workflowRunID, digest)
}

func preferWorkflowNodeError(current, candidate error) error {
	if candidate == nil {
		return current
	}
	if current == nil {
		return candidate
	}
	if isWorkflowContextError(current) && !isWorkflowContextError(candidate) {
		return candidate
	}
	return current
}

func isWorkflowContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (s *Service) waitForRuntimeRunCompletion(ctx context.Context, userID, runID uuid.UUID) (*runtime.RunResponse, error) {
	if runID == uuid.Nil {
		return nil, fmt.Errorf("workflow node runID 为空")
	}
	if s.runUpdates != nil && s.runUpdates.Healthy() {
		subscription, err := s.runUpdates.SubscribeRun(runID)
		if err == nil {
			defer subscription.Close()
			return s.waitForRuntimeRunCompletionByEvent(ctx, userID, runID, subscription)
		}
	}
	return s.waitForRuntimeRunCompletionByPolling(ctx, userID, runID)
}

func (s *Service) waitForRuntimeRunCompletionByPolling(
	ctx context.Context,
	userID, runID uuid.UUID,
) (*runtime.RunResponse, error) {
	for i := 0; i < workflowNodeRunPollMaxLoops; i += 1 {
		if s.observer != nil {
			s.observer.ObserveWorker(runtime.WorkerObservation{
				Category: "workflow.child_run.query", Reason: "poll", BatchSize: 1,
			})
		}
		childRun, err := s.runtime.GetRun(ctx, userID, runID)
		if err != nil {
			return nil, err
		}
		childRun.Status = normalizeRunStatus(childRun.Status)
		switch childRun.Status {
		case runtimeRunStatusSuccess, runtimeRunStatusFailed, runtimeRunStatusTimeout, runtimeRunStatusCanceled:
			return childRun, nil
		case runtimeRunStatusRunning, runtimeRunStatusPending:
			sleep := time.After(workflowNodeRunPollInterval)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-sleep:
				continue
			}
		default:
			return childRun, nil
		}
	}
	return nil, fmt.Errorf("workflow node run %s did not finish within %s", runID.String(), (workflowNodeRunPollInterval * workflowNodeRunPollMaxLoops))
}

func (s *Service) waitForRuntimeRunCompletionByEvent(
	ctx context.Context,
	userID, runID uuid.UUID,
	subscription runtime.RunUpdateSubscription,
) (*runtime.RunResponse, error) {
	timeout := workflowNodeRunPollInterval * workflowNodeRunPollMaxLoops
	deadline := time.Now().Add(timeout)
	reason := "event_initial"
	for {
		if s.observer != nil {
			s.observer.ObserveWorker(runtime.WorkerObservation{
				Category: "workflow.child_run.query", Reason: reason, BatchSize: 1,
			})
		}
		childRun, err := s.runtime.GetRun(ctx, userID, runID)
		if err != nil {
			return nil, err
		}
		childRun.Status = normalizeRunStatus(childRun.Status)
		switch childRun.Status {
		case runtimeRunStatusSuccess, runtimeRunStatusFailed, runtimeRunStatusTimeout, runtimeRunStatusCanceled:
			return childRun, nil
		case runtimeRunStatusRunning, runtimeRunStatusPending:
		default:
			return childRun, nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, fmt.Errorf("workflow node run %s did not finish within %s", runID.String(), timeout)
		}
		healthCheck := 2 * time.Second
		if remaining < healthCheck {
			healthCheck = remaining
		}
		waitCtx, cancel := context.WithTimeout(ctx, healthCheck)
		waitErr := subscription.Wait(waitCtx)
		cancel()
		if waitErr == nil {
			reason = "event_wake"
			continue
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if time.Until(deadline) <= 0 {
			reason = "deadline_final"
			continue
		}
		if !s.runUpdates.Healthy() {
			return s.waitForRuntimeRunCompletionByPollingUntil(ctx, userID, runID, deadline, timeout)
		}
		if !errors.Is(waitErr, context.DeadlineExceeded) {
			// Advisory wake failures degrade to the pre-existing database path;
			// they are not Workflow business failures.
			return s.waitForRuntimeRunCompletionByPollingUntil(ctx, userID, runID, deadline, timeout)
		}
	}
}

func (s *Service) waitForRuntimeRunCompletionByPollingUntil(
	ctx context.Context,
	userID, runID uuid.UUID,
	deadline time.Time,
	timeout time.Duration,
) (*runtime.RunResponse, error) {
	for time.Now().Before(deadline) {
		if s.observer != nil {
			s.observer.ObserveWorker(runtime.WorkerObservation{
				Category: "workflow.child_run.query", Reason: "degraded_poll", BatchSize: 1,
			})
		}
		childRun, err := s.runtime.GetRun(ctx, userID, runID)
		if err != nil {
			return nil, err
		}
		childRun.Status = normalizeRunStatus(childRun.Status)
		switch childRun.Status {
		case runtimeRunStatusSuccess, runtimeRunStatusFailed, runtimeRunStatusTimeout, runtimeRunStatusCanceled:
			return childRun, nil
		case runtimeRunStatusRunning, runtimeRunStatusPending:
		default:
			return childRun, nil
		}
		wait := workflowNodeRunPollInterval
		if remaining := time.Until(deadline); remaining < wait {
			wait = remaining
		}
		if wait <= 0 {
			break
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("workflow node run %s did not finish within %s", runID.String(), timeout)
}

func (s *Service) GetWorkflowRun(ctx context.Context, userID, workflowRunID uuid.UUID) (*WorkflowRunResponse, error) {
	run, err := s.getWorkflowRunForOwner(ctx, userID, workflowRunID)
	if err != nil {
		return nil, err
	}
	steps, err := s.queries.ListWorkflowRunSteps(ctx, workflowRunID)
	if err != nil {
		log.Error().Err(err).Str("workflow_run_id", workflowRunID.String()).Msg("workflow.GetWorkflowRun: steps")
		return nil, httpx.Internal("查询 workflow_run_steps 失败")
	}
	resp := workflowRunToResponse(run, steps)
	return &resp, nil
}

func (s *Service) getWorkflowRunForOwner(ctx context.Context, userID, workflowRunID uuid.UUID) (db.WorkflowRun, error) {
	run, err := s.queries.GetWorkflowRunByID(ctx, workflowRunID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return db.WorkflowRun{}, httpx.NotFound("workflow_run 不存在")
		}
		log.Error().Err(err).Str("workflow_run_id", workflowRunID.String()).Msg("workflow.getWorkflowRunForOwner")
		return db.WorkflowRun{}, httpx.Internal("查询 workflow_run 失败")
	}
	if run.UserID != userID {
		return db.WorkflowRun{}, httpx.NotFound("workflow_run 不存在")
	}
	return run, nil
}

func (s *Service) workflowRunStopped(ctx context.Context, userID, workflowRunID uuid.UUID) (bool, *WorkflowRunResponse, error) {
	run, err := s.queries.GetWorkflowRunByID(ctx, workflowRunID)
	if err != nil {
		log.Error().Err(err).Str("workflow_run_id", workflowRunID.String()).Msg("workflow.workflowRunStopped")
		return false, nil, httpx.Internal("查询 workflow_run 状态失败")
	}
	switch run.Status {
	case workflowRunStatusPaused, workflowRunStatusCanceled:
		resp, err := s.GetWorkflowRun(ctx, userID, workflowRunID)
		return true, resp, err
	default:
		return false, nil, nil
	}
}

func (s *Service) ClaimAndRunPendingWorkflow(ctx context.Context) (bool, error) {
	if s.runtime == nil {
		return false, httpx.Internal("workflow runtime 未配置")
	}
	run, err := s.queries.ClaimPendingWorkflowRun(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		log.Error().Err(err).Msg("workflow.ClaimAndRunPendingWorkflow: claim")
		return false, httpx.Internal("认领 workflow_run 失败")
	}
	if err := s.executeClaimedWorkflowRun(ctx, run); err != nil {
		log.Warn().Err(err).Str("workflow_run_id", run.ID.String()).Msg("workflow.ClaimAndRunPendingWorkflow: execute")
	}
	return true, nil
}

func (s *Service) RequeueStaleWorkflowRuns(ctx context.Context, staleAfter time.Duration) (int64, error) {
	if staleAfter < 0 {
		staleAfter = 0
	}
	total, err := s.queries.RequeueStaleWorkflowRuns(ctx, time.Now().Add(-staleAfter))
	if err != nil {
		log.Error().Err(err).Dur("stale_after", staleAfter).Msg("workflow.RequeueStaleWorkflowRuns")
		return 0, httpx.Internal("恢复超时 workflow_run 失败")
	}
	return total, nil
}

func (s *Service) executeClaimedWorkflowRun(ctx context.Context, run db.WorkflowRun) error {
	if run.ClaimedAt != nil {
		defer s.releasePausedWorkflowRunClaim(ctx, run.ID, *run.ClaimedAt, run.AttemptCount)
	}

	w, nodes, err := s.getWorkflowDefinition(ctx, run.WorkflowID)
	if err != nil {
		return s.failWorkflowRun(ctx, run.ID, err.Error())
	}
	if len(nodes) == 0 {
		return s.failWorkflowRun(ctx, run.ID, "workflow 没有可执行节点")
	}
	graph, err := workflowGraphFromDefinition(w, nodes)
	if err != nil {
		return s.failWorkflowRun(ctx, run.ID, err.Error())
	}
	input := map[string]interface{}{}
	if len(run.Input) > 0 {
		if err := json.Unmarshal(run.Input, &input); err != nil {
			return s.failWorkflowRun(ctx, run.ID, "workflow_run input 不是合法 JSON")
		}
	}
	if run.AttemptCount > 1 {
		if err := s.queries.DeleteWorkflowRunSteps(ctx, run.ID); err != nil {
			return s.failWorkflowRun(ctx, run.ID, "清理重试 workflow steps 失败: "+err.Error())
		}
	}
	if err := s.validateWorkflowStoredAgentsAvailable(ctx, run.UserID, nodes, true); err != nil {
		return s.failWorkflowRun(ctx, run.ID, err.Error())
	}
	_, err = s.executeWorkflowRun(ctx, w, nodes, graph, run, input)
	return err
}

func (s *Service) releasePausedWorkflowRunClaim(ctx context.Context, workflowRunID uuid.UUID, claimedAt time.Time, attemptCount int32) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), workflowRunClaimReleaseTimeout)
	defer cancel()
	if _, err := s.queries.ReleasePausedWorkflowRunClaim(releaseCtx, db.ReleasePausedWorkflowRunClaimParams{
		ID:                   workflowRunID,
		ExpectedClaimedAt:    claimedAt,
		ExpectedAttemptCount: attemptCount,
	}); err != nil {
		log.Warn().Err(err).Str("workflow_run_id", workflowRunID.String()).Msg("workflow.releasePausedWorkflowRunClaim")
	}
}

func (s *Service) getWorkflowForOwner(ctx context.Context, userID, workflowID uuid.UUID) (db.Workflow, []db.WorkflowNode, error) {
	w, err := s.queries.GetWorkflowByID(ctx, workflowID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return db.Workflow{}, nil, httpx.NotFound("workflow 不存在")
		}
		log.Error().Err(err).Str("workflow_id", workflowID.String()).Msg("workflow.getWorkflowForOwner")
		return db.Workflow{}, nil, httpx.Internal("查询 workflow 失败")
	}
	if w.UserID != userID {
		return db.Workflow{}, nil, httpx.NotFound("workflow 不存在")
	}
	nodes, err := s.queries.ListWorkflowNodes(ctx, workflowID)
	if err != nil {
		log.Error().Err(err).Str("workflow_id", workflowID.String()).Msg("workflow.getWorkflowForOwner: nodes")
		return db.Workflow{}, nil, httpx.Internal("查询 workflow 节点失败")
	}
	return w, nodes, nil
}

func (s *Service) getWorkflowDefinition(ctx context.Context, workflowID uuid.UUID) (db.Workflow, []db.WorkflowNode, error) {
	w, err := s.queries.GetWorkflowByID(ctx, workflowID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return db.Workflow{}, nil, httpx.NotFound("workflow 不存在")
		}
		log.Error().Err(err).Str("workflow_id", workflowID.String()).Msg("workflow.getWorkflowDefinition")
		return db.Workflow{}, nil, httpx.Internal("查询 workflow 失败")
	}
	nodes, err := s.queries.ListWorkflowNodes(ctx, workflowID)
	if err != nil {
		log.Error().Err(err).Str("workflow_id", workflowID.String()).Msg("workflow.getWorkflowDefinition: nodes")
		return db.Workflow{}, nil, httpx.Internal("查询 workflow 节点失败")
	}
	return w, nodes, nil
}

func (s *Service) listWorkflowNodesByWorkflowIDs(ctx context.Context, workflowIDs []uuid.UUID) (map[uuid.UUID][]db.WorkflowNode, error) {
	grouped := map[uuid.UUID][]db.WorkflowNode{}
	if len(workflowIDs) == 0 {
		return grouped, nil
	}
	nodes, err := s.queries.ListWorkflowNodesByWorkflowIDs(ctx, workflowIDs)
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
		grouped[node.WorkflowID] = append(grouped[node.WorkflowID], node)
	}
	return grouped, nil
}

func (s *Service) listWorkflowRunStepsByRunIDs(ctx context.Context, runIDs []uuid.UUID) (map[uuid.UUID][]db.WorkflowRunStep, error) {
	grouped := map[uuid.UUID][]db.WorkflowRunStep{}
	if len(runIDs) == 0 {
		return grouped, nil
	}
	steps, err := s.queries.ListWorkflowRunStepsByRunIDs(ctx, runIDs)
	if err != nil {
		return nil, err
	}
	for _, step := range steps {
		grouped[step.WorkflowRunID] = append(grouped[step.WorkflowRunID], step)
	}
	return grouped, nil
}

type workflowAgentRef struct {
	NodeKey string
	Title   string
	AgentID uuid.UUID
}

func (s *Service) validateWorkflowRequestAgentsAvailable(ctx context.Context, userID uuid.UUID, nodes []WorkflowNodeRequest, requireRuntimeOnline bool) error {
	refs := make([]workflowAgentRef, 0, len(nodes))
	for _, node := range nodes {
		refs = append(refs, workflowAgentRef{
			NodeKey: strings.TrimSpace(node.Key),
			Title:   strings.TrimSpace(node.Title),
			AgentID: node.AgentID,
		})
	}
	return s.validateWorkflowAgentsAvailable(ctx, userID, refs, requireRuntimeOnline)
}

func (s *Service) validateWorkflowStoredAgentsAvailable(ctx context.Context, userID uuid.UUID, nodes []db.WorkflowNode, requireRuntimeOnline bool) error {
	refs := make([]workflowAgentRef, 0, len(nodes))
	for _, node := range nodes {
		refs = append(refs, workflowAgentRef{
			NodeKey: node.NodeKey,
			Title:   node.Title,
			AgentID: node.AgentID,
		})
	}
	return s.validateWorkflowAgentsAvailable(ctx, userID, refs, requireRuntimeOnline)
}

func (s *Service) validateWorkflowAgentsAvailable(ctx context.Context, userID uuid.UUID, refs []workflowAgentRef, requireRuntimeOnline bool) error {
	for _, ref := range refs {
		if ref.AgentID == uuid.Nil {
			return httpx.BadRequest("workflow node agent_id 不能为空")
		}
		agentRow, err := s.queries.GetAgentByID(ctx, ref.AgentID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return httpx.Conflict("workflow node Agent 当前不可用于工作流: " + workflowAgentRefLabel(ref))
			}
			log.Error().Err(err).Str("agent_id", ref.AgentID.String()).Msg("workflow.validateWorkflowAgentsAvailable: GetAgentByID")
			return httpx.Internal("校验 workflow Agent 失败")
		}
		if agentRow.LifecycleStatus != "active" || (agentRow.Visibility != "public" && agentRow.CreatorID != userID) {
			return httpx.Conflict("workflow node Agent 当前不可用于工作流: " + workflowAgentRefLabel(ref))
		}
		if agentRow.CreatorID != userID && hasReservedWorkflowAgentTag(agentRow.Tags) {
			return httpx.Conflict("workflow node Agent 当前不可用于工作流: " + workflowAgentRefLabel(ref))
		}
		callable, err := s.workflowAgentCallable(ctx, agentRow, requireRuntimeOnline)
		if err != nil {
			return err
		}
		if !callable {
			return httpx.Conflict("workflow node Agent 当前不可调用: " + workflowAgentRefLabel(ref))
		}
	}
	return nil
}

func hasReservedWorkflowAgentTag(tags []string) bool {
	for _, tag := range tags {
		switch strings.ToLower(strings.TrimSpace(tag)) {
		case "internal", "test", "testing", "validation":
			return true
		}
		switch strings.TrimSpace(tag) {
		case "内部", "测试", "验收":
			return true
		}
	}
	return false
}

func (s *Service) workflowAgentCallable(ctx context.Context, agentRow db.Agent, requireRuntimeOnline bool) (bool, error) {
	if agentRow.ConnectionMode == "runtime" {
		hasActiveSession, err := s.queries.HasActiveRuntimeSessionForAgent(ctx, db.HasActiveRuntimeSessionForAgentParams{
			AgentID:             agentRow.ID,
			RuntimeStaleAfterMs: runtime.CurrentRuntimeLivenessPolicy().SessionStaleAfter.Milliseconds(),
		})
		if err != nil {
			log.Error().Err(err).Str("agent_id", agentRow.ID.String()).Msg("workflow.workflowAgentCallable: HasActiveRuntimeSessionForAgent")
			return false, httpx.Internal("校验 Workflow 的 Runtime Worker 连接状态失败")
		}
		// Runtime Session is the current source of truth. A newly registered
		// Runtime Agent has no historical availability snapshot yet, while an
		// active Session already proves that it can accept work. Likewise, a
		// recovered Session supersedes an older unreachable snapshot.
		if hasActiveSession {
			return true, nil
		}
		if requireRuntimeOnline {
			return false, nil
		}
	}

	snapshot, err := s.queries.GetAgentAvailabilitySnapshot(ctx, agentRow.ID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		log.Error().Err(err).Str("agent_id", agentRow.ID.String()).Msg("workflow.workflowAgentCallable: GetAgentAvailabilitySnapshot")
		return false, httpx.Internal("校验 workflow Agent 可用性失败")
	}
	if snapshot.AvailabilityStatus == "unreachable" {
		return false, nil
	}
	return true, nil
}

func workflowAgentRefLabel(ref workflowAgentRef) string {
	if ref.Title != "" {
		return ref.Title
	}
	if ref.NodeKey != "" {
		return ref.NodeKey
	}
	return ref.AgentID.String()
}

func (s *Service) failWorkflowRun(ctx context.Context, workflowRunID uuid.UUID, message string) error {
	msg := truncate(message, 1000)
	if err := s.markWorkflowRunFailedStatus(ctx, workflowRunID, msg); err != nil {
		return err
	}
	return httpx.Internal("workflow 执行失败: " + msg)
}

func (s *Service) markWorkflowRunFailedStatus(ctx context.Context, workflowRunID uuid.UUID, message string) error {
	msg := truncate(message, 1000)
	_, err := s.queries.MarkWorkflowRunFailed(ctx, db.MarkWorkflowRunFailedParams{
		ID:           workflowRunID,
		ErrorMessage: &msg,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return httpx.Conflict("workflow_run 已不是 running 状态")
		}
		log.Error().Err(err).Str("workflow_run_id", workflowRunID.String()).Msg("workflow.failWorkflowRun")
		return httpx.Internal("更新 workflow_run 失败")
	}
	return nil
}

func (s *Service) copyWorkflowRunStep(ctx context.Context, workflowRunID uuid.UUID, sourceStep db.WorkflowRunStep) error {
	step, err := s.queries.CreateWorkflowRunStep(ctx, db.CreateWorkflowRunStepParams{
		WorkflowRunID:  workflowRunID,
		WorkflowNodeID: sourceStep.WorkflowNodeID,
		NodeKey:        sourceStep.NodeKey,
		AgentID:        sourceStep.AgentID,
		Input:          sourceStep.Input,
		Sequence:       sourceStep.Sequence,
	})
	if err != nil {
		log.Error().Err(err).Str("workflow_run_id", workflowRunID.String()).Str("node_key", sourceStep.NodeKey).Msg("workflow.copyWorkflowRunStep: create")
		return httpx.Internal("复制 workflow step 失败")
	}
	output := sourceStep.Output
	if len(output) == 0 {
		output = []byte(`{}`)
	}
	if _, err := s.queries.MarkWorkflowRunStepSuccess(ctx, db.MarkWorkflowRunStepSuccessParams{
		ID:     step.ID,
		RunID:  sourceStep.RunID,
		Output: output,
	}); err != nil {
		log.Error().Err(err).Str("workflow_run_id", workflowRunID.String()).Str("node_key", sourceStep.NodeKey).Msg("workflow.copyWorkflowRunStep: success")
		return httpx.Internal("复制 workflow step 输出失败")
	}
	return nil
}

type workflowGraph struct {
	Parents  map[string][]string
	Children map[string][]string
	Levels   [][]db.WorkflowNode
	Sequence map[string]int32
	Sinks    []string
}

func normalizeWorkflowEdgesFromRequest(nodes []WorkflowNodeRequest, rawEdges []map[string]interface{}) ([]map[string]interface{}, error) {
	nodeKeys := make([]string, 0, len(nodes))
	for _, node := range nodes {
		nodeKeys = append(nodeKeys, strings.TrimSpace(node.Key))
	}
	return normalizeWorkflowEdges(nodeKeys, rawEdges)
}

func validateWorkflowGraphFromRequest(nodes []WorkflowNodeRequest, edges []map[string]interface{}) error {
	dbNodes := make([]db.WorkflowNode, 0, len(nodes))
	for i, node := range nodes {
		dbNodes = append(dbNodes, db.WorkflowNode{
			NodeKey:  strings.TrimSpace(node.Key),
			AgentID:  node.AgentID,
			Position: int32(i),
		})
	}
	_, err := buildWorkflowGraph(dbNodes, edges)
	return err
}

func workflowGraphFromDefinition(w db.Workflow, nodes []db.WorkflowNode) (*workflowGraph, error) {
	rawEdges := []map[string]interface{}{}
	if len(w.Edges) > 0 {
		if err := json.Unmarshal(w.Edges, &rawEdges); err != nil {
			return nil, httpx.BadRequest("workflow edges 不是合法 JSON")
		}
	}
	nodeKeys := make([]string, 0, len(nodes))
	for _, node := range nodes {
		nodeKeys = append(nodeKeys, node.NodeKey)
	}
	edges, err := normalizeWorkflowEdges(nodeKeys, rawEdges)
	if err != nil {
		return nil, err
	}
	return buildWorkflowGraph(nodes, edges)
}

func normalizeWorkflowEdges(nodeKeys []string, rawEdges []map[string]interface{}) ([]map[string]interface{}, error) {
	if len(rawEdges) == 0 {
		return []map[string]interface{}{}, nil
	}
	known := map[string]struct{}{}
	for _, key := range nodeKeys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, exists := known[key]; exists {
			return nil, httpx.BadRequest("workflow node key 不能重复: " + key)
		}
		known[key] = struct{}{}
	}
	seen := map[string]struct{}{}
	edges := make([]map[string]interface{}, 0, len(rawEdges))
	for _, raw := range rawEdges {
		from := workflowEdgeEndpoint(raw, "from", "source", "source_key", "sourceKey")
		to := workflowEdgeEndpoint(raw, "to", "target", "target_key", "targetKey")
		if from == "" || to == "" {
			return nil, httpx.BadRequest("workflow edge 必须包含 from/to")
		}
		if from == to {
			return nil, httpx.BadRequest("workflow edge 不能连接自身: " + from)
		}
		if _, ok := known[from]; !ok {
			return nil, httpx.BadRequest("workflow edge from 不存在: " + from)
		}
		if _, ok := known[to]; !ok {
			return nil, httpx.BadRequest("workflow edge to 不存在: " + to)
		}
		key := from + "->" + to
		if _, ok := seen[key]; ok {
			return nil, httpx.BadRequest("workflow edge 不能重复: " + key)
		}
		seen[key] = struct{}{}
		edge := map[string]interface{}{"from": from, "to": to}
		for k, v := range raw {
			if isWorkflowEndpointKey(k) {
				continue
			}
			edge[k] = v
		}
		edges = append(edges, edge)
	}
	return edges, nil
}

func buildWorkflowGraph(nodes []db.WorkflowNode, edges []map[string]interface{}) (*workflowGraph, error) {
	index := map[string]int{}
	byKey := map[string]db.WorkflowNode{}
	for i, node := range nodes {
		index[node.NodeKey] = i
		byKey[node.NodeKey] = node
	}
	if len(edges) == 0 && len(nodes) > 1 {
		edges = make([]map[string]interface{}, 0, len(nodes)-1)
		for i := 1; i < len(nodes); i++ {
			edges = append(edges, map[string]interface{}{
				"from": nodes[i-1].NodeKey,
				"to":   nodes[i].NodeKey,
			})
		}
	}
	parents := map[string][]string{}
	children := map[string][]string{}
	inDegree := map[string]int{}
	for _, node := range nodes {
		parents[node.NodeKey] = []string{}
		children[node.NodeKey] = []string{}
		inDegree[node.NodeKey] = 0
	}
	for _, edge := range edges {
		from := workflowEdgeEndpoint(edge, "from")
		to := workflowEdgeEndpoint(edge, "to")
		if _, ok := byKey[from]; !ok {
			return nil, httpx.BadRequest("workflow edge from 不存在: " + from)
		}
		if _, ok := byKey[to]; !ok {
			return nil, httpx.BadRequest("workflow edge to 不存在: " + to)
		}
		children[from] = append(children[from], to)
		parents[to] = append(parents[to], from)
		inDegree[to]++
	}
	sortWorkflowKeys := func(keys []string) {
		sort.Slice(keys, func(i, j int) bool {
			return index[keys[i]] < index[keys[j]]
		})
	}
	for key := range parents {
		sortWorkflowKeys(parents[key])
		sortWorkflowKeys(children[key])
	}
	ready := make([]string, 0)
	for _, node := range nodes {
		if inDegree[node.NodeKey] == 0 {
			ready = append(ready, node.NodeKey)
		}
	}
	sortWorkflowKeys(ready)
	levels := [][]db.WorkflowNode{}
	sequence := map[string]int32{}
	processed := 0
	var seq int32
	for len(ready) > 0 {
		current := append([]string{}, ready...)
		ready = []string{}
		level := make([]db.WorkflowNode, 0, len(current))
		for _, key := range current {
			level = append(level, byKey[key])
			sequence[key] = seq
			seq++
			processed++
			for _, child := range children[key] {
				inDegree[child]--
			}
		}
		for _, node := range nodes {
			if inDegree[node.NodeKey] == 0 {
				if _, alreadySequenced := sequence[node.NodeKey]; !alreadySequenced {
					ready = append(ready, node.NodeKey)
				}
			}
		}
		sortWorkflowKeys(ready)
		levels = append(levels, level)
	}
	if processed != len(nodes) {
		return nil, httpx.BadRequest("workflow edges 存在环，无法执行 DAG")
	}
	sinks := make([]string, 0)
	for _, node := range nodes {
		if len(children[node.NodeKey]) == 0 {
			sinks = append(sinks, node.NodeKey)
		}
	}
	sortWorkflowKeys(sinks)
	return &workflowGraph{
		Parents:  parents,
		Children: children,
		Levels:   levels,
		Sequence: sequence,
		Sinks:    sinks,
	}, nil
}

func workflowEdgeEndpoint(edge map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if raw, ok := edge[key]; ok {
			if s, ok := raw.(string); ok {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

func isWorkflowEndpointKey(key string) bool {
	switch key {
	case "from", "to", "source", "target", "source_key", "target_key", "sourceKey", "targetKey":
		return true
	default:
		return false
	}
}

func workflowFinalOutput(outputsByNode map[string]map[string]interface{}, sinks []string) map[string]interface{} {
	terminalOutputs := map[string]interface{}{}
	for _, key := range sinks {
		terminalOutputs[key] = outputsByNode[key]
	}
	if len(sinks) == 1 {
		final := map[string]interface{}{}
		for k, v := range outputsByNode[sinks[0]] {
			final[k] = v
		}
		final["terminal_nodes"] = append([]string{}, sinks...)
		final["workflow_outputs"] = workflowOutputsMap(outputsByNode)
		return final
	}
	return map[string]interface{}{
		"terminal_nodes":   append([]string{}, sinks...),
		"terminal_outputs": terminalOutputs,
		"workflow_outputs": workflowOutputsMap(outputsByNode),
	}
}

func workflowOutputsMap(outputsByNode map[string]map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{}
	keys := make([]string, 0, len(outputsByNode))
	for key := range outputsByNode {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		out[key] = outputsByNode[key]
	}
	return out
}

func workflowToResponse(w db.Workflow, nodes []db.WorkflowNode) WorkflowResponse {
	edges := []map[string]interface{}{}
	_ = json.Unmarshal(w.Edges, &edges)
	out := WorkflowResponse{
		ID:          w.ID.String(),
		Name:        w.Name,
		Description: w.Description,
		Status:      w.Status,
		Nodes:       make([]WorkflowNodeResponse, 0, len(nodes)),
		Edges:       edges,
		CreatedAt:   w.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:   w.UpdatedAt.UTC().Format(time.RFC3339),
	}
	for _, node := range nodes {
		out.Nodes = append(out.Nodes, workflowNodeToResponse(node))
	}
	return out
}

func workflowNodeToResponse(node db.WorkflowNode) WorkflowNodeResponse {
	config := map[string]interface{}{}
	_ = json.Unmarshal(node.Config, &config)
	return WorkflowNodeResponse{
		ID:       node.ID.String(),
		Key:      node.NodeKey,
		Type:     node.NodeType,
		AgentID:  node.AgentID.String(),
		Title:    node.Title,
		Config:   config,
		Position: node.Position,
	}
}

func workflowRunToResponse(run db.WorkflowRun, steps []db.WorkflowRunStep) WorkflowRunResponse {
	input := map[string]interface{}{}
	_ = json.Unmarshal(run.Input, &input)
	output := map[string]interface{}{}
	if len(run.Output) > 0 {
		_ = json.Unmarshal(run.Output, &output)
	}
	resp := WorkflowRunResponse{
		ID:           run.ID.String(),
		WorkflowID:   run.WorkflowID.String(),
		Status:       run.Status,
		Input:        input,
		Output:       output,
		Steps:        make([]WorkflowRunStepResponse, 0, len(steps)),
		AttemptCount: run.AttemptCount,
		MaxAttempts:  run.MaxAttempts,
		StartedAt:    run.StartedAt.UTC().Format(time.RFC3339),
		CreatedAt:    run.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:    run.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if run.ErrorMessage != nil {
		resp.Error = *run.ErrorMessage
	}
	if run.NextRetryAt != nil {
		resp.NextRetryAt = run.NextRetryAt.UTC().Format(time.RFC3339)
	}
	if run.ClaimedAt != nil {
		resp.ClaimedAt = run.ClaimedAt.UTC().Format(time.RFC3339)
	}
	if run.LastWorkerError != nil {
		resp.LastWorkerError = *run.LastWorkerError
	}
	if run.FinishedAt != nil {
		resp.FinishedAt = run.FinishedAt.UTC().Format(time.RFC3339)
	}
	for _, step := range steps {
		resp.Steps = append(resp.Steps, workflowRunStepToResponse(step))
	}
	return resp
}

func workflowRunStepToResponse(step db.WorkflowRunStep) WorkflowRunStepResponse {
	input := map[string]interface{}{}
	_ = json.Unmarshal(step.Input, &input)
	output := map[string]interface{}{}
	if len(step.Output) > 0 {
		_ = json.Unmarshal(step.Output, &output)
	}
	resp := WorkflowRunStepResponse{
		ID:        step.ID.String(),
		NodeID:    step.WorkflowNodeID.String(),
		NodeKey:   step.NodeKey,
		AgentID:   step.AgentID.String(),
		Status:    step.Status,
		Input:     input,
		Output:    output,
		Sequence:  step.Sequence,
		StartedAt: step.StartedAt.UTC().Format(time.RFC3339),
	}
	if step.RunID != nil {
		resp.RunID = step.RunID.String()
	}
	if step.ErrorMessage != nil {
		resp.Error = *step.ErrorMessage
	}
	if step.FinishedAt != nil {
		resp.FinishedAt = step.FinishedAt.UTC().Format(time.RFC3339)
	}
	return resp
}

func latestWorkflowStepByNodeKey(steps []db.WorkflowRunStep) map[string]db.WorkflowRunStep {
	out := map[string]db.WorkflowRunStep{}
	for _, step := range steps {
		out[step.NodeKey] = step
	}
	return out
}

func workflowAffectedNodeKeys(graph *workflowGraph, nodeKey string) map[string]struct{} {
	affected := map[string]struct{}{nodeKey: {}}
	queue := []string{nodeKey}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, child := range graph.Children[current] {
			if _, ok := affected[child]; ok {
				continue
			}
			affected[child] = struct{}{}
			queue = append(queue, child)
		}
	}
	return affected
}

func workflowStepOutputMap(step db.WorkflowRunStep) (map[string]interface{}, error) {
	output := map[string]interface{}{}
	if len(step.Output) == 0 {
		return output, nil
	}
	if err := json.Unmarshal(step.Output, &output); err != nil {
		return nil, fmt.Errorf("workflow step %s output 不是合法 JSON", step.NodeKey)
	}
	return output, nil
}

func compareWorkflowRuns(baseRun, candidateRun db.WorkflowRun, baseSteps, candidateSteps []db.WorkflowRunStep) WorkflowRunComparisonResponse {
	baseByKey := latestWorkflowStepByNodeKey(baseSteps)
	candidateByKey := latestWorkflowStepByNodeKey(candidateSteps)
	resp := WorkflowRunComparisonResponse{
		BaseRunID:      baseRun.ID.String(),
		CandidateRunID: candidateRun.ID.String(),
		WorkflowID:     baseRun.WorkflowID.String(),
		StatusChanged:  baseRun.Status != candidateRun.Status,
		OutputChanged:  !jsonBytesEqual(baseRun.Output, candidateRun.Output),
		Steps:          []WorkflowRunStepCompareResponse{},
	}
	for _, key := range orderedWorkflowStepKeys(baseSteps, candidateSteps) {
		baseStep, hasBase := baseByKey[key]
		candidateStep, hasCandidate := candidateByKey[key]
		step := WorkflowRunStepCompareResponse{NodeKey: key}
		if hasBase {
			step.BaseStatus = baseStep.Status
			if baseStep.RunID != nil {
				step.BaseRunID = baseStep.RunID.String()
			}
		}
		if hasCandidate {
			step.CandidateStatus = candidateStep.Status
			if candidateStep.RunID != nil {
				step.CandidateRunID = candidateStep.RunID.String()
			}
		}
		step.StatusChanged = !hasBase || !hasCandidate || baseStep.Status != candidateStep.Status
		step.RunChanged = !hasBase || !hasCandidate || workflowRunIDString(baseStep.RunID) != workflowRunIDString(candidateStep.RunID)
		step.OutputChanged = !hasBase || !hasCandidate || !jsonBytesEqual(baseStep.Output, candidateStep.Output)
		step.ErrorChanged = !hasBase || !hasCandidate || stringPtrValue(baseStep.ErrorMessage) != stringPtrValue(candidateStep.ErrorMessage)
		step.Changed = step.StatusChanged || step.RunChanged || step.OutputChanged || step.ErrorChanged
		if step.Changed {
			resp.ChangedNodeKeys = append(resp.ChangedNodeKeys, key)
		}
		resp.Steps = append(resp.Steps, step)
	}
	return resp
}

func orderedWorkflowStepKeys(baseSteps, candidateSteps []db.WorkflowRunStep) []string {
	seen := map[string]struct{}{}
	keys := []string{}
	add := func(steps []db.WorkflowRunStep) {
		for _, step := range steps {
			if _, ok := seen[step.NodeKey]; ok {
				continue
			}
			seen[step.NodeKey] = struct{}{}
			keys = append(keys, step.NodeKey)
		}
	}
	add(baseSteps)
	add(candidateSteps)
	return keys
}

func jsonBytesEqual(left, right []byte) bool {
	return reflect.DeepEqual(jsonComparableValue(left), jsonComparableValue(right))
}

func jsonComparableValue(raw []byte) interface{} {
	if len(raw) == 0 {
		return map[string]interface{}{}
	}
	var out interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return string(raw)
	}
	return out
}

func workflowRunIDString(id *uuid.UUID) string {
	if id == nil {
		return ""
	}
	return id.String()
}

func stringPtrValue(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
