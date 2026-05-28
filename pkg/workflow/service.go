package workflow

import (
	"context"
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

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
	"github.com/kinzhi/openlinker-core/pkg/runtime"
)

// Service persists workflows and executes Agent nodes through the runtime service.
type Service struct {
	queries *db.Queries
	pool    *pgxpool.Pool
	runtime *runtime.Service
}

const defaultWorkflowRunMaxAttempts int32 = 3

const (
	workflowRunStatusPending  = "pending"
	workflowRunStatusRunning  = "running"
	workflowRunStatusPaused   = "paused"
	workflowRunStatusCanceled = "canceled"
	workflowRunStatusSuccess  = "success"
	workflowRunStatusFailed   = "failed"
)

func NewService(pool *pgxpool.Pool, runtimeSvc *runtime.Service) *Service {
	return &Service{queries: db.New(pool), pool: pool, runtime: runtimeSvc}
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
	rows, err := s.queries.ListWorkflowsByUser(ctx, db.ListWorkflowsByUserParams{
		UserID: userID,
		Limit:  limit,
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("workflow.ListWorkflows")
		return nil, httpx.Internal("查询 workflows 失败")
	}
	total, err := s.queries.CountWorkflowsByUser(ctx, userID)
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("workflow.ListWorkflows: count")
		return nil, httpx.Internal("查询 workflow 数量失败")
	}
	items := make([]WorkflowResponse, 0, len(rows))
	for _, w := range rows {
		nodes, err := s.queries.ListWorkflowNodes(ctx, w.ID)
		if err != nil {
			log.Error().Err(err).Str("workflow_id", w.ID.String()).Msg("workflow.ListWorkflows: nodes")
			return nil, httpx.Internal("查询 workflow 节点失败")
		}
		items = append(items, workflowToResponse(w, nodes))
	}
	return &WorkflowListResponse{Items: items, Total: total}, nil
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
	case workflowRunStatusPending, workflowRunStatusRunning:
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
	if _, err := s.queries.ResumeWorkflowRun(ctx, workflowRunID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return s.GetWorkflowRun(ctx, userID, workflowRunID)
		}
		log.Error().Err(err).Str("workflow_run_id", workflowRunID.String()).Msg("workflow.ResumeWorkflowRun")
		return nil, httpx.Internal("恢复 workflow_run 失败")
	}
	return s.GetWorkflowRun(ctx, userID, workflowRunID)
}

func (s *Service) CancelWorkflowRun(ctx context.Context, userID, workflowRunID uuid.UUID) (*WorkflowRunResponse, error) {
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
	if _, _, err := s.getWorkflowForOwner(ctx, userID, workflowID); err != nil {
		return nil, err
	}
	rows, err := s.queries.ListWorkflowRunsByWorkflow(ctx, db.ListWorkflowRunsByWorkflowParams{
		WorkflowID: workflowID,
		Limit:      limit,
	})
	if err != nil {
		log.Error().Err(err).Str("workflow_id", workflowID.String()).Msg("workflow.ListWorkflowRuns")
		return nil, httpx.Internal("查询 workflow_runs 失败")
	}
	total, err := s.queries.CountWorkflowRunsByWorkflow(ctx, workflowID)
	if err != nil {
		log.Error().Err(err).Str("workflow_id", workflowID.String()).Msg("workflow.ListWorkflowRuns: count")
		return nil, httpx.Internal("查询 workflow_run 数量失败")
	}
	items := make([]WorkflowRunResponse, 0, len(rows))
	for _, run := range rows {
		steps, err := s.queries.ListWorkflowRunSteps(ctx, run.ID)
		if err != nil {
			log.Error().Err(err).Str("workflow_run_id", run.ID.String()).Msg("workflow.ListWorkflowRuns: steps")
			return nil, httpx.Internal("查询 workflow_run_steps 失败")
		}
		items = append(items, workflowRunToResponse(run, steps))
	}
	return &WorkflowRunListResponse{Items: items, Total: total}, nil
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
		results := make(chan workflowNodeRunResult, len(level))
		var wg sync.WaitGroup
		for _, node := range level {
			node := node
			stepInput := workflowStepInput(input, outputsByNode, graph.Parents[node.NodeKey], node)
			sequence := graph.Sequence[node.NodeKey]
			wg.Add(1)
			go func() {
				defer wg.Done()
				results <- s.runWorkflowNode(ctx, run.UserID, w, run, node, stepInput, sequence)
			}()
		}
		wg.Wait()
		close(results)
		for result := range results {
			if result.Err != nil {
				if stopped, resp, err := s.workflowRunStopped(ctx, run.UserID, run.ID); stopped || err != nil {
					return resp, err
				}
				return nil, s.failWorkflowRun(ctx, run.ID, result.Err.Error())
			}
			outputsByNode[result.NodeKey] = result.Output
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
	stepInput map[string]interface{},
	sequence int32,
) workflowNodeRunResult {
	stepInputJSON, _ := json.Marshal(stepInput)
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
	resp, err := s.runtime.Run(ctx, userID, &runtime.RunRequest{
		AgentID: node.AgentID.String(),
		Input:   stepInput,
		Metadata: map[string]interface{}{
			"workflow_id":       w.ID.String(),
			"workflow_run_id":   run.ID.String(),
			"workflow_node_id":  node.ID.String(),
			"workflow_node_key": node.NodeKey,
		},
	}, "api")
	if err != nil {
		msg := err.Error()
		_, _ = s.queries.MarkWorkflowRunStepFailed(ctx, db.MarkWorkflowRunStepFailedParams{
			ID:           step.ID,
			ErrorMessage: &msg,
		})
		return workflowNodeRunResult{NodeKey: node.NodeKey, Err: errors.New(msg)}
	}
	childRunID := uuid.Nil
	if resp.RunID != "" {
		childRunID, _ = uuid.Parse(resp.RunID)
	}
	childRunIDPtr := &childRunID
	if childRunID == uuid.Nil {
		childRunIDPtr = nil
	}
	if resp.Status != "success" {
		msg := strings.TrimSpace(resp.ErrorMsg)
		if msg == "" {
			msg = "workflow step " + node.NodeKey + " returned status " + resp.Status
		}
		_, _ = s.queries.MarkWorkflowRunStepFailed(ctx, db.MarkWorkflowRunStepFailedParams{
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
	if _, err := s.queries.MarkWorkflowRunStepSuccess(ctx, db.MarkWorkflowRunStepSuccessParams{
		ID:     step.ID,
		RunID:  childRunIDPtr,
		Output: outputJSON,
	}); err != nil {
		return workflowNodeRunResult{NodeKey: node.NodeKey, Err: fmt.Errorf("更新 step 失败: %w", err)}
	}
	return workflowNodeRunResult{NodeKey: node.NodeKey, Output: output}
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
	_, err = s.executeWorkflowRun(ctx, w, nodes, graph, run, input)
	return err
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

func workflowStepInput(original map[string]interface{}, outputsByNode map[string]map[string]interface{}, parents []string, node db.WorkflowNode) map[string]interface{} {
	if len(parents) == 0 {
		return map[string]interface{}{
			"workflow_input": original,
			"node_key":       node.NodeKey,
		}
	}
	dependencies := map[string]interface{}{}
	for _, key := range parents {
		dependencies[key] = outputsByNode[key]
	}
	input := map[string]interface{}{
		"workflow_input": original,
		"dependencies":   dependencies,
		"node_key":       node.NodeKey,
	}
	if len(parents) == 1 {
		input["previous_node"] = parents[0]
		input["previous_output"] = outputsByNode[parents[0]]
	}
	return input
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
