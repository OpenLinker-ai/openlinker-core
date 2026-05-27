package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	edges := req.Edges
	if edges == nil {
		edges = []map[string]interface{}{}
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
	w, nodes, err := s.getWorkflowForOwner(ctx, userID, workflowID)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, httpx.BadRequest("workflow 没有可执行节点")
	}
	input := map[string]interface{}{}
	if req != nil && req.Input != nil {
		input = req.Input
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return nil, httpx.BadRequest("input 不是合法 JSON")
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

	var previousOutput map[string]interface{}
	var previousNodeKey string
	for i, node := range nodes {
		stepInput := workflowStepInput(input, previousOutput, previousNodeKey, node)
		stepInputJSON, _ := json.Marshal(stepInput)
		step, err := s.queries.CreateWorkflowRunStep(ctx, db.CreateWorkflowRunStepParams{
			WorkflowRunID:  run.ID,
			WorkflowNodeID: node.ID,
			NodeKey:        node.NodeKey,
			AgentID:        node.AgentID,
			Input:          stepInputJSON,
			Sequence:       int32(i),
		})
		if err != nil {
			return nil, s.failWorkflowRun(ctx, run.ID, fmt.Sprintf("创建 step 失败: %v", err))
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
			return nil, s.failWorkflowRun(ctx, run.ID, msg)
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
			return nil, s.failWorkflowRun(ctx, run.ID, msg)
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
			return nil, s.failWorkflowRun(ctx, run.ID, fmt.Sprintf("更新 step 失败: %v", err))
		}
		previousOutput = output
		previousNodeKey = node.NodeKey
	}

	finalOutput := previousOutput
	if finalOutput == nil {
		finalOutput = map[string]interface{}{}
	}
	finalJSON, _ := json.Marshal(finalOutput)
	if _, err := s.queries.MarkWorkflowRunSuccess(ctx, db.MarkWorkflowRunSuccessParams{
		ID:     run.ID,
		Output: finalJSON,
	}); err != nil {
		return nil, httpx.Internal("更新 workflow_run 失败")
	}
	return s.GetWorkflowRun(ctx, userID, run.ID)
}

func (s *Service) GetWorkflowRun(ctx context.Context, userID, workflowRunID uuid.UUID) (*WorkflowRunResponse, error) {
	run, err := s.queries.GetWorkflowRunByID(ctx, workflowRunID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("workflow_run 不存在")
		}
		log.Error().Err(err).Str("workflow_run_id", workflowRunID.String()).Msg("workflow.GetWorkflowRun")
		return nil, httpx.Internal("查询 workflow_run 失败")
	}
	if run.UserID != userID {
		return nil, httpx.NotFound("workflow_run 不存在")
	}
	steps, err := s.queries.ListWorkflowRunSteps(ctx, workflowRunID)
	if err != nil {
		log.Error().Err(err).Str("workflow_run_id", workflowRunID.String()).Msg("workflow.GetWorkflowRun: steps")
		return nil, httpx.Internal("查询 workflow_run_steps 失败")
	}
	resp := workflowRunToResponse(run, steps)
	return &resp, nil
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

func (s *Service) failWorkflowRun(ctx context.Context, workflowRunID uuid.UUID, message string) error {
	msg := truncate(message, 1000)
	_, err := s.queries.MarkWorkflowRunFailed(ctx, db.MarkWorkflowRunFailedParams{
		ID:           workflowRunID,
		ErrorMessage: &msg,
	})
	if err != nil {
		log.Error().Err(err).Str("workflow_run_id", workflowRunID.String()).Msg("workflow.failWorkflowRun")
		return httpx.Internal("更新 workflow_run 失败")
	}
	return httpx.Internal("workflow 执行失败: " + msg)
}

func workflowStepInput(original, previous map[string]interface{}, previousNodeKey string, node db.WorkflowNode) map[string]interface{} {
	if previous == nil {
		return map[string]interface{}{
			"workflow_input": original,
			"node_key":       node.NodeKey,
		}
	}
	return map[string]interface{}{
		"workflow_input":  original,
		"previous_node":   previousNodeKey,
		"previous_output": previous,
		"node_key":        node.NodeKey,
	}
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
		ID:         run.ID.String(),
		WorkflowID: run.WorkflowID.String(),
		Status:     run.Status,
		Input:      input,
		Output:     output,
		Steps:      make([]WorkflowRunStepResponse, 0, len(steps)),
		StartedAt:  run.StartedAt.UTC().Format(time.RFC3339),
		CreatedAt:  run.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:  run.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if run.ErrorMessage != nil {
		resp.Error = *run.ErrorMessage
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

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
