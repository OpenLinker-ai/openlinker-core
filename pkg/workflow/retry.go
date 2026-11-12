package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const (
	workflowChildrenStoppingCode    = httpx.ErrorCode("WORKFLOW_CHILDREN_STOPPING")
	workflowChildrenUnconfirmedCode = httpx.ErrorCode("WORKFLOW_CHILDREN_UNCONFIRMED")
	workflowRerunNotReusableCode    = httpx.ErrorCode("WORKFLOW_RERUN_NOT_REUSABLE")
)

// workflowStopGate is the Retry/Rerun stop threshold over every child Run
// the source run ever referenced (launches and step run_ids, including
// children carried over by earlier reruns and legacy orphan launches),
// classified by Runtime, plus the run's own open stop work.
type workflowStopGate struct {
	inProgress  []string
	unconfirmed []string
}

func (g workflowStopGate) err(acknowledged bool) error {
	if len(g.inProgress) > 0 {
		e := httpx.NewError(409, workflowChildrenStoppingCode,
			"source workflow_run 的子 run 仍在停止中，请稍后再试: "+strings.Join(g.inProgress, ", "))
		e.Details = map[string]any{"nodes": g.inProgress}
		return e
	}
	if len(g.unconfirmed) > 0 && !acknowledged {
		e := httpx.NewError(409, workflowChildrenUnconfirmedCode,
			"无法确认 source workflow_run 的子 run 已在后端停止: "+strings.Join(g.unconfirmed, ", ")+
				"；确认接受旧执行可能仍在继续后，带 acknowledge_unconfirmed_children 重新提交")
		e.Details = map[string]any{"nodes": g.unconfirmed, "acknowledge_field": "acknowledge_unconfirmed_children"}
		return e
	}
	return nil
}

// evaluateWorkflowStopGate runs in the caller's transaction, which holds the
// source run's row lock (stop reconcilers and public cancellation take the
// same lock before writing stop state).
func evaluateWorkflowStopGate(ctx context.Context, tx pgx.Tx, sourceRunID uuid.UUID) (workflowStopGate, error) {
	gate := workflowStopGate{}
	inProgress := map[string]bool{}
	unconfirmed := map[string]bool{}
	rows, err := tx.Query(ctx, `
		SELECT n.node_key FROM workflow_step_stops s JOIN workflow_nodes n ON n.id = s.workflow_node_id
		WHERE s.workflow_run_id = $1 AND s.state IN ('requested', 'stopping')
		UNION
		SELECT '(workflow cancellation)' FROM workflow_run_cancellations
		WHERE workflow_run_id = $1 AND state IN ('requested', 'stopping')
	`, sourceRunID)
	if err != nil {
		return gate, err
	}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return gate, err
		}
		inProgress[key] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return gate, err
	}
	rows, err = tx.Query(ctx, `
		SELECT DISTINCT child_id, node_key FROM (
		    SELECT run_id AS child_id, node_key FROM workflow_step_launches
		    WHERE workflow_run_id = $1 AND run_id IS NOT NULL
		    UNION ALL
		    SELECT run_id, node_key FROM workflow_run_steps
		    WHERE workflow_run_id = $1 AND run_id IS NOT NULL
		) children
		ORDER BY child_id
	`, sourceRunID)
	if err != nil {
		return gate, err
	}
	type child struct {
		id  uuid.UUID
		key string
	}
	children := []child{}
	for rows.Next() {
		var c child
		if err := rows.Scan(&c.id, &c.key); err != nil {
			rows.Close()
			return gate, err
		}
		children = append(children, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return gate, err
	}
	for _, c := range children {
		evidence, err := runtime.ClassifyWorkflowChildStop(ctx, tx, c.id)
		if err != nil {
			return gate, err
		}
		switch evidence.State {
		case runtime.WorkflowChildStopInProgress:
			inProgress[c.key] = true
		case runtime.WorkflowChildStopUnconfirmed:
			unconfirmed[c.key] = true
		}
	}
	gate.inProgress = sortedKeys(inProgress)
	gate.unconfirmed = sortedKeys(unconfirmed)
	return gate, nil
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// lockSourceWorkflowRun locks the source run for Retry/Rerun and checks
// ownership and the allowed source states.
func lockSourceWorkflowRun(ctx context.Context, tx pgx.Tx, userID, runID uuid.UUID, allowed ...string) (db.WorkflowRun, error) {
	var locked int
	if err := tx.QueryRow(ctx, `SELECT 1 FROM workflow_runs WHERE id = $1 FOR UPDATE`, runID).Scan(&locked); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return db.WorkflowRun{}, httpx.NotFound("workflow_run 不存在")
		}
		return db.WorkflowRun{}, err
	}
	run, err := db.New(tx).GetWorkflowRunByID(ctx, runID)
	if err != nil {
		return db.WorkflowRun{}, err
	}
	if run.UserID != userID {
		return db.WorkflowRun{}, httpx.NotFound("workflow_run 不存在")
	}
	for _, status := range allowed {
		if run.Status == status {
			return run, nil
		}
	}
	return db.WorkflowRun{}, httpx.Conflict("workflow_run 当前状态不能执行该操作: " + run.Status)
}

func createGatedWorkflowRun(
	ctx context.Context,
	tx pgx.Tx,
	source db.WorkflowRun,
	userID uuid.UUID,
	gate workflowStopGate,
) (db.WorkflowRun, error) {
	// Retry and Rerun execute the source run's definition version.
	var sourceVersionID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT workflow_version_id FROM workflow_runs WHERE id = $1`, source.ID).Scan(&sourceVersionID); err != nil {
		return db.WorkflowRun{}, err
	}
	run, err := db.New(tx).CreatePendingWorkflowRun(ctx, db.CreatePendingWorkflowRunParams{
		WorkflowID: source.WorkflowID, UserID: userID, Input: source.Input, MaxAttempts: source.MaxAttempts,
		WorkflowVersionID: &sourceVersionID,
	})
	if err := workflowRunCreateError(err); err != nil {
		return db.WorkflowRun{}, err
	}
	if len(gate.unconfirmed) > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE workflow_runs SET unconfirmed_children_acknowledged_by = $2 WHERE id = $1
		`, run.ID, userID); err != nil {
			return db.WorkflowRun{}, err
		}
	}
	return run, nil
}

// RetryWorkflowRun creates a pending run with the failed source run's input
// in one transaction that locks the source and checks the stop threshold.
// The new run is executed by the worker.
func (s *Service) RetryWorkflowRun(ctx context.Context, userID, workflowRunID uuid.UUID, req *RetryWorkflowRunRequest) (*WorkflowRunResponse, error) {
	source, err := s.getWorkflowRunForOwner(ctx, userID, workflowRunID)
	if err != nil {
		return nil, err
	}
	if source.Status != workflowRunStatusFailed {
		return nil, httpx.Conflict("只有 failed workflow_run 可以重试")
	}
	if _, _, err := s.getWorkflowForOwner(ctx, userID, source.WorkflowID); err != nil {
		return nil, err
	}
	if _, err := unmarshalWorkflowRunInput(source.Input); err != nil {
		return nil, httpx.Internal("workflow_run input 不是合法 JSON")
	}
	if s.pool == nil {
		return nil, httpx.Internal("workflow 存储未配置")
	}
	acknowledged := req != nil && req.AcknowledgeUnconfirmedChildren
	var created db.WorkflowRun
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		locked, err := lockSourceWorkflowRun(ctx, tx, userID, workflowRunID, workflowRunStatusFailed)
		if err != nil {
			return err
		}
		gate, err := evaluateWorkflowStopGate(ctx, tx, workflowRunID)
		if err != nil {
			return err
		}
		if err := gate.err(acknowledged); err != nil {
			return err
		}
		created, err = createGatedWorkflowRun(ctx, tx, locked, userID, gate)
		return err
	})
	if err != nil {
		return nil, workflowGateError(err, "workflow.RetryWorkflowRun")
	}
	return s.GetWorkflowRun(ctx, userID, created.ID)
}

func workflowGateError(err error, op string) error {
	var httpErr *httpx.HTTPError
	if errors.As(err, &httpErr) {
		return err
	}
	log.Error().Err(err).Msg(op)
	return httpx.Internal("创建 workflow_run 失败")
}

type sourceLogicalNode struct {
	nodeID     uuid.UUID
	state      string
	attempt    *int32
	skipReason *string
}

// RerunWorkflowStep creates a pending run that re-executes the target node
// and its descendants and reuses every other node's logical result from the
// source run. In one transaction it locks the source, checks the stop
// threshold and the reuse rules, creates the run and copies the reused
// nodes (logical state and effective attempt, source=reused, keeping the
// source child Run and output). The worker executes the rest.
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
	w, nodes, err := s.getWorkflowRunDefinition(ctx, sourceRun)
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
	affected := workflowAffectedNodeKeys(graph, nodeKey)
	acknowledged := req.AcknowledgeUnconfirmedChildren
	if s.pool == nil {
		return nil, httpx.Internal("workflow 存储未配置")
	}

	var created db.WorkflowRun
	reusedKeys, rerunKeys := []string{}, []string{}
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		locked, err := lockSourceWorkflowRun(ctx, tx, userID, workflowRunID, workflowRunStatusSuccess, workflowRunStatusFailed)
		if err != nil {
			return err
		}
		gate, err := evaluateWorkflowStopGate(ctx, tx, workflowRunID)
		if err != nil {
			return err
		}
		if err := gate.err(acknowledged); err != nil {
			return err
		}
		logical, err := loadSourceLogicalNodes(ctx, tx, workflowRunID)
		if err != nil {
			return err
		}
		if _, ok := logical[nodeKey]; !ok {
			return httpx.Conflict("source workflow_run 尚未执行该 step，请重跑已执行过的 step 或其失败父节点")
		}
		for _, key := range workflowTopologicalOrder(graph) {
			if _, rerun := affected[key]; rerun {
				rerunKeys = append(rerunKeys, key)
				continue
			}
			node, ok := logical[key]
			if !ok || !s.workflowNodeReusable(node.state, nodeByKey[key].Config) {
				e := httpx.NewError(409, workflowRerunNotReusableCode,
					"非重跑路径上的 step 不能复用，请改为重跑该 step 或其祖先: "+key)
				e.Details = map[string]any{"node_key": key}
				return e
			}
			reusedKeys = append(reusedKeys, key)
		}
		created, err = createGatedWorkflowRun(ctx, tx, locked, userID, gate)
		if err != nil {
			return err
		}
		for _, key := range reusedKeys {
			if err := copyReusedWorkflowNode(ctx, tx, workflowRunID, created.ID, logical[key]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, workflowGateError(err, "workflow.RerunWorkflowStep")
	}
	runResp, err := s.GetWorkflowRun(ctx, userID, created.ID)
	if err != nil {
		return nil, err
	}
	comparison, err := s.CompareWorkflowRuns(ctx, userID, sourceRun.ID, created.ID)
	if err != nil {
		return nil, err
	}
	return &WorkflowStepRerunResponse{
		SourceRunID:    sourceRun.ID.String(),
		RerunRunID:     created.ID.String(),
		NodeKey:        nodeKey,
		ReusedNodeKeys: reusedKeys,
		RerunNodeKeys:  rerunKeys,
		Run:            *runResp,
		Comparison:     *comparison,
	}, nil
}

// workflowNodeReusable: a node outside the rerun set is reused only when its
// source logical result is final and did not fail the workflow.
func (s *Service) workflowNodeReusable(state string, config []byte) bool {
	switch state {
	case nodeSuccess, nodeSkipped:
		return true
	case nodeFailed, nodeTimeout:
		return s.workflowNodePolicy(config).onFailure != onFailureFailWorkflow
	}
	return false
}

func loadSourceLogicalNodes(ctx context.Context, tx pgx.Tx, runID uuid.UUID) (map[string]sourceLogicalNode, error) {
	rows, err := tx.Query(ctx, `
		SELECT node_key, workflow_node_id, state, effective_attempt, skip_reason
		FROM workflow_run_nodes WHERE workflow_run_id = $1
	`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	nodes := map[string]sourceLogicalNode{}
	for rows.Next() {
		var key string
		var node sourceLogicalNode
		if err := rows.Scan(&key, &node.nodeID, &node.state, &node.attempt, &node.skipReason); err != nil {
			return nil, err
		}
		nodes[key] = node
	}
	return nodes, rows.Err()
}

func copyReusedWorkflowNode(ctx context.Context, tx pgx.Tx, sourceRunID, newRunID uuid.UUID, node sourceLogicalNode) error {
	var effective *int32
	if node.attempt != nil {
		tag, err := tx.Exec(ctx, `
			INSERT INTO workflow_run_steps (
			    workflow_run_id, workflow_node_id, node_key, agent_id, run_id, status, input, output,
			    error_message, sequence, started_at, finished_at, attempt, deadline_at, source
			)
			SELECT $2, workflow_node_id, node_key, agent_id, run_id, status, input, output,
			       error_message, sequence, started_at, finished_at, 1, deadline_at, 'reused'
			FROM workflow_run_steps
			WHERE workflow_run_id = $1 AND workflow_node_id = $3 AND attempt = $4
		`, sourceRunID, newRunID, node.nodeID, *node.attempt)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 1 {
			one := int32(1)
			effective = &one
		}
	}
	var key string
	if err := tx.QueryRow(ctx, `SELECT node_key FROM workflow_nodes WHERE id = $1`, node.nodeID).Scan(&key); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO workflow_run_nodes (
		    workflow_run_id, workflow_node_id, node_key, state, effective_attempt, skip_reason, terminal_at
		) VALUES ($1, $2, $3, $4, $5, $6, clock_timestamp())
	`, newRunID, node.nodeID, key, node.state, effective, node.skipReason)
	return err
}

func unmarshalWorkflowRunInput(raw []byte) (map[string]interface{}, error) {
	input := map[string]interface{}{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, err
		}
	}
	return input, nil
}
