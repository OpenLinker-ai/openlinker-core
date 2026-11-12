package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const (
	defaultWorkflowParallelism    = 4
	workflowSchedulerPollInterval = time.Second
	workflowSettleRetryInterval   = time.Second
)

// SetWorkflowParallelism caps how many node attempts of one run execute at
// the same time.
func (s *Service) SetWorkflowParallelism(n int) {
	if n > 0 {
		s.parallelism = n
	}
}

func (s *Service) workflowParallelism() int {
	if s.parallelism > 0 {
		return s.parallelism
	}
	return defaultWorkflowParallelism
}

// workflowRunFailedError reports that the run ended failed. Synchronous
// entry points return it as their error, as before; Rerun reports the run.
type workflowRunFailedError struct{ message string }

func (e *workflowRunFailedError) Error() string { return "workflow 执行失败: " + e.message }

// workflowExecutionError maps an execution result for synchronous callers.
func workflowExecutionError(err error) error {
	var failed *workflowRunFailedError
	if errors.As(err, &failed) {
		return httpx.Internal(failed.Error())
	}
	return err
}

// workflowNodeSnapshot is one logical node and its effective attempt.
type workflowNodeSnapshot struct {
	nodeID       uuid.UUID
	nodeKey      string
	state        string
	attempt      int32
	status       string
	stepID       uuid.UUID
	input        []byte
	output       []byte
	errorMessage string
	deadline     *time.Time
	startable    bool // pending and its not_before has passed
}

type workflowRunSnapshot struct {
	runStatus string
	nodes     map[string]workflowNodeSnapshot
}

func (s *Service) loadWorkflowRunSnapshot(ctx context.Context, runID uuid.UUID) (workflowRunSnapshot, error) {
	snapshot := workflowRunSnapshot{nodes: map[string]workflowNodeSnapshot{}}
	if err := s.pool.QueryRow(ctx, `SELECT status FROM workflow_runs WHERE id = $1`, runID).Scan(&snapshot.runStatus); err != nil {
		return snapshot, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT n.workflow_node_id, n.node_key, n.state,
		       s.attempt, s.status, s.id, s.input, s.output, COALESCE(s.error_message, ''), s.deadline_at,
		       s.status = 'pending' AND (s.not_before IS NULL OR s.not_before <= clock_timestamp())
		FROM workflow_run_nodes n
		JOIN LATERAL (
		    SELECT * FROM workflow_run_steps st
		    WHERE st.workflow_run_id = n.workflow_run_id AND st.workflow_node_id = n.workflow_node_id
		    ORDER BY st.attempt DESC
		    LIMIT 1
		) s ON true
		WHERE n.workflow_run_id = $1
	`, runID)
	if err != nil {
		return snapshot, err
	}
	defer rows.Close()
	for rows.Next() {
		var node workflowNodeSnapshot
		if err := rows.Scan(&node.nodeID, &node.nodeKey, &node.state, &node.attempt, &node.status, &node.stepID,
			&node.input, &node.output, &node.errorMessage, &node.deadline, &node.startable); err != nil {
			return snapshot, err
		}
		snapshot.nodes[node.nodeKey] = node
	}
	return snapshot, rows.Err()
}

type workflowSchedulingDecision struct {
	// done: every node is logically terminal and none failed the run.
	done bool
	// failed: a node ended failed/timeout/canceled and its policy fails the
	// workflow. failedKey names the first such node in topological order.
	failed    bool
	failedKey string
	// drive lists nodes whose effective attempt this executor should drive:
	// running attempts it does not drive yet (recovery) and startable
	// pending attempts whose parents all succeeded, up to parallelism.
	drive []string
	// recompute lists nodes waiting on stop evidence (stopping).
	recompute []string
	// skip lists v2 nodes whose incoming edges all resolved without
	// activating them (join all: some inactive; join any: none active).
	skip []workflowSkip
}

type workflowSkip struct {
	key    string
	reason string
}

// workflowRunSemantics is how a run interprets its definition version.
type workflowRunSemantics struct {
	semantics string
	v2        *workflowV2Definition // nil for v1
}

// decideWorkflowScheduling is the pure ready-set decision. Edges only look
// at logical node states: a parent that is retrying or stopping keeps its
// children waiting; there is no level barrier.
func decideWorkflowScheduling(
	graph *workflowGraph,
	order []string,
	snapshot workflowRunSnapshot,
	active map[string]bool,
	parallelism int,
	onFailure func(nodeKey string) string,
	rs workflowRunSemantics,
) workflowSchedulingDecision {
	decision := workflowSchedulingDecision{}
	outputs := workflowSnapshotOutputs(snapshot)
	allTerminal := true
	for _, key := range order {
		node, ok := snapshot.nodes[key]
		if !ok {
			allTerminal = false
			continue
		}
		switch node.state {
		case nodeFailed, nodeTimeout, nodeCanceled:
			if (node.state == nodeCanceled || onFailure(key) == onFailureFailWorkflow) && !decision.failed {
				decision.failed, decision.failedKey = true, key
			}
		case nodeSuccess, nodeSkipped:
		default:
			allTerminal = false
		}
		if node.state == nodeStopping {
			decision.recompute = append(decision.recompute, key)
		}
	}
	if decision.failed {
		return decision
	}
	if allTerminal {
		decision.done = true
		return decision
	}
	inFlight := 0
	for _, key := range order {
		if active[key] {
			inFlight++
			continue
		}
		if node, ok := snapshot.nodes[key]; ok && node.status == attemptRunning {
			// Recovered in-flight attempts are driven regardless of the
			// limit: their child may already be executing.
			decision.drive = append(decision.drive, key)
			inFlight++
		}
	}
	for _, key := range order {
		node, ok := snapshot.nodes[key]
		if !ok || active[key] || !node.startable {
			continue
		}
		if inFlight >= parallelism && (rs.v2 == nil || node.state != nodeWaiting) {
			continue
		}
		if node.state != nodeWaiting && node.state != nodeRetryWait {
			continue
		}
		ready := true
		if rs.v2 != nil && node.state == nodeWaiting {
			activation, reason := rs.v2.activation(key, snapshot, outputs)
			switch activation {
			case "skip":
				decision.skip = append(decision.skip, workflowSkip{key: key, reason: reason})
				ready = false
			case "unresolved":
				ready = false
			}
		} else {
			for _, parent := range graph.Parents[key] {
				if p, ok := snapshot.nodes[parent]; !ok || p.state != nodeSuccess {
					ready = false
					break
				}
			}
		}
		if ready && inFlight < parallelism {
			decision.drive = append(decision.drive, key)
			inFlight++
		}
	}
	return decision
}

// activation decides a waiting v2 node from its incoming edges: "ready",
// "skip" (with the reason) or "unresolved" while any edge is undecided. An
// edge is active when its source succeeded and its condition (if any) holds,
// inactive when the source failed under continue, was skipped, or the
// condition is false.
func (d *workflowV2Definition) activation(key string, snapshot workflowRunSnapshot, outputs map[string]map[string]interface{}) (string, string) {
	incoming := d.incoming[key]
	if len(incoming) == 0 {
		return "ready", ""
	}
	activeCount := 0
	inactive := []string{}
	for _, edge := range incoming {
		source, ok := snapshot.nodes[edge.from]
		if !ok || !workflowNodeStateTerminal(source.state) {
			return "unresolved", ""
		}
		if source.state == nodeSuccess && (edge.when == nil || edge.when.evaluate(outputs[edge.from])) {
			activeCount++
		} else {
			inactive = append(inactive, edge.from)
		}
	}
	switch d.join[key] {
	case joinAny:
		if activeCount > 0 {
			return "ready", ""
		}
		return "skip", "no active incoming edge (join any): " + strings.Join(inactive, ", ")
	default:
		if len(inactive) == 0 {
			return "ready", ""
		}
		return "skip", "inactive incoming edge (join all): " + strings.Join(inactive, ", ")
	}
}

func workflowTopologicalOrder(graph *workflowGraph) []string {
	order := make([]string, 0, len(graph.Sequence))
	for key := range graph.Sequence {
		order = append(order, key)
	}
	sort.Slice(order, func(i, j int) bool {
		if graph.Sequence[order[i]] != graph.Sequence[order[j]] {
			return graph.Sequence[order[i]] < graph.Sequence[order[j]]
		}
		return order[i] < order[j]
	})
	return order
}

// executeWorkflowRun drives a leased run to completion with ready-set
// scheduling. It returns when the run is finished, paused, canceled, or the
// executor lost its lease or was shut down. Shutdown and lease loss leave
// every row as it was: in-flight attempts keep running and the next
// executor resumes them.
func (s *Service) executeWorkflowRun(
	ctx context.Context,
	w db.Workflow,
	nodes []db.WorkflowNode,
	graph *workflowGraph,
	run db.WorkflowRun,
	input map[string]interface{},
) (*WorkflowRunResponse, error) {
	if err := s.initWorkflowRunNodes(ctx, run.ID, graph, nodes); err != nil {
		return nil, s.executorStopped(ctx, err)
	}
	byKey := make(map[string]db.WorkflowNode, len(nodes))
	for _, node := range nodes {
		byKey[node.NodeKey] = node
	}
	order := workflowTopologicalOrder(graph)
	rs, err := s.loadWorkflowRunSemantics(ctx, run, w, nodes)
	if err != nil {
		return nil, s.executorStopped(ctx, err)
	}
	onFailure := func(nodeKey string) string {
		return s.workflowNodePolicy(rs.semantics, byKey[nodeKey].Config).onFailure
	}

	// Core executes direct HTTP/MCP children inline in the calling
	// goroutine, so a child call runs under callCtx: pausing leaves it
	// running (in-flight children are not stopped by a pause), while the
	// attempt deadline bounds it and finishing, failing or canceling the run
	// aborts it. driveCtx (waits, settlement, starting new work) also ends on
	// pause.
	callCtx, abortCalls := context.WithCancel(ctx)
	driveCtx, cancelDrives := context.WithCancel(callCtx)
	var wg sync.WaitGroup
	defer func() {
		abortCalls()
		cancelDrives()
		wg.Wait()
	}()
	finished := make(chan string, len(nodes))
	active := map[string]bool{}
	// A drive that ended without moving its attempt forward is not
	// restarted before the next poll.
	cooling := map[string]time.Time{}
	ticker := time.NewTicker(workflowSchedulerPollInterval)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		snapshot, err := s.loadWorkflowRunSnapshot(ctx, run.ID)
		if err != nil {
			return nil, s.executorStopped(ctx, err)
		}
		switch snapshot.runStatus {
		case workflowRunStatusRunning:
		case workflowRunStatusPending:
			// Recovery took the run back: this executor lost it.
			return nil, errWorkflowRunLeaseLost
		case workflowRunStatusPaused:
			// In-flight children keep running; their attempts are settled by
			// the next executor or, at their deadline, by the reconciler.
			cancelDrives()
			wg.Wait()
			return s.GetWorkflowRun(context.WithoutCancel(ctx), run.UserID, run.ID)
		default:
			// Canceled or finished elsewhere.
			abortCalls()
			wg.Wait()
			return s.GetWorkflowRun(context.WithoutCancel(ctx), run.UserID, run.ID)
		}
		busy := make(map[string]bool, len(active)+len(cooling))
		for key := range active {
			busy[key] = true
		}
		for key, until := range cooling {
			if time.Now().Before(until) {
				busy[key] = true
			} else {
				delete(cooling, key)
			}
		}
		decision := decideWorkflowScheduling(graph, order, snapshot, busy, s.workflowParallelism(), onFailure, rs)
		for _, skip := range decision.skip {
			if err := s.skipWorkflowNode(ctx, run.ID, byKey[skip.key].ID, skip.reason); err != nil {
				return nil, s.executorStopped(ctx, err)
			}
		}
		if len(decision.skip) > 0 {
			continue
		}
		for _, key := range decision.recompute {
			nodeID := byKey[key].ID
			if err := s.leasedTransition(ctx, run.ID, func(tx pgx.Tx, _ string) error {
				return s.recomputeNode(ctx, tx, run.ID, nodeID)
			}); err != nil {
				return nil, s.executorStopped(ctx, err)
			}
		}
		if decision.failed {
			abortCalls()
			wg.Wait()
			node := snapshot.nodes[decision.failedKey]
			msg := node.errorMessage
			if msg == "" {
				msg = "workflow step " + decision.failedKey + " " + node.state
			}
			if err := s.failWorkflowRunAtomic(context.WithoutCancel(ctx), run.ID, msg); err != nil {
				return nil, s.executorStopped(ctx, err)
			}
			// Hand the new stop requests to the stop reconciler right away
			// rather than waiting for the next worker tick.
			s.reconcileWorkflowRunStops(context.WithoutCancel(ctx), run.ID)
			resp, err := s.GetWorkflowRun(context.WithoutCancel(ctx), run.UserID, run.ID)
			if err != nil {
				return nil, err
			}
			return resp, &workflowRunFailedError{message: msg}
		}
		if decision.done {
			return s.completeWorkflowRun(ctx, run, graph, snapshot, rs, input)
		}
		outputs := workflowSnapshotOutputs(snapshot)
		for _, key := range decision.drive {
			node := byKey[key]
			attempt := snapshot.nodes[key]
			stepInput := workflowStepInput(input, outputs, graph.Parents[key], node)
			active[key] = true
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.driveWorkflowAttempt(driveCtx, callCtx, w, run, node, attempt, stepInput, rs.semantics)
				finished <- node.NodeKey
			}()
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case key := <-finished:
			delete(active, key)
			cooling[key] = time.Now().Add(workflowSchedulerPollInterval)
		case <-ticker.C:
		}
	}
}

// executorStopped classifies an executor error: shutdown and lease loss are
// returned as-is (no evidence is written); anything else is an infrastructure
// error.
func (s *Service) executorStopped(ctx context.Context, err error) error {
	if ctx.Err() != nil || errors.Is(err, errWorkflowRunLeaseLost) || isWorkflowContextError(err) {
		if errors.Is(err, errWorkflowRunLeaseLost) {
			return err
		}
		return ctx.Err()
	}
	log.Error().Err(err).Msg("workflow: executor")
	return httpx.Internal("workflow 执行失败")
}

func workflowSnapshotOutputs(snapshot workflowRunSnapshot) map[string]map[string]interface{} {
	outputs := map[string]map[string]interface{}{}
	for key, node := range snapshot.nodes {
		if node.state != nodeSuccess {
			continue
		}
		output := map[string]interface{}{}
		if len(node.output) > 0 {
			_ = json.Unmarshal(node.output, &output)
		}
		outputs[key] = output
	}
	return outputs
}

// completeWorkflowRun is the success-side finalizer once every node is
// logically terminal without failing the workflow. v1: the output of the
// sinks (all succeeded). v2 with output_mapping: success only when every
// field evaluates (or defaults), else failed with output_mapping_failed.
// v2 without it: success with the successful sinks' outputs when at least
// one sink succeeded, else failed (no usable terminal output).
func (s *Service) completeWorkflowRun(
	ctx context.Context,
	run db.WorkflowRun,
	graph *workflowGraph,
	snapshot workflowRunSnapshot,
	rs workflowRunSemantics,
	input map[string]interface{},
) (*WorkflowRunResponse, error) {
	outputs := workflowSnapshotOutputs(snapshot)
	var finalOutput map[string]interface{}
	failure := ""
	switch {
	case rs.v2 != nil && rs.v2.output != nil:
		mapped, err := rs.v2.output.evaluate(workflowMappingContext{input: input, outputs: outputs})
		if err != nil {
			failure = "output_mapping_failed: " + err.Error()
		}
		finalOutput = mapped
	case rs.v2 != nil:
		successfulSinks := []string{}
		for _, sink := range graph.Sinks {
			if _, ok := outputs[sink]; ok {
				successfulSinks = append(successfulSinks, sink)
			}
		}
		if len(successfulSinks) == 0 {
			failure = "workflow 没有可用的终端输出：所有汇点都被跳过或失败"
		}
		finalOutput = workflowFinalOutput(outputs, successfulSinks)
	default:
		finalOutput = workflowFinalOutput(outputs, graph.Sinks)
	}
	if failure != "" {
		if err := s.failWorkflowRunAtomic(context.WithoutCancel(ctx), run.ID, failure); err != nil {
			return nil, s.executorStopped(ctx, err)
		}
		resp, err := s.GetWorkflowRun(context.WithoutCancel(ctx), run.UserID, run.ID)
		if err != nil {
			return nil, err
		}
		return resp, &workflowRunFailedError{message: failure}
	}
	finalJSON, _ := json.Marshal(finalOutput)
	if err := s.fencedWrite(ctx, run.ID, func(q *db.Queries) error {
		_, err := q.MarkWorkflowRunSuccess(ctx, db.MarkWorkflowRunSuccessParams{ID: run.ID, Output: finalJSON})
		return err
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return s.GetWorkflowRun(ctx, run.UserID, run.ID)
		}
		if errors.Is(err, errWorkflowRunLeaseLost) {
			return nil, err
		}
		return nil, httpx.Internal("更新 workflow_run 失败")
	}
	return s.GetWorkflowRun(ctx, run.UserID, run.ID)
}

// loadWorkflowRunSemantics reads the run's version semantics and, for v2,
// parses the definition (already validated when the version was written).
func (s *Service) loadWorkflowRunSemantics(ctx context.Context, run db.WorkflowRun, w db.Workflow, nodes []db.WorkflowNode) (workflowRunSemantics, error) {
	rs := workflowRunSemantics{semantics: workflowSemanticsV1}
	if s.pool == nil {
		return rs, nil
	}
	var outputJSON []byte
	if err := s.pool.QueryRow(ctx, `
		SELECT v.semantics, v.output_mapping
		FROM workflow_runs r JOIN workflow_versions v ON v.id = r.workflow_version_id
		WHERE r.id = $1
	`, run.ID).Scan(&rs.semantics, &outputJSON); err != nil {
		return rs, err
	}
	if rs.semantics != workflowSemanticsV2 {
		return rs, nil
	}
	edges := []map[string]interface{}{}
	if len(w.Edges) > 0 {
		if err := json.Unmarshal(w.Edges, &edges); err != nil {
			return rs, err
		}
	}
	v2Nodes := make([]workflowV2NodeInput, 0, len(nodes))
	for _, node := range nodes {
		v2Nodes = append(v2Nodes, workflowV2NodeInput{key: node.NodeKey, config: node.Config})
	}
	var output interface{}
	if len(outputJSON) > 0 {
		if err := json.Unmarshal(outputJSON, &output); err != nil {
			return rs, err
		}
	}
	def, err := parseWorkflowV2Definition(v2Nodes, edges, output)
	if err != nil {
		return rs, err
	}
	rs.v2 = def
	return rs, nil
}

// skipWorkflowNode marks a waiting v2 node skipped (logically terminal with
// its reason) and voids its never-started attempt. Requires the lease.
func (s *Service) skipWorkflowNode(ctx context.Context, runID, nodeID uuid.UUID, reason string) error {
	return s.leasedTransition(ctx, runID, func(tx pgx.Tx, _ string) error {
		tag, err := tx.Exec(ctx, `
			UPDATE workflow_run_nodes
			SET state = 'skipped', skip_reason = $3, terminal_at = clock_timestamp(), updated_at = clock_timestamp()
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND state = 'waiting' AND terminal_at IS NULL
		`, runID, nodeID, truncate(reason, 500))
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		_, err = tx.Exec(ctx, `
			UPDATE workflow_run_steps
			SET status = 'canceled', finished_at = clock_timestamp(), updated_at = clock_timestamp(),
			    error_message = 'workflow node skipped'
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND status = 'pending'
		`, runID, nodeID)
		return err
	})
}

// failWorkflowRunAtomic is the fail_workflow finalization: in one parent
// transaction under the execution lease the run becomes failed, every
// pending and running attempt becomes canceled, in-flight children get stop
// requests (reason workflow_failure) and claimed launches are invalidated.
// There is never a failed run without stop requests for its children.
func (s *Service) failWorkflowRunAtomic(ctx context.Context, runID uuid.UUID, message string) error {
	msg := truncate(message, 1000)
	return s.leasedTransition(ctx, runID, func(tx pgx.Tx, status string) error {
		if status != workflowRunStatusRunning {
			return nil
		}
		if _, err := s.queries.WithTx(tx).MarkWorkflowRunFailed(ctx, db.MarkWorkflowRunFailedParams{
			ID: runID, ErrorMessage: &msg,
		}); err != nil {
			return err
		}
		return s.cancelAttempts(ctx, tx, runID, stopReasonWorkflowFailure, true)
	})
}

// driveWorkflowAttempt executes one node attempt: start it (pending), claim
// the launch, create or recover the child, attach it, then wait until the
// child ends or the deadline passes and settle from Runtime. A canceled
// context (shutdown, pause, lease loss) returns without writing anything.
func (s *Service) driveWorkflowAttempt(
	ctx context.Context,
	callCtx context.Context,
	w db.Workflow,
	run db.WorkflowRun,
	node db.WorkflowNode,
	attempt workflowNodeSnapshot,
	stepInput workflowStepInputResult,
	semantics string,
) {
	policy := s.workflowNodePolicy(semantics, node.Config)
	stepID := attempt.stepID
	inputJSON := attempt.input
	var deadline time.Time
	if attempt.status == attemptPending {
		if stepInput.Value == nil {
			stepInput.Value = map[string]interface{}{}
		}
		inputJSON, _ = json.Marshal(stepInput.Value)
		started, ok, err := s.startAttempt(ctx, run.ID, node.ID, attempt.attempt, inputJSON, policy.timeout)
		if err != nil || !ok || started.status != attemptRunning {
			return
		}
		stepID, deadline = started.stepID, started.deadline
		if stepInput.Err != nil {
			s.failAttemptLocalLogged(ctx, run.ID, node, attempt.attempt, stepInput.Err.Error())
			return
		}
		if stepInput.Mapped {
			if err := s.validateMappedWorkflowNodeInput(ctx, node, stepInput.Value); err != nil {
				s.failAttemptLocalLogged(ctx, run.ID, node, attempt.attempt, err.Error())
				return
			}
		}
	} else if attempt.deadline != nil {
		deadline = *attempt.deadline
	} else {
		// A running attempt without a deadline never started under this
		// engine; it goes back to pending and is started normally.
		if err := s.requeueAttempt(ctx, run.ID, node.ID, attempt.attempt); err != nil &&
			ctx.Err() == nil && !errors.Is(err, errWorkflowRunLeaseLost) {
			log.Warn().Err(err).Str("workflow_run_id", run.ID.String()).Str("node_key", node.NodeKey).Msg("workflow: requeue attempt")
		}
		return
	}
	input := map[string]interface{}{}
	if len(inputJSON) > 0 {
		_ = json.Unmarshal(inputJSON, &input)
	}
	runRequest := &runtime.RunRequest{
		AgentID:          node.AgentID.String(),
		Input:            input,
		IdempotencyKey:   workflowNodeAttemptIdempotencyKey(run.ID, node.NodeKey, attempt.attempt),
		CreationProtocol: "workflow",
		CreationMethod:   "node.run",
		Metadata:         workflowNodeRunMetadata(w.ID, run.ID, node, attempt.attempt),
	}
	launcher, ok := s.runtime.(workflowLaunchRuntime)
	if !ok {
		s.failAttemptLocalLogged(ctx, run.ID, node, attempt.attempt, "workflow runtime 不支持受围栏的子 run 创建")
		return
	}
	identity, err := launcher.PrepareRunCreationIdentity(runRequest, "api")
	if err != nil {
		s.failAttemptLocalLogged(ctx, run.ID, node, attempt.attempt, err.Error())
		return
	}
	runIdentity := dbWorkflowRunIdentity{ID: run.ID, UserID: run.UserID}
	fence, childID, err := s.claimWorkflowChildLaunch(ctx, runIdentity, node.ID, stepID, node.NodeKey, attempt.attempt, identity)
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, errWorkflowRunLeaseLost) {
			return
		}
		if errors.Is(err, runtime.ErrWorkflowChildLaunchFenceRejected) {
			s.failAttemptLocalLogged(ctx, run.ID, node, attempt.attempt, "workflow child launch 校验失败: "+err.Error())
		}
		return
	}
	if childID == uuid.Nil {
		childCallCtx, cancelChildCall := context.WithDeadline(callCtx, deadline)
		resp, runErr := launcher.RunWorkflowChild(childCallCtx, run.UserID, runRequest, "api", fence)
		cancelChildCall()
		if runErr != nil {
			if ctx.Err() != nil {
				return
			}
			failErr := s.failAttemptLocal(ctx, run.ID, node.ID, attempt.attempt, runErr.Error())
			if !errors.Is(failErr, errWorkflowAttemptHasChild) {
				return
			}
			// The creation committed although the call failed: recover the
			// child from the launch record.
			fence, childID, err = s.claimWorkflowChildLaunch(ctx, runIdentity, node.ID, stepID, node.NodeKey, attempt.attempt, identity)
			if err != nil || childID == uuid.Nil {
				return
			}
		} else if childID, err = workflowChildRunIDFromResponse(resp); err != nil {
			return
		}
	}
	attachCtx, cancelAttach := context.WithTimeout(context.WithoutCancel(callCtx), workflowStepEvidenceWriteTimeout)
	attachErr := s.attachWorkflowChildLaunch(attachCtx, fence, stepID, childID)
	cancelAttach()
	if ctx.Err() != nil || errors.Is(attachErr, errWorkflowRunLeaseLost) {
		return
	}
	waitCtx, cancelWait := context.WithDeadline(ctx, deadline)
	_, _ = s.waitForRuntimeRunCompletion(waitCtx, run.UserID, childID)
	cancelWait()
	for ctx.Err() == nil {
		status, err := s.settleFromRuntime(ctx, run.ID, node.ID, attempt.attempt)
		if err == nil && status != attemptRunning {
			return
		}
		timer := time.NewTimer(workflowSettleRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *Service) failAttemptLocalLogged(ctx context.Context, runID uuid.UUID, node db.WorkflowNode, attempt int32, message string) {
	if ctx.Err() != nil {
		return
	}
	if err := s.failAttemptLocal(ctx, runID, node.ID, attempt, message); err != nil &&
		!errors.Is(err, errWorkflowRunLeaseLost) && !errors.Is(err, errWorkflowAttemptHasChild) {
		log.Warn().Err(err).Str("workflow_run_id", runID.String()).Str("node_key", node.NodeKey).Msg("workflow: fail attempt")
	}
}

// workflowNodeAttemptIdempotencyKey keeps attempt 1 on the original key (its
// creation envelope is frozen); later attempts get their own key.
func workflowNodeAttemptIdempotencyKey(workflowRunID uuid.UUID, nodeKey string, attempt int32) string {
	key := workflowNodeRunIdempotencyKey(workflowRunID, nodeKey)
	if attempt <= 1 {
		return key
	}
	return fmt.Sprintf("%s/attempt/%d", key, attempt)
}

// workflowNodeRunMetadata freezes attempt 1's metadata field set; only later
// attempts carry the attempt number.
func workflowNodeRunMetadata(workflowID, runID uuid.UUID, node db.WorkflowNode, attempt int32) map[string]interface{} {
	metadata := map[string]interface{}{
		"workflow_id":       workflowID.String(),
		"workflow_run_id":   runID.String(),
		"workflow_node_id":  node.ID.String(),
		"workflow_node_key": node.NodeKey,
	}
	if attempt > 1 {
		metadata["workflow_node_attempt"] = attempt
	}
	return metadata
}
