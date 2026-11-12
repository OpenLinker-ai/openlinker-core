package workflow

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// Attempt states (workflow_run_steps.status) and logical node states
// (workflow_run_nodes.state). Edges and the scheduler read only logical node
// states; a logical terminal state never changes again.
const (
	attemptPending  = "pending"
	attemptRunning  = "running"
	attemptSuccess  = "success"
	attemptFailed   = "failed"
	attemptTimeout  = "timeout"
	attemptCanceled = "canceled"

	nodeWaiting   = "waiting"
	nodeRunning   = "running"
	nodeRetryWait = "retry_wait"
	nodeStopping  = "stopping"
	nodeSuccess   = "success"
	nodeFailed    = "failed"
	nodeTimeout   = "timeout"
	nodeSkipped   = "skipped"
	nodeCanceled  = "canceled"

	stopReasonNodeTimeout     = "node_timeout"
	stopReasonWorkflowFailure = "workflow_failure"
	stopReasonWorkflowCancel  = "workflow_cancel"

	onFailureFailWorkflow = "fail_workflow"

	// workflowChildCanceledPrefix marks an attempt whose child Run was
	// canceled by its owner (not by the workflow). It is never retried.
	workflowChildCanceledPrefix = "child_canceled: "
)

// Q1: a node attempt times out after 30 minutes unless configured otherwise.
const defaultWorkflowNodeTimeout = 30 * time.Minute

var errWorkflowAttemptHasChild = errors.New("workflow attempt already created its child run")

func workflowNodeStateTerminal(state string) bool {
	switch state {
	case nodeSuccess, nodeFailed, nodeTimeout, nodeSkipped, nodeCanceled:
		return true
	}
	return false
}

// workflowNodePolicy is a node's execution policy. Until node policies are
// parsed from definitions (phase C) every node uses the service defaults:
// one attempt, fail the workflow on failure.
type workflowNodePolicy struct {
	timeout     time.Duration
	maxAttempts int32
	backoff     time.Duration
	onFailure   string
}

// SetWorkflowNodeTimeout sets the default node attempt timeout.
func (s *Service) SetWorkflowNodeTimeout(timeout time.Duration) {
	if timeout > 0 {
		s.nodeTimeout = timeout
	}
}

// SetWorkflowNodeRetryDefaults sets the default node retry budget. Node
// definitions do not carry retry policies yet; the default is one attempt.
func (s *Service) SetWorkflowNodeRetryDefaults(maxAttempts int32, backoff time.Duration) {
	if maxAttempts >= 1 {
		s.nodeRetryAttempts = maxAttempts
	}
	if backoff >= 0 {
		s.nodeRetryBackoff = backoff
	}
}

// workflowNodePolicy is a node's execution policy. v1 definitions ignore
// config.policy entirely (the service defaults apply, as before); v2
// definitions use their validated policy, falling back to the defaults for
// fields they leave out.
func (s *Service) workflowNodePolicy(semantics string, config []byte) workflowNodePolicy {
	policy := workflowNodePolicy{
		timeout:     defaultWorkflowNodeTimeout,
		maxAttempts: 1,
		onFailure:   onFailureFailWorkflow,
	}
	if s.nodeTimeout > 0 {
		policy.timeout = s.nodeTimeout
	}
	if s.nodeRetryAttempts > 1 {
		policy.maxAttempts = s.nodeRetryAttempts
		policy.backoff = s.nodeRetryBackoff
	}
	if semantics != workflowSemanticsV2 {
		return policy
	}
	spec, err := parseWorkflowNodePolicy(config)
	if err != nil || spec == nil {
		return policy
	}
	if spec.TimeoutSeconds > 0 {
		policy.timeout = time.Duration(spec.TimeoutSeconds) * time.Second
	}
	if spec.Retry != nil {
		policy.maxAttempts = spec.Retry.MaxAttempts
		policy.backoff = time.Duration(spec.Retry.BackoffSeconds) * time.Second
	}
	if spec.OnFailure != "" {
		policy.onFailure = spec.OnFailure
	}
	return policy
}

// lockWorkflowRunForTransition locks the parent run, then reads its status
// with a fresh statement. Every attempt transition holds this lock.
func lockWorkflowRunForTransition(ctx context.Context, tx pgx.Tx, runID uuid.UUID) (string, error) {
	var locked int
	if err := tx.QueryRow(ctx, `SELECT 1 FROM workflow_runs WHERE id = $1 FOR UPDATE`, runID).Scan(&locked); err != nil {
		return "", err
	}
	var status string
	err := tx.QueryRow(ctx, `SELECT status FROM workflow_runs WHERE id = $1`, runID).Scan(&status)
	return status, err
}

// initWorkflowRunNodes creates the logical node rows and attempt 1 of every
// node, idempotently. A node that already has a success attempt (copied by a
// step rerun) starts as logically successful. Runs under the execution lease.
func (s *Service) initWorkflowRunNodes(ctx context.Context, runID uuid.UUID, graph *workflowGraph, nodes []db.WorkflowNode) error {
	return s.leasedTransition(ctx, runID, func(tx pgx.Tx, _ string) error {
		for _, node := range nodes {
			if _, err := tx.Exec(ctx, `
				INSERT INTO workflow_run_steps (
				    workflow_run_id, workflow_node_id, node_key, agent_id, status, input, sequence, attempt
				)
				SELECT $1, $2, $3, $4, 'pending', '{}'::jsonb, $5, 1
				WHERE NOT EXISTS (
				    SELECT 1 FROM workflow_run_steps
				    WHERE workflow_run_id = $1 AND workflow_node_id = $2
				)
			`, runID, node.ID, node.NodeKey, node.AgentID, graph.Sequence[node.NodeKey]); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO workflow_run_nodes (workflow_run_id, workflow_node_id, node_key, state)
				VALUES ($1, $2, $3, 'waiting')
				ON CONFLICT (workflow_run_id, workflow_node_id) DO NOTHING
			`, runID, node.ID, node.NodeKey); err != nil {
				return err
			}
			if err := s.recomputeNode(ctx, tx, runID, node.ID); err != nil {
				return err
			}
		}
		return nil
	})
}

// leasedTransition runs fn in one transaction after checking the caller's
// execution lease under the parent row lock.
func (s *Service) leasedTransition(ctx context.Context, runID uuid.UUID, fn func(pgx.Tx, string) error) error {
	if s.pool == nil {
		return errors.New("workflow transactional store is not configured")
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.lockWorkflowRunLease(ctx, tx, runID); err != nil {
			return err
		}
		var status string
		if err := tx.QueryRow(ctx, `SELECT status FROM workflow_runs WHERE id = $1`, runID).Scan(&status); err != nil {
			return err
		}
		return fn(tx, status)
	})
}

type startedAttempt struct {
	stepID   uuid.UUID
	status   string
	deadline time.Time
}

// startAttempt moves a pending attempt whose backoff has passed to running.
// The first start fixes deadline_at; a later start (after recovery) keeps it,
// and an attempt whose kept deadline already passed becomes timeout. Requires
// the execution lease and a running (not paused) run.
func (s *Service) startAttempt(
	ctx context.Context,
	runID, nodeID uuid.UUID,
	attempt int32,
	input []byte,
	timeout time.Duration,
) (startedAttempt, bool, error) {
	var started startedAttempt
	ok := false
	err := s.leasedTransition(ctx, runID, func(tx pgx.Tx, status string) error {
		if status != workflowRunStatusRunning {
			return nil
		}
		var expired bool
		err := tx.QueryRow(ctx, `
			UPDATE workflow_run_steps
			SET status = 'running',
			    input = $4,
			    started_at = clock_timestamp(),
			    deadline_at = COALESCE(deadline_at, clock_timestamp() + make_interval(secs => $5)),
			    updated_at = clock_timestamp()
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
			  AND status = 'pending'
			  AND (not_before IS NULL OR not_before <= clock_timestamp())
			RETURNING id, deadline_at, deadline_at <= clock_timestamp()
		`, runID, nodeID, attempt, input, timeout.Seconds()).Scan(&started.stepID, &started.deadline, &expired)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		ok = true
		started.status = attemptRunning
		if expired {
			// A recovered attempt whose deadline passed has no child: it
			// times out without needing a stop.
			if _, err := tx.Exec(ctx, `
				UPDATE workflow_run_steps
				SET status = 'timeout', finished_at = clock_timestamp(),
				    error_message = 'workflow node attempt deadline exceeded', updated_at = clock_timestamp()
				WHERE id = $1
			`, started.stepID); err != nil {
				return err
			}
			started.status = attemptTimeout
		}
		return s.recomputeNode(ctx, tx, runID, nodeID)
	})
	return started, ok, err
}

// requeueAttempt returns a running attempt that has not created a child
// (no launch, or a launch still only claimed) to pending, keeping its
// deadline. Requires the execution lease.
func (s *Service) requeueAttempt(ctx context.Context, runID, nodeID uuid.UUID, attempt int32) error {
	return s.leasedTransition(ctx, runID, func(tx pgx.Tx, _ string) error {
		var launchRunID *uuid.UUID
		err := tx.QueryRow(ctx, `
			SELECT run_id FROM workflow_step_launches
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
			FOR UPDATE
		`, runID, nodeID, attempt).Scan(&launchRunID)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if launchRunID != nil {
			return errWorkflowAttemptHasChild
		}
		tag, err := tx.Exec(ctx, `
			UPDATE workflow_run_steps
			SET status = 'pending', updated_at = clock_timestamp()
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
			  AND status = 'running' AND run_id IS NULL AND NOT legacy_unfinished
		`, runID, nodeID, attempt)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		return s.recomputeNode(ctx, tx, runID, nodeID)
	})
}

// failAttemptLocal records a failure that happened before any child Run was
// created (input mapping, schema mismatch, a definite creation rejection).
// Under the parent lock, an attempt past its deadline is recorded as timeout
// instead. A claimed launch is invalidated in the same transaction. If the
// launch already created a child, the attempt must be settled from Runtime
// instead and errWorkflowAttemptHasChild is returned.
func (s *Service) failAttemptLocal(ctx context.Context, runID, nodeID uuid.UUID, attempt int32, message string) error {
	return s.leasedTransition(ctx, runID, func(tx pgx.Tx, _ string) error {
		var launchState string
		var launchRunID *uuid.UUID
		err := tx.QueryRow(ctx, `
			SELECT state, run_id FROM workflow_step_launches
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
			FOR UPDATE
		`, runID, nodeID, attempt).Scan(&launchState, &launchRunID)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if launchRunID != nil {
			return errWorkflowAttemptHasChild
		}
		tag, err := tx.Exec(ctx, `
			UPDATE workflow_run_steps
			SET status = CASE WHEN deadline_at IS NOT NULL AND clock_timestamp() >= deadline_at
			                  THEN 'timeout' ELSE 'failed' END,
			    error_message = CASE WHEN deadline_at IS NOT NULL AND clock_timestamp() >= deadline_at
			                  THEN 'workflow node attempt deadline exceeded' ELSE $4 END,
			    finished_at = clock_timestamp(),
			    updated_at = clock_timestamp()
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
			  AND status = 'running'
		`, runID, nodeID, attempt, truncate(message, 1000))
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
		if _, err := tx.Exec(ctx, `
			UPDATE workflow_step_launches
			SET state = 'invalidated', updated_at = clock_timestamp()
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
			  AND state = 'claimed' AND run_id IS NULL
		`, runID, nodeID, attempt); err != nil {
			return err
		}
		return s.recomputeNode(ctx, tx, runID, nodeID)
	})
}

// settleFromRuntime settles a running attempt from Runtime facts and the
// database clock. It needs no lease: the outcome is fully determined.
//
//  1. child terminal and finished at or before deadline_at: success/failed;
//  2. otherwise, at or past deadline_at: timeout, plus a stop request for a
//     created child (or invalidation of a claimed launch);
//  3. otherwise: unchanged.
//
// It returns the attempt status after the transaction.
func (s *Service) settleFromRuntime(ctx context.Context, runID, nodeID uuid.UUID, attempt int32) (string, error) {
	var result string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := lockWorkflowRunForTransition(ctx, tx, runID); err != nil {
			return err
		}
		var launchState string
		var launchRunID *uuid.UUID
		err := tx.QueryRow(ctx, `
			SELECT state, run_id FROM workflow_step_launches
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
			FOR UPDATE
		`, runID, nodeID, attempt).Scan(&launchState, &launchRunID)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var stepID uuid.UUID
		var status, nodeKey string
		var stepRunID *uuid.UUID
		var deadline *time.Time
		var legacy bool
		if err := tx.QueryRow(ctx, `
			SELECT id, status, node_key, run_id, deadline_at, legacy_unfinished
			FROM workflow_run_steps
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
			FOR UPDATE
		`, runID, nodeID, attempt).Scan(&stepID, &status, &nodeKey, &stepRunID, &deadline, &legacy); err != nil {
			return err
		}
		result = status
		if legacy || (status != attemptRunning && status != attemptPending) || deadline == nil {
			return nil
		}
		childID := stepRunID
		if childID == nil {
			childID = launchRunID
		}
		if status == attemptRunning && childID != nil {
			outcome, err := runtime.LockWorkflowChildOutcome(ctx, tx, *childID)
			if err != nil {
				return err
			}
			if outcome.Terminal && outcome.FinishedAt != nil && !outcome.FinishedAt.After(*deadline) {
				if normalizeRunStatus(outcome.Status) == runtimeRunStatusSuccess {
					output := outcome.Output
					if len(output) == 0 || string(output) == "null" {
						output = []byte(`{}`)
					}
					if _, err := tx.Exec(ctx, `
						UPDATE workflow_run_steps
						SET status = 'success', run_id = $2, output = $3, error_message = NULL,
						    finished_at = clock_timestamp(), updated_at = clock_timestamp()
						WHERE id = $1
					`, stepID, *childID, output); err != nil {
						return err
					}
					result = attemptSuccess
				} else {
					msg := outcome.ErrorMessage
					if msg == "" {
						msg = "workflow step " + nodeKey + " returned status " + normalizeRunStatus(outcome.Status)
					}
					if normalizeRunStatus(outcome.Status) == runtimeRunStatusCanceled {
						msg = workflowChildCanceledPrefix + msg
					}
					if _, err := tx.Exec(ctx, `
						UPDATE workflow_run_steps
						SET status = 'failed', run_id = $2, error_message = $3,
						    finished_at = clock_timestamp(), updated_at = clock_timestamp()
						WHERE id = $1
					`, stepID, *childID, truncate(msg, 1000)); err != nil {
						return err
					}
					result = attemptFailed
				}
				return s.recomputeNode(ctx, tx, runID, nodeID)
			}
		}
		// Re-read the clock now that every lock is held.
		var expired bool
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp() >= $1::timestamptz`, *deadline).Scan(&expired); err != nil {
			return err
		}
		if !expired {
			return nil
		}
		if _, err := tx.Exec(ctx, `
			UPDATE workflow_run_steps
			SET status = 'timeout', run_id = COALESCE(run_id, $2),
			    error_message = 'workflow node attempt deadline exceeded',
			    finished_at = clock_timestamp(), updated_at = clock_timestamp()
			WHERE id = $1
		`, stepID, childID); err != nil {
			return err
		}
		result = attemptTimeout
		if childID != nil {
			if err := insertWorkflowStepStop(ctx, tx, runID, nodeID, attempt, *childID, stopReasonNodeTimeout); err != nil {
				return err
			}
		} else if _, err := tx.Exec(ctx, `
			UPDATE workflow_step_launches
			SET state = 'invalidated', updated_at = clock_timestamp()
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
			  AND state = 'claimed' AND run_id IS NULL
		`, runID, nodeID, attempt); err != nil {
			return err
		}
		return s.recomputeNode(ctx, tx, runID, nodeID)
	})
	return result, err
}

func insertWorkflowStepStop(ctx context.Context, tx pgx.Tx, runID, nodeID uuid.UUID, attempt int32, childID uuid.UUID, reason string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO workflow_step_stops (workflow_run_id, workflow_node_id, attempt, child_run_id, reason, state)
		VALUES ($1, $2, $3, $4, $5, 'requested')
		ON CONFLICT (workflow_run_id, workflow_node_id, attempt) DO NOTHING
	`, runID, nodeID, attempt, childID, reason)
	return err
}

// cancelAttempts turns every pending and running attempt of the run into
// canceled, in the caller's transaction (which holds the parent lock). A
// running attempt that created a child gets a stop request when writeStops
// is set; claimed launches that created nothing are invalidated. Logical
// nodes are recomputed. Public cancellation passes writeStops=false: its own
// reconciliation stops the children.
func (s *Service) cancelAttempts(ctx context.Context, tx pgx.Tx, runID uuid.UUID, reason string, writeStops bool) error {
	if writeStops {
		if _, err := tx.Exec(ctx, `
			INSERT INTO workflow_step_stops (workflow_run_id, workflow_node_id, attempt, child_run_id, reason, state)
			SELECT s.workflow_run_id, s.workflow_node_id, s.attempt, COALESCE(s.run_id, l.run_id), $2, 'requested'
			FROM workflow_run_steps s
			LEFT JOIN workflow_step_launches l
			       ON l.workflow_run_id = s.workflow_run_id
			      AND l.workflow_node_id = s.workflow_node_id
			      AND l.attempt = s.attempt
			WHERE s.workflow_run_id = $1
			  AND s.status = 'running'
			  AND NOT s.legacy_unfinished
			  AND COALESCE(s.run_id, l.run_id) IS NOT NULL
			ON CONFLICT (workflow_run_id, workflow_node_id, attempt) DO NOTHING
		`, runID, reason); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE workflow_step_launches
		SET state = 'invalidated', updated_at = clock_timestamp()
		WHERE workflow_run_id = $1 AND state = 'claimed' AND run_id IS NULL
	`, runID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE workflow_run_steps
		SET status = 'canceled', finished_at = clock_timestamp(), updated_at = clock_timestamp(),
		    error_message = COALESCE(error_message, $2)
		WHERE workflow_run_id = $1 AND status IN ('pending', 'running') AND NOT legacy_unfinished
	`, runID, "workflow attempt canceled ("+reason+")"); err != nil {
		return err
	}
	return s.recomputeOpenNodes(ctx, tx, runID)
}

func (s *Service) recomputeOpenNodes(ctx context.Context, tx pgx.Tx, runID uuid.UUID) error {
	rows, err := tx.Query(ctx, `
		SELECT workflow_node_id FROM workflow_run_nodes
		WHERE workflow_run_id = $1 AND terminal_at IS NULL
		ORDER BY workflow_node_id
	`, runID)
	if err != nil {
		return err
	}
	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		if err := s.recomputeNode(ctx, tx, runID, id); err != nil {
			return err
		}
	}
	return nil
}

// recomputeNode re-derives a logical node's state from its attempts, stop
// records and Runtime stop evidence. It needs no lease (pure derivation, no
// external effect) and must run under the parent lock. It may insert the
// next attempt as pending data; executing it still needs a lease. A logical
// terminal state is never changed. When the run is finished or being
// canceled, or the attempt was stopped by a workflow failure, it only
// materializes state and never inserts a new attempt.
func (s *Service) recomputeNode(ctx context.Context, tx pgx.Tx, runID, nodeID uuid.UUID) error {
	var state string
	var terminal bool
	err := tx.QueryRow(ctx, `
		SELECT state, terminal_at IS NOT NULL FROM workflow_run_nodes
		WHERE workflow_run_id = $1 AND workflow_node_id = $2
		FOR UPDATE
	`, runID, nodeID).Scan(&state, &terminal)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // history without a logical row
	}
	if err != nil {
		return err
	}
	if terminal {
		return nil
	}
	var attempt int32
	var status string
	var childID *uuid.UUID
	var errorMessage *string
	if err := tx.QueryRow(ctx, `
		SELECT s.attempt, s.status, COALESCE(s.run_id, l.run_id), s.error_message
		FROM workflow_run_steps s
		LEFT JOIN workflow_step_launches l
		       ON l.workflow_run_id = s.workflow_run_id
		      AND l.workflow_node_id = s.workflow_node_id
		      AND l.attempt = s.attempt
		WHERE s.workflow_run_id = $1 AND s.workflow_node_id = $2
		ORDER BY s.attempt DESC
		LIMIT 1
	`, runID, nodeID).Scan(&attempt, &status, &childID, &errorMessage); err != nil {
		return err
	}
	next := state
	retryExhausted := false
	switch status {
	case attemptPending:
		next = nodeWaiting
		if attempt > 1 {
			next = nodeRetryWait
		}
	case attemptRunning:
		next = nodeRunning
	case attemptSuccess:
		next = nodeSuccess
	case attemptTimeout:
		next = nodeTimeout
	case attemptCanceled:
		next = nodeCanceled
	case attemptFailed:
		next, retryExhausted, err = s.recomputeFailedNode(ctx, tx, runID, nodeID, attempt, childID, errorMessage)
		if err != nil {
			return err
		}
		if next == nodeRetryWait {
			attempt++ // the inserted pending attempt is now effective
		}
	}
	_, err = tx.Exec(ctx, `
		UPDATE workflow_run_nodes
		SET state = $3,
		    effective_attempt = $4,
		    retry_exhausted = $5,
		    terminal_at = CASE WHEN $6 THEN clock_timestamp() ELSE NULL END,
		    updated_at = clock_timestamp()
		WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND terminal_at IS NULL
	`, runID, nodeID, next, attempt, retryExhausted, workflowNodeStateTerminal(next))
	return err
}

// recomputeFailedNode decides whether a failed attempt is retried. A retry
// needs: budget left, the run still active, the attempt not stopped by a
// workflow failure, a child not canceled by its owner, and, when a child was
// created, Runtime evidence that it stopped (confirmed). While the child may
// still be running the node is stopping; unconfirmed is final.
func (s *Service) recomputeFailedNode(
	ctx context.Context,
	tx pgx.Tx,
	runID, nodeID uuid.UUID,
	attempt int32,
	childID *uuid.UUID,
	errorMessage *string,
) (string, bool, error) {
	var runStatus string
	var config []byte
	var semantics string
	// The node of the run's own version (the workflow_nodes projection only
	// holds the current version).
	if err := tx.QueryRow(ctx, `
		SELECT r.status, n.config, v.semantics
		FROM workflow_runs r
		JOIN workflow_version_nodes n ON n.id = $2
		JOIN workflow_versions v ON v.id = n.workflow_version_id
		WHERE r.id = $1
	`, runID, nodeID).Scan(&runStatus, &config, &semantics); err != nil {
		return "", false, err
	}
	policy := s.workflowNodePolicy(semantics, config)
	if attempt >= policy.maxAttempts {
		return nodeFailed, true, nil
	}
	if runStatus != workflowRunStatusRunning && runStatus != workflowRunStatusPaused && runStatus != workflowRunStatusPending {
		return nodeFailed, false, nil
	}
	var stoppedByFailure bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
		    SELECT 1 FROM workflow_step_stops
		    WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3 AND reason = $4
		)
	`, runID, nodeID, attempt, stopReasonWorkflowFailure).Scan(&stoppedByFailure); err != nil {
		return "", false, err
	}
	if stoppedByFailure {
		return nodeFailed, false, nil
	}
	if errorMessage != nil && len(*errorMessage) >= len(workflowChildCanceledPrefix) &&
		(*errorMessage)[:len(workflowChildCanceledPrefix)] == workflowChildCanceledPrefix {
		return nodeFailed, false, nil
	}
	if childID != nil {
		evidence, err := runtime.ClassifyWorkflowChildStop(ctx, tx, *childID)
		if err != nil {
			return "", false, err
		}
		switch evidence.State {
		case runtime.WorkflowChildStopInProgress:
			return nodeStopping, false, nil
		case runtime.WorkflowChildStopUnconfirmed:
			return nodeFailed, false, nil
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO workflow_run_steps (
		    workflow_run_id, workflow_node_id, node_key, agent_id, status, input, sequence, attempt, not_before
		)
		SELECT workflow_run_id, workflow_node_id, node_key, agent_id, 'pending', '{}'::jsonb, sequence, attempt + 1,
		       clock_timestamp() + make_interval(secs => $4)
		FROM workflow_run_steps
		WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
		ON CONFLICT (workflow_run_id, workflow_node_id, attempt) DO NOTHING
	`, runID, nodeID, attempt, policy.backoff.Seconds()); err != nil {
		return "", false, err
	}
	return nodeRetryWait, false, nil
}
