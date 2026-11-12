package runtime

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

// WorkflowChildStopState is Runtime's classification of whether a workflow
// child Run is known to have stopped executing. The workflow engine uses only
// this classification; it never interprets Runtime tables itself.
type WorkflowChildStopState string

const (
	// WorkflowChildStopInProgress: the Run or one of its Attempts has not
	// ended, or a cancellation is still requested/delivered/stopping.
	WorkflowChildStopInProgress WorkflowChildStopState = "in_progress"
	// WorkflowChildStopConfirmed: the Run is terminal and every Attempt
	// either never started or ended with a definite result its executor
	// reported without being canceled.
	WorkflowChildStopConfirmed WorkflowChildStopState = "confirmed"
	// WorkflowChildStopUnconfirmed: everything else. Runtime cannot prove
	// the backend stopped (a canceled accepted Attempt, a presumed ending
	// such as lease_expired/timeout/result_unknown, or an unknown result).
	WorkflowChildStopUnconfirmed WorkflowChildStopState = "unconfirmed"
)

// WorkflowChildStopEvidence is the read-only answer for one child Run.
type WorkflowChildStopEvidence struct {
	State WorkflowChildStopState
	// RunTerminal reports that the child Run and all of its Attempts ended.
	RunTerminal bool
	// CancellationUnconfirmed reports that Runtime's own cancellation of the
	// child ended without a stop confirmation.
	CancellationUnconfirmed bool
	Reason                  string
}

type workflowChildStopAttempt struct {
	id         uuid.UUID
	accepted   bool
	finished   bool
	outcome    string
	hasResult  bool
	errorCode  string
	canceledBy bool
}

// runtimeResultUnknownErrorCodes are Runtime-assigned codes that mean the
// backend outcome is not known even though an Attempt carries them.
var runtimeResultUnknownErrorCodes = map[string]struct{}{
	string(RuntimeErrorEndpointResultUnknown): {},
	string(RuntimeErrorLeaseExpired):          {},
	string(RuntimeErrorRunDeadlineExceeded):   {},
	string(RuntimeErrorRetryExhausted):        {},
	string(RuntimeErrorRunCancelRequested):    {},
	string(RuntimeErrorRunCancelUnconfirmed):  {},
	string(RuntimeErrorDispatchTimeout):       {},
	string(RuntimeErrorStaleLease):            {},
	string(RuntimeErrorLeaseIdentityMismatch): {},
	string(RuntimeErrorSessionConflict):       {},
	string(RuntimeErrorSpoolCorrupt):          {},
	string(RuntimeErrorEventsMissing):         {},
}

// runtimeResultCodeIsDefinite reports whether an executor-reported result
// carrying this error code is a definite outcome. Runtime's own "unknown"
// codes, and any code that names itself unknown, are not.
func runtimeResultCodeIsDefinite(code string) bool {
	code = strings.TrimSpace(code)
	if code == "" {
		return true
	}
	if _, unknown := runtimeResultUnknownErrorCodes[code]; unknown {
		return false
	}
	return !strings.Contains(strings.ToUpper(code), "UNKNOWN")
}

func runtimeRunStatusTerminal(status string) bool {
	switch status {
	case string(RuntimeRunSuccess), string(RuntimeRunFailed), string(RuntimeRunTimeout), string(RuntimeRunCanceled):
		return true
	}
	return false
}

func classifyWorkflowChildStop(
	runStatus string,
	attempts []workflowChildStopAttempt,
	cancellationState string,
) WorkflowChildStopEvidence {
	evidence := WorkflowChildStopEvidence{}
	switch cancellationState {
	case "unconfirmed", "unsupported", "failed":
		evidence.CancellationUnconfirmed = true
	}
	allFinished := true
	for _, attempt := range attempts {
		if !attempt.finished {
			allFinished = false
		}
	}
	evidence.RunTerminal = runtimeRunStatusTerminal(runStatus) && allFinished
	switch {
	case !runtimeRunStatusTerminal(runStatus):
		evidence.State, evidence.Reason = WorkflowChildStopInProgress, "run_not_terminal"
		return evidence
	case !allFinished:
		evidence.State, evidence.Reason = WorkflowChildStopInProgress, "attempt_not_finished"
		return evidence
	}
	switch cancellationState {
	case "requested", "delivered", "stopping":
		evidence.State, evidence.Reason = WorkflowChildStopInProgress, "cancellation_in_progress"
		return evidence
	}
	for _, attempt := range attempts {
		if !attempt.accepted && !attempt.hasResult {
			continue // never_started
		}
		natural := attempt.hasResult && !attempt.canceledBy &&
			(attempt.outcome == "success" || attempt.outcome == "retryable_failure" || attempt.outcome == "non_retryable_failure") &&
			runtimeResultCodeIsDefinite(attempt.errorCode)
		if !natural {
			evidence.State = WorkflowChildStopUnconfirmed
			switch {
			case attempt.canceledBy:
				evidence.Reason = "accepted_attempt_canceled"
			case attempt.outcome != "":
				evidence.Reason = "attempt_" + attempt.outcome
			default:
				evidence.Reason = "attempt_result_unknown"
			}
			return evidence
		}
	}
	evidence.State, evidence.Reason = WorkflowChildStopConfirmed, "natural_or_never_started"
	return evidence
}

// ClassifyWorkflowChildStop reads the child Run, its Attempts and its
// cancellation in the caller's transaction (or pool) and classifies them. It
// is read-only and takes no locks beyond what the caller holds.
func ClassifyWorkflowChildStop(ctx context.Context, q db.DBTX, childRunID uuid.UUID) (WorkflowChildStopEvidence, error) {
	if q == nil || childRunID == uuid.Nil {
		return WorkflowChildStopEvidence{}, errors.New("workflow child stop classification requires a Run")
	}
	var runStatus string
	if err := q.QueryRow(ctx, `SELECT status FROM runs WHERE id = $1`, childRunID).Scan(&runStatus); err != nil {
		return WorkflowChildStopEvidence{}, err
	}
	var cancellationState string
	var cancellationTarget *uuid.UUID
	err := q.QueryRow(ctx, `
		SELECT state, target_attempt_id FROM run_cancellations WHERE run_id = $1
	`, childRunID).Scan(&cancellationState, &cancellationTarget)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return WorkflowChildStopEvidence{}, err
	}
	rows, err := q.Query(ctx, `
		SELECT id, accepted_at IS NOT NULL, finished_at IS NOT NULL,
		       COALESCE(outcome, ''), result_id IS NOT NULL, COALESCE(error_code, '')
		FROM run_attempts
		WHERE run_id = $1
		ORDER BY created_at, id
	`, childRunID)
	if err != nil {
		return WorkflowChildStopEvidence{}, err
	}
	defer rows.Close()
	attempts := []workflowChildStopAttempt{}
	for rows.Next() {
		var attempt workflowChildStopAttempt
		if err := rows.Scan(&attempt.id, &attempt.accepted, &attempt.finished,
			&attempt.outcome, &attempt.hasResult, &attempt.errorCode); err != nil {
			return WorkflowChildStopEvidence{}, err
		}
		attempt.canceledBy = attempt.outcome == "canceled" ||
			(cancellationTarget != nil && *cancellationTarget == attempt.id)
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return WorkflowChildStopEvidence{}, err
	}
	return classifyWorkflowChildStop(runStatus, attempts, cancellationState), nil
}

// WorkflowChildOutcome is a child Run's terminal facts, read under a share
// lock in the workflow settlement transaction.
type WorkflowChildOutcome struct {
	Status       string
	Terminal     bool
	FinishedAt   *time.Time
	Output       []byte
	ErrorCode    string
	ErrorMessage string
}

// LockWorkflowChildOutcome reads a child Run's status, finish time, output
// and error under FOR SHARE, in the caller's transaction. The workflow
// settlement compares FinishedAt with the attempt deadline.
func LockWorkflowChildOutcome(ctx context.Context, q db.DBTX, childRunID uuid.UUID) (WorkflowChildOutcome, error) {
	var outcome WorkflowChildOutcome
	var errorCode, errorMessage *string
	err := q.QueryRow(ctx, `
		SELECT status, finished_at, output, error_code, error_message
		FROM runs WHERE id = $1
		FOR SHARE
	`, childRunID).Scan(&outcome.Status, &outcome.FinishedAt, &outcome.Output, &errorCode, &errorMessage)
	if err != nil {
		return WorkflowChildOutcome{}, err
	}
	outcome.Terminal = runtimeRunStatusTerminal(outcome.Status)
	if errorCode != nil {
		outcome.ErrorCode = *errorCode
	}
	if errorMessage != nil {
		outcome.ErrorMessage = *errorMessage
	}
	return outcome, nil
}
