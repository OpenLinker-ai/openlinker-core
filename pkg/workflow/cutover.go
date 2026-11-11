package workflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Workflow release cutover checks. A release drains every workflow run before
// migrating (admission 'draining', every run finished, no cancellation still
// reconciling), migrates history only, verifies the result while admission
// stays draining, and only then reopens admission.

const (
	CutoverBlockerAdmissionOpen        = "workflow_admission_open"
	CutoverBlockerNonterminalRuns      = "workflow_runs_nonterminal"
	CutoverBlockerCancellationsPending = "workflow_cancellations_pending"
	CutoverBlockerAttemptGaps          = "workflow_attempt_gaps"
	CutoverBlockerUnlinkedLaunches     = "workflow_launches_unlinked"
	CutoverBlockerSchemaMissing        = "workflow_schema_missing"
)

// ErrWorkflowCutoverBlocked means the checks found blockers; the report lists
// them. It is not an infrastructure failure.
var ErrWorkflowCutoverBlocked = errors.New("workflow cutover is blocked")

type CutoverBlocker struct {
	Code   string `json:"code"`
	Count  int64  `json:"count,omitempty"`
	Detail string `json:"detail"`
}

type CutoverReport struct {
	Phase                 string           `json:"phase"`
	AdmissionState        string           `json:"admission_state"`
	NonterminalRuns       int64            `json:"nonterminal_runs"`
	CancellationsPending  int64            `json:"cancellations_pending"`
	DuplicateStepNodes    int64            `json:"duplicate_step_nodes"`
	UnlinkedLaunches      int64            `json:"unlinked_launches"`
	LegacyOrphanLaunches  int64            `json:"legacy_orphan_launches"`
	LegacyUnfinishedSteps int64            `json:"legacy_unfinished_steps"`
	AttemptGaps           int64            `json:"attempt_gaps"`
	Blockers              []CutoverBlocker `json:"blockers"`
	Ready                 bool             `json:"ready"`
}

type cutoverQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// PreflightWorkflowCutover is read-only and runs against the schema before
// the workflow attempt migration (it needs only migration 103).
func PreflightWorkflowCutover(ctx context.Context, pool *pgxpool.Pool) (CutoverReport, error) {
	report := CutoverReport{Phase: "preflight", Blockers: []CutoverBlocker{}}
	if err := readDrainState(ctx, pool, &report); err != nil {
		return report, err
	}
	// Informational: what the migration will number or keep as orphans.
	if err := pool.QueryRow(ctx, `
		SELECT
		    (SELECT count(*) FROM (
		        SELECT 1 FROM workflow_run_steps
		        GROUP BY workflow_run_id, workflow_node_id HAVING count(*) > 1
		    ) duplicates),
		    (SELECT count(*) FROM workflow_step_launches WHERE workflow_run_step_id IS NULL)
	`).Scan(&report.DuplicateStepNodes, &report.UnlinkedLaunches); err != nil {
		return report, fmt.Errorf("inspect workflow history: %w", err)
	}
	return finishCutoverReport(report)
}

// VerifyWorkflowCutover is read-only and runs after the workflow attempt
// migration, while admission is still draining.
func VerifyWorkflowCutover(ctx context.Context, pool *pgxpool.Pool) (CutoverReport, error) {
	report := CutoverReport{Phase: "verify", Blockers: []CutoverBlocker{}}
	var hasAttempts bool
	if err := pool.QueryRow(ctx,
		`SELECT to_regclass('public.workflow_run_nodes') IS NOT NULL AND to_regclass('public.workflow_step_stops') IS NOT NULL`,
	).Scan(&hasAttempts); err != nil {
		return report, fmt.Errorf("inspect workflow schema: %w", err)
	}
	if !hasAttempts {
		report.Blockers = append(report.Blockers, CutoverBlocker{
			Code: CutoverBlockerSchemaMissing, Detail: "workflow attempt schema is not migrated",
		})
		return finishCutoverReport(report)
	}
	if err := readDrainState(ctx, pool, &report); err != nil {
		return report, err
	}
	if err := pool.QueryRow(ctx, `
		SELECT
		    (SELECT count(*) FROM (
		        SELECT 1 FROM workflow_run_steps
		        GROUP BY workflow_run_id, workflow_node_id
		        HAVING max(attempt) <> count(*) OR min(attempt) <> 1
		    ) gaps),
		    (SELECT count(*) FROM workflow_step_launches
		     WHERE workflow_run_step_id IS NULL AND NOT legacy_orphan),
		    (SELECT count(*) FROM workflow_step_launches WHERE legacy_orphan),
		    (SELECT count(*) FROM workflow_run_steps WHERE legacy_unfinished)
	`).Scan(&report.AttemptGaps, &report.UnlinkedLaunches, &report.LegacyOrphanLaunches, &report.LegacyUnfinishedSteps); err != nil {
		return report, fmt.Errorf("inspect workflow attempts: %w", err)
	}
	if report.AttemptGaps > 0 {
		report.Blockers = append(report.Blockers, CutoverBlocker{
			Code: CutoverBlockerAttemptGaps, Count: report.AttemptGaps,
			Detail: "attempts for a node must be numbered 1..n without gaps",
		})
	}
	if report.UnlinkedLaunches > 0 {
		report.Blockers = append(report.Blockers, CutoverBlocker{
			Code: CutoverBlockerUnlinkedLaunches, Count: report.UnlinkedLaunches,
			Detail: "every launch must link its attempt or be a legacy orphan",
		})
	}
	return finishCutoverReport(report)
}

// ReopenWorkflowAdmission verifies first and reopens only when nothing blocks.
func ReopenWorkflowAdmission(ctx context.Context, pool *pgxpool.Pool, reason string) (CutoverReport, error) {
	report, err := VerifyWorkflowCutover(ctx, pool)
	if err != nil {
		return report, err
	}
	// Reopening is the step that ends the drain, so an open admission is not a
	// blocker here; everything else is.
	blockers := report.Blockers[:0]
	for _, blocker := range report.Blockers {
		if blocker.Code != CutoverBlockerAdmissionOpen {
			blockers = append(blockers, blocker)
		}
	}
	report.Blockers = blockers
	report.Phase = "reopen"
	if len(report.Blockers) > 0 {
		report.Ready = false
		return report, ErrWorkflowCutoverBlocked
	}
	if reason == "" {
		reason = "workflow release verified"
	}
	if err := pool.QueryRow(ctx, `
		UPDATE workflow_admission
		SET state = 'open', reason = $1, changed_by = NULL, changed_at = clock_timestamp()
		WHERE singleton
		RETURNING state
	`, reason).Scan(&report.AdmissionState); err != nil {
		return report, fmt.Errorf("reopen workflow admission: %w", err)
	}
	report.Ready = true
	return report, nil
}

func readDrainState(ctx context.Context, q cutoverQuerier, report *CutoverReport) error {
	if err := q.QueryRow(ctx, `
		SELECT
		    (SELECT state FROM workflow_admission WHERE singleton),
		    (SELECT count(*) FROM workflow_runs WHERE status IN ('pending', 'running', 'paused')),
		    (SELECT count(*) FROM workflow_run_cancellations WHERE state IN ('requested', 'stopping'))
	`).Scan(&report.AdmissionState, &report.NonterminalRuns, &report.CancellationsPending); err != nil {
		return fmt.Errorf("inspect workflow drain state: %w", err)
	}
	if report.AdmissionState != WorkflowAdmissionDraining {
		report.Blockers = append(report.Blockers, CutoverBlocker{
			Code: CutoverBlockerAdmissionOpen, Detail: "workflow admission must be draining",
		})
	}
	if report.NonterminalRuns > 0 {
		report.Blockers = append(report.Blockers, CutoverBlocker{
			Code: CutoverBlockerNonterminalRuns, Count: report.NonterminalRuns,
			Detail: "every workflow run must be finished; cancel or wait for pending, running and paused runs",
		})
	}
	if report.CancellationsPending > 0 {
		report.Blockers = append(report.Blockers, CutoverBlocker{
			Code: CutoverBlockerCancellationsPending, Count: report.CancellationsPending,
			Detail: "workflow cancellations are still reconciling",
		})
	}
	return nil
}

func finishCutoverReport(report CutoverReport) (CutoverReport, error) {
	report.Ready = len(report.Blockers) == 0
	if !report.Ready {
		return report, ErrWorkflowCutoverBlocked
	}
	return report, nil
}
