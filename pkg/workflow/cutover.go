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

// workflowDrainRequiredVersions are the Core migrations that refuse to run
// while any workflow run is unfinished (104 attempts, 105 versions). A
// release crossing one of them must drain workflow runs first.
var workflowDrainRequiredVersions = []int64{104, 105}

// workflowAdmissionVersion is the first schema with the admission barrier.
const workflowAdmissionVersion int64 = 103

// WorkflowCutoverMode is the release decision for the database's current
// Core schema: "cutover" when upgrading to this release crosses a migration
// that needs drained workflow runs, "none" otherwise.
type WorkflowCutoverMode struct {
	Mode          string `json:"mode"`
	SourceVersion int64  `json:"source_version"`
	Reason        string `json:"reason,omitempty"`
}

// ErrWorkflowCutoverUnsupportedSource: the database predates the admission
// release, so workflow runs cannot be drained by this release.
var ErrWorkflowCutoverUnsupportedSource = errors.New("deploy the workflow admission release (Core schema 103) first")

func DecideWorkflowCutoverMode(ctx context.Context, pool *pgxpool.Pool) (WorkflowCutoverMode, error) {
	var version int64
	var dirty bool
	if err := pool.QueryRow(ctx, `SELECT version, dirty FROM schema_migrations`).Scan(&version, &dirty); err != nil {
		return WorkflowCutoverMode{}, fmt.Errorf("read Core schema version: %w", err)
	}
	mode := WorkflowCutoverMode{Mode: "none", SourceVersion: version}
	if dirty {
		return mode, fmt.Errorf("Core schema %d is dirty", version)
	}
	for _, required := range workflowDrainRequiredVersions {
		if version < required {
			mode.Mode = "cutover"
			mode.Reason = fmt.Sprintf("migration %d requires drained workflow runs", required)
			break
		}
	}
	if mode.Mode == "cutover" && version < workflowAdmissionVersion {
		return mode, ErrWorkflowCutoverUnsupportedSource
	}
	return mode, nil
}

// DrainWorkflowAdmission switches admission to draining (idempotently) and
// reports whether the drain is complete. It works on the schema of the
// release being replaced (103 or later): the admission row is the barrier
// both releases honor. Blocked means runs or cancellations remain.
func DrainWorkflowAdmission(ctx context.Context, pool *pgxpool.Pool, reason string) (CutoverReport, error) {
	report := CutoverReport{Phase: "drain", Blockers: []CutoverBlocker{}}
	if reason == "" {
		reason = "workflow release drain"
	}
	if _, err := pool.Exec(ctx, `
		UPDATE workflow_admission
		SET state = 'draining', reason = $1, changed_by = NULL, changed_at = clock_timestamp()
		WHERE singleton AND state <> 'draining'
	`, reason); err != nil {
		return report, fmt.Errorf("drain workflow admission (requires the schema 103 release): %w", err)
	}
	if err := readDrainState(ctx, pool, &report); err != nil {
		return report, err
	}
	return finishCutoverReport(report)
}

// AbortWorkflowDrain reopens admission without any check. It is only for a
// release abandoned before the workflow attempt migration ran, so the
// previous release can take workflow runs again.
func AbortWorkflowDrain(ctx context.Context, pool *pgxpool.Pool, reason string) (CutoverReport, error) {
	report := CutoverReport{Phase: "abort-drain", Blockers: []CutoverBlocker{}}
	if reason == "" {
		reason = "workflow release drain aborted"
	}
	if err := pool.QueryRow(ctx, `
		UPDATE workflow_admission
		SET state = 'open', reason = $1, changed_by = NULL, changed_at = clock_timestamp()
		WHERE singleton
		RETURNING state
	`, reason).Scan(&report.AdmissionState); err != nil {
		return report, fmt.Errorf("abort workflow drain: %w", err)
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
