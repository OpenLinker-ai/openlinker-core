package agent

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	defaultAvailabilityMonitorInterval = 5 * time.Minute
	defaultAvailabilityMonitorStale    = 15 * time.Minute
	defaultAvailabilityMonitorBatch    = 20
	availabilityClaimCleanupTimeout    = 5 * time.Second
)

// errAvailabilityResultDiscarded marks a monitor result that was not stored:
// the Agent row changed, the check lock session was lost, or the guarded write
// failed. Nothing is written and no alert is sent; the next pass re-checks.
var errAvailabilityResultDiscarded = errors.New("availability check result discarded")

// AvailabilityMonitorConfig controls the background platform health checker.
type AvailabilityMonitorConfig struct {
	Interval     time.Duration
	InitialDelay time.Duration
	StaleAfter   time.Duration
	BatchSize    int32
}

// StartAvailabilityMonitor periodically dry-runs due direct_http / mcp_server Agents
// and creates creator-visible alerts when availability changes.
func StartAvailabilityMonitor(ctx context.Context, svc *Service, cfg AvailabilityMonitorConfig) {
	if svc == nil {
		return
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultAvailabilityMonitorInterval
	}
	if cfg.StaleAfter <= 0 {
		cfg.StaleAfter = defaultAvailabilityMonitorStale
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultAvailabilityMonitorBatch
	}

	go func() {
		if cfg.InitialDelay > 0 {
			timer := time.NewTimer(cfg.InitialDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}

		runAvailabilityMonitorTick(ctx, svc, cfg)
		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runAvailabilityMonitorTick(ctx, svc, cfg)
			}
		}
	}()
}

func runAvailabilityMonitorTick(ctx context.Context, svc *Service, cfg AvailabilityMonitorConfig) {
	resp, err := svc.RunDueAvailabilityChecks(ctx, cfg.BatchSize, clampDurationSecondsToInt32(cfg.StaleAfter))
	if err != nil {
		log.Warn().Err(err).Msg("agent.availabilityMonitor")
		return
	}
	if resp.Checked > 0 {
		log.Info().
			Int32("checked", resp.Checked).
			Int32("passed", resp.Passed).
			Int32("failed", resp.Failed).
			Msg("agent availability monitor tick")
	}
}

// RunDueAvailabilityChecks executes one monitor batch. It is exposed for tests
// and operational one-shot runs; regular production use should go through
// StartAvailabilityMonitor.
func (s *Service) RunDueAvailabilityChecks(ctx context.Context, limit, staleSeconds int32) (*AvailabilityCheckBatchResponse, error) {
	if s.dryRunner == nil {
		return nil, httpx.ServiceUnavailable("availability monitor 暂不可用")
	}
	if limit <= 0 || limit > 100 {
		limit = defaultAvailabilityMonitorBatch
	}
	if staleSeconds < 0 {
		staleSeconds = 0
	}

	agents, err := s.queries.ListAgentsDueAvailabilityCheck(ctx, db.ListAgentsDueAvailabilityCheckParams{
		StaleSeconds: staleSeconds,
		Limit:        limit,
	})
	if err != nil {
		log.Error().Err(err).Msg("agent.RunDueAvailabilityChecks: ListAgentsDueAvailabilityCheck")
		return nil, httpx.Internal("查询待巡检 Agent 失败")
	}

	resp := &AvailabilityCheckBatchResponse{
		Alerts: []AvailabilityAlertResponse{},
	}
	claims := availabilityCheckClaims{pool: s.pool}
	defer claims.close()
	for i := range agents {
		agentRow := agents[i]
		claimed, err := claims.claim(ctx, agentRow.ID, staleSeconds)
		if err != nil {
			log.Warn().Err(err).Str("agent_id", agentRow.ID.String()).Msg("agent.RunDueAvailabilityChecks: claim check")
			continue
		}
		if !claimed {
			continue
		}
		s.checkDueAgentAvailability(ctx, &agentRow, availabilityResultGuard{lockConn: claims.conn}, resp)
		claims.release(agentRow.ID)
	}
	return resp, nil
}

func (s *Service) checkDueAgentAvailability(
	ctx context.Context,
	agentRow *db.Agent,
	guard availabilityResultGuard,
	resp *AvailabilityCheckBatchResponse,
) {
	before := s.agentAvailability(ctx, agentRow.ID, agentRow.ConnectionMode)
	dryRun, err := s.runDryRun(ctx, agentRow.ID, agentRow.CreatorID, &guard)
	var preflightAvailability Availability
	if err != nil && !errors.Is(err, errAvailabilityResultDiscarded) {
		// A preflight failure (missing example / capability, schema mismatch)
		// is stored through the same guard, against the row this batch listed.
		var stored bool
		preflightAvailability, stored = s.recordGuardedAvailabilityResult(ctx, agentRow, guard, "fail", err.Error(), nil)
		if !stored {
			err = errAvailabilityResultDiscarded
		}
	}
	if errors.Is(err, errAvailabilityResultDiscarded) {
		return
	}
	resp.Checked++
	if err != nil {
		errMsg := err.Error()
		resp.Failed++
		alert, alertErr := s.createAvailabilityFailureAlert(ctx, agentRow, preflightAvailability, errMsg, nil)
		if alertErr != nil {
			log.Warn().Err(alertErr).Str("agent_id", agentRow.ID.String()).Msg("agent.RunDueAvailabilityChecks: create failure alert")
		} else {
			resp.Alerts = append(resp.Alerts, *alert)
		}
		return
	}
	if dryRun.Result == "fail" {
		resp.Failed++
		errMsg := ""
		if dryRun.Error != nil {
			errMsg = *dryRun.Error
		}
		alert, alertErr := s.createAvailabilityFailureAlert(ctx, agentRow, dryRun.Availability, errMsg, dryRun.RepairHints)
		if alertErr != nil {
			log.Warn().Err(alertErr).Str("agent_id", agentRow.ID.String()).Msg("agent.RunDueAvailabilityChecks: create failure alert")
		} else {
			resp.Alerts = append(resp.Alerts, *alert)
		}
		return
	}
	resp.Passed++
	if before.Status == "degraded" || before.Status == "unreachable" {
		alert, alertErr := s.createAvailabilityRecoveredAlert(ctx, agentRow, dryRun.Availability)
		if alertErr != nil {
			log.Warn().Err(alertErr).Str("agent_id", agentRow.ID.String()).Msg("agent.RunDueAvailabilityChecks: create recovery alert")
		} else {
			resp.Alerts = append(resp.Alerts, *alert)
		}
	}
}

// availabilityCheckClaims coordinates the monitor across Core replicas: every
// replica runs it, so each Agent is probed only by the replica holding its
// session-level advisory lock, and only if its snapshot is still stale once
// the lock is held. A replica that finished a check wrote last_checked_at
// before unlocking, so one replica probes a given Agent per staleness window.
// The locks live on one connection taken out of the pool for the batch: it
// never pins pool capacity the probe itself needs, and closing it releases any
// lock that could not be released explicitly.
type availabilityCheckClaims struct {
	pool *pgxpool.Pool
	conn *pgx.Conn
}

// availabilityResultGuard routes a monitor result write through the session
// that holds the Agent's check lock. The write commits only while that
// session, and therefore the lock, is alive; the Agent row version comes from
// the row the check read. The connection is used only by the batch goroutine.
type availabilityResultGuard struct {
	lockConn *pgx.Conn
}

func (c *availabilityCheckClaims) claim(ctx context.Context, agentID uuid.UUID, staleSeconds int32) (bool, error) {
	if c.conn == nil {
		pooled, err := c.pool.Acquire(ctx)
		if err != nil {
			return false, err
		}
		c.conn = pooled.Hijack()
	}
	locks := db.New(c.conn)
	locked, err := locks.TryLockAgentAvailabilityCheck(ctx, agentID)
	if err != nil {
		c.close()
		return false, err
	}
	if !locked {
		return false, nil
	}
	due, err := locks.IsAgentAvailabilityCheckDue(ctx, db.IsAgentAvailabilityCheckDueParams{
		AgentID:      agentID,
		StaleSeconds: staleSeconds,
	})
	if err != nil || !due {
		c.release(agentID)
		return false, err
	}
	return true, nil
}

func (c *availabilityCheckClaims) release(agentID uuid.UUID) {
	if c.conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), availabilityClaimCleanupTimeout)
	defer cancel()
	released, err := db.New(c.conn).UnlockAgentAvailabilityCheck(ctx, agentID)
	if err != nil || !released {
		log.Warn().Err(err).Str("agent_id", agentID.String()).
			Msg("agent.RunDueAvailabilityChecks: unlock check; closing lock connection")
		c.close()
	}
}

func (c *availabilityCheckClaims) close() {
	if c.conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), availabilityClaimCleanupTimeout)
	defer cancel()
	if err := c.conn.Close(ctx); err != nil {
		log.Warn().Err(err).Msg("agent.RunDueAvailabilityChecks: close lock connection")
	}
	c.conn = nil
}

func (s *Service) ListAvailabilityAlerts(ctx context.Context, creatorID uuid.UUID, limit int32) (*AvailabilityAlertListResponse, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	rows, err := s.queries.ListAgentAvailabilityAlertsByCreator(ctx, db.ListAgentAvailabilityAlertsByCreatorParams{
		CreatorID: creatorID,
		Limit:     limit,
	})
	if err != nil {
		log.Error().Err(err).Str("creator_id", creatorID.String()).Msg("agent.ListAvailabilityAlerts")
		return nil, httpx.Internal("查询可用性告警失败")
	}
	total, err := s.queries.CountAgentAvailabilityAlertsByCreator(ctx, creatorID)
	if err != nil {
		log.Error().Err(err).Str("creator_id", creatorID.String()).Msg("agent.ListAvailabilityAlerts: count")
		return nil, httpx.Internal("查询可用性告警失败")
	}
	unread, err := s.queries.CountUnreadAgentAvailabilityAlertsByCreator(ctx, creatorID)
	if err != nil {
		log.Error().Err(err).Str("creator_id", creatorID.String()).Msg("agent.ListAvailabilityAlerts: unread")
		return nil, httpx.Internal("查询未读可用性告警失败")
	}
	items := make([]AvailabilityAlertResponse, 0, len(rows))
	for i := range rows {
		items = append(items, availabilityAlertRowToResponse(&rows[i]))
	}
	return &AvailabilityAlertListResponse{Items: items, Total: total, Unread: unread}, nil
}

func (s *Service) MarkAvailabilityAlertRead(ctx context.Context, creatorID, alertID uuid.UUID) (*AvailabilityAlertResponse, error) {
	alert, err := s.queries.MarkAgentAvailabilityAlertRead(ctx, db.MarkAgentAvailabilityAlertReadParams{
		ID:        alertID,
		CreatorID: creatorID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("可用性告警不存在")
		}
		log.Error().Err(err).Str("alert_id", alertID.String()).Msg("agent.MarkAvailabilityAlertRead")
		return nil, httpx.Internal("更新可用性告警失败")
	}
	resp := availabilityAlertToResponse(&alert, "", "")
	return &resp, nil
}

func (s *Service) createAvailabilityFailureAlert(
	ctx context.Context,
	agentRow *db.Agent,
	availability Availability,
	errMsg string,
	repairHints []string,
) (*AvailabilityAlertResponse, error) {
	severity := "warning"
	if availability.Status == "unreachable" {
		severity = "critical"
	}
	title := agentRow.Name + " 可用性异常"
	message := "平台巡检调用失败，已将 Agent 标记为" + availability.Label + "。"
	if availability.ConsecutiveFailures > 0 {
		message += "连续失败次数会影响市场排序和用户信任。"
	}
	if len(repairHints) == 0 {
		repairHints = repairHintsForDryRun(agentRow, errMsg)
	}
	alert, err := s.queries.UpsertAgentAvailabilityAlert(ctx, db.UpsertAgentAvailabilityAlertParams{
		AgentID:             agentRow.ID,
		CreatorID:           agentRow.CreatorID,
		AlertType:           "availability_failed",
		Severity:            severity,
		AvailabilityStatus:  availability.Status,
		ConsecutiveFailures: availability.ConsecutiveFailures,
		Title:               title,
		Message:             message,
		LastError:           stringPtrOrNil(errMsg),
		RepairHints:         repairHints,
	})
	if err != nil {
		return nil, err
	}
	resp := availabilityAlertToResponse(&alert, agentRow.Slug, agentRow.Name)
	return &resp, nil
}

func (s *Service) createAvailabilityRecoveredAlert(ctx context.Context, agentRow *db.Agent, availability Availability) (*AvailabilityAlertResponse, error) {
	alert, err := s.queries.UpsertAgentAvailabilityAlert(ctx, db.UpsertAgentAvailabilityAlertParams{
		AgentID:             agentRow.ID,
		CreatorID:           agentRow.CreatorID,
		AlertType:           "availability_recovered",
		Severity:            "info",
		AvailabilityStatus:  availability.Status,
		ConsecutiveFailures: availability.ConsecutiveFailures,
		Title:               agentRow.Name + " 已恢复可用",
		Message:             "平台巡检已通过，Agent 可用性恢复为" + availability.Label + "。",
		RepairHints:         []string{},
	})
	if err != nil {
		return nil, err
	}
	resp := availabilityAlertToResponse(&alert, agentRow.Slug, agentRow.Name)
	return &resp, nil
}

func availabilityAlertRowToResponse(row *db.ListAgentAvailabilityAlertsByCreatorRow) AvailabilityAlertResponse {
	return availabilityAlertToResponse(&row.AgentAvailabilityAlert, row.AgentSlug, row.AgentName)
}

func availabilityAlertToResponse(alert *db.AgentAvailabilityAlert, slug, name string) AvailabilityAlertResponse {
	return AvailabilityAlertResponse{
		ID:                  alert.ID.String(),
		AgentID:             alert.AgentID.String(),
		AgentSlug:           slug,
		AgentName:           name,
		Type:                alert.AlertType,
		Severity:            alert.Severity,
		AvailabilityStatus:  alert.AvailabilityStatus,
		ConsecutiveFailures: alert.ConsecutiveFailures,
		Title:               alert.Title,
		Message:             alert.Message,
		LastError:           alert.LastError,
		RepairHints:         alert.RepairHints,
		ReadAt:              formatOptionalTime(alert.ReadAt),
		CreatedAt:           alert.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:           alert.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func stringPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func formatOptionalTime(t *time.Time) *string {
	if t == nil {
		return nil
	}
	formatted := t.UTC().Format(time.RFC3339)
	return &formatted
}
