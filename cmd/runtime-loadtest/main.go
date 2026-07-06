package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type config struct {
	APIRoot               string
	DatabaseURL           string
	Mode                  string
	Users                 int
	Agents                int
	WorkersPerAgent       int
	Runs                  int
	RunConcurrency        int
	SetupConcurrency      int
	SetupUserConcurrency  int
	SetupAgentConcurrency int
	EventsPerRun          int
	ResultDelay           time.Duration
	SubmitDuration        time.Duration
	HistoryPerAgent       int
	ContextMode           bool
	AccountRunID          string
	Timeout               time.Duration
	ReadyTimeout          time.Duration
	RequestTimeout        time.Duration
	PullClaimWait         time.Duration
	WSHeartbeat           time.Duration
	WSConnectStagger      time.Duration
	HoldAfter             time.Duration
	Output                string
	InsecureTLS           bool
	FailOnIncomplete      bool
}

type apiClient struct {
	root   string
	client *http.Client
}

type account struct {
	Email string `json:"email"`
	JWT   string `json:"jwt"`
	User  struct {
		ID string `json:"id"`
	} `json:"user"`
}

type verificationCodeResponse struct {
	DebugCode string `json:"debug_code"`
}

type agentRef struct {
	ID          string   `json:"id"`
	Slug        string   `json:"slug"`
	Creator     *account `json:"-"`
	RuntimeKeys []string `json:"-"`
}

type httpMetric struct {
	Op         string
	StatusCode int
	Duration   time.Duration
	Err        string
}

type runRecord struct {
	ClientID    string
	RunID       string
	AgentID     string
	UserID      string
	RootContext string
	Phase       string
	SubmittedAt time.Time
	CreatedAt   time.Time
	AssignedAt  time.Time
	CompletedAt time.Time
	CreateErr   string
	ResultErr   string
}

type runTracker struct {
	mu       sync.Mutex
	byClient map[string]*runRecord
	byRun    map[string]*runRecord
}

func newRunTracker() *runTracker {
	return &runTracker{
		byClient: map[string]*runRecord{},
		byRun:    map[string]*runRecord{},
	}
}

func (t *runTracker) upsertSubmitted(clientID, agentID, userID, root, phase string, submitted time.Time) *runRecord {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := t.byClient[clientID]
	if r == nil {
		r = &runRecord{ClientID: clientID}
		t.byClient[clientID] = r
	}
	r.AgentID = agentID
	r.UserID = userID
	r.RootContext = root
	r.Phase = phase
	r.SubmittedAt = submitted
	return r
}

func (t *runTracker) markCreated(clientID, runID string, at time.Time, errText string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := t.byClient[clientID]
	if r == nil {
		r = &runRecord{ClientID: clientID}
		t.byClient[clientID] = r
	}
	if existing := t.byRun[runID]; runID != "" && existing != nil && existing != r {
		if r.AssignedAt.IsZero() {
			r.AssignedAt = existing.AssignedAt
		}
		if r.CompletedAt.IsZero() {
			r.CompletedAt = existing.CompletedAt
		}
		if r.ResultErr == "" {
			r.ResultErr = existing.ResultErr
		}
	}
	r.RunID = runID
	r.CreatedAt = at
	r.CreateErr = errText
	if runID != "" {
		t.byRun[runID] = r
	}
}

func (t *runTracker) markAssigned(runID, clientID string, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var r *runRecord
	if runID != "" {
		r = t.byRun[runID]
	}
	if r == nil && clientID != "" {
		r = t.byClient[clientID]
	}
	if r == nil {
		r = &runRecord{ClientID: clientID, RunID: runID}
		if clientID != "" {
			t.byClient[clientID] = r
		}
	}
	if runID != "" {
		r.RunID = runID
		t.byRun[runID] = r
	}
	if r.AssignedAt.IsZero() {
		r.AssignedAt = at
	}
}

func (t *runTracker) markCompleted(runID string, at time.Time, errText string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := t.byRun[runID]
	if r == nil {
		r = &runRecord{RunID: runID}
		t.byRun[runID] = r
	}
	if r.CompletedAt.IsZero() {
		r.CompletedAt = at
	}
	r.ResultErr = errText
}

func (t *runTracker) snapshot() []*runRecord {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*runRecord, 0, len(t.byClient)+len(t.byRun))
	seen := map[*runRecord]struct{}{}
	for _, r := range t.byClient {
		if _, ok := seen[r]; ok {
			continue
		}
		cp := *r
		out = append(out, &cp)
		seen[r] = struct{}{}
	}
	for _, r := range t.byRun {
		if _, ok := seen[r]; ok {
			continue
		}
		cp := *r
		out = append(out, &cp)
		seen[r] = struct{}{}
	}
	return out
}

func (t *runTracker) phaseCompleted(phase string) int {
	count := 0
	for _, r := range t.snapshot() {
		if r.Phase == phase && !r.CompletedAt.IsZero() && r.ResultErr == "" {
			count++
		}
	}
	return count
}

func (t *runTracker) phaseRunIDs(phase string) []string {
	ids := []string{}
	for _, r := range t.snapshot() {
		if r.Phase == phase && r.RunID != "" {
			ids = append(ids, r.RunID)
		}
	}
	sort.Strings(ids)
	return ids
}

type counters struct {
	wsConnected       atomic.Int64
	wsReady           atomic.Int64
	wsReconnects      atomic.Int64
	wsDisconnects     atomic.Int64
	wsPings           atomic.Int64
	wsHeartbeats      atomic.Int64
	wsHeartbeatAcks   atomic.Int64
	wsMessages        atomic.Int64
	wsErrors          atomic.Int64
	pullClaims        atomic.Int64
	pullEmpty         atomic.Int64
	assignments       atomic.Int64
	resultAttempts    atomic.Int64
	resultAccepted    atomic.Int64
	workerErrors      atomic.Int64
	unknownAssignment atomic.Int64
}

type metrics struct {
	startedAt time.Time
	endedAt   time.Time

	mu               sync.Mutex
	httpOps          []httpMetric
	wsReadyStartedAt time.Time
	wsReadyAt        []time.Time
	wsErrorSamples   []string
	c                counters
}

type dbActivitySampler struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
	pool   *pgxpool.Pool
	start  time.Time
	end    time.Time

	mu                    sync.Mutex
	samples               int
	maxTotalConnections   int
	maxActiveConnections  int
	maxIdleInTx           int
	maxIdleInTxAgeMs      float64
	maxWaitingConnections int
	maxLockWaitConns      int
	maxWaitingLocks       int
	maxOldestXactMs       float64
	maxQueryAgeMs         float64
	idleInTxPeakActivity  []dbActivityDetail
	lockWaitPeakActivity  []dbActivityDetail
	errors                []string
}

type dbActivityPoint struct {
	totalConnections   int
	activeConnections  int
	idleInTx           int
	idleInTxAgeMs      float64
	waitingConnections int
	lockWaitConns      int
	waitingLocks       int
	oldestXactMs       float64
	queryAgeMs         float64
	activity           []dbActivityDetail
}

type dbActivityDetail struct {
	PID           int
	State         string
	WaitEventType string
	WaitEvent     string
	XactAgeMs     float64
	QueryAgeMs    float64
	Query         string
}

func startDBActivitySampler(parent context.Context, databaseURL string, interval time.Duration) *dbActivitySampler {
	s := &dbActivitySampler{start: time.Now()}
	if strings.TrimSpace(databaseURL) == "" {
		s.addError("database URL is empty")
		return s
	}
	if interval <= 0 {
		interval = time.Second
	}
	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel
	connectCtx, connectCancel := context.WithTimeout(ctx, 5*time.Second)
	pool, err := pgxpool.New(connectCtx, databaseURL)
	connectCancel()
	if err != nil {
		s.addError("connect: " + err.Error())
		return s
	}
	s.pool = pool
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.sample(ctx)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.sample(ctx)
			}
		}
	}()
	return s
}

func (s *dbActivitySampler) sample(parent context.Context) {
	if s == nil || s.pool == nil {
		return
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	point, err := readDBActivityPoint(ctx, s.pool)
	if err != nil {
		if ctx.Err() != nil || parent.Err() != nil {
			return
		}
		s.addError(err.Error())
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.samples++
	if point.totalConnections > s.maxTotalConnections {
		s.maxTotalConnections = point.totalConnections
	}
	if point.activeConnections > s.maxActiveConnections {
		s.maxActiveConnections = point.activeConnections
	}
	if point.idleInTx > s.maxIdleInTx {
		s.maxIdleInTx = point.idleInTx
		s.idleInTxPeakActivity = append([]dbActivityDetail{}, point.activity...)
	}
	if point.idleInTxAgeMs > s.maxIdleInTxAgeMs {
		s.maxIdleInTxAgeMs = point.idleInTxAgeMs
		s.idleInTxPeakActivity = append([]dbActivityDetail{}, point.activity...)
	}
	if point.waitingConnections > s.maxWaitingConnections {
		s.maxWaitingConnections = point.waitingConnections
	}
	if point.lockWaitConns > s.maxLockWaitConns {
		s.maxLockWaitConns = point.lockWaitConns
		s.lockWaitPeakActivity = append([]dbActivityDetail{}, point.activity...)
	}
	if point.waitingLocks > s.maxWaitingLocks {
		s.maxWaitingLocks = point.waitingLocks
		s.lockWaitPeakActivity = append([]dbActivityDetail{}, point.activity...)
	}
	if point.oldestXactMs > s.maxOldestXactMs {
		s.maxOldestXactMs = point.oldestXactMs
	}
	if point.queryAgeMs > s.maxQueryAgeMs {
		s.maxQueryAgeMs = point.queryAgeMs
	}
}

func readDBActivityPoint(ctx context.Context, pool *pgxpool.Pool) (dbActivityPoint, error) {
	row := pool.QueryRow(ctx, `
SELECT
	  COUNT(*)::int AS total_connections,
	  COUNT(*) FILTER (WHERE state = 'active')::int AS active_connections,
	  COUNT(*) FILTER (WHERE state = 'idle in transaction')::int AS idle_in_tx_connections,
	  COALESCE(EXTRACT(EPOCH FROM MAX(now() - xact_start) FILTER (WHERE state = 'idle in transaction' AND xact_start IS NOT NULL)) * 1000, 0)::float8 AS idle_in_tx_age_ms,
	  COUNT(*) FILTER (WHERE wait_event_type IS NOT NULL)::int AS waiting_connections,
	  COUNT(*) FILTER (WHERE wait_event_type = 'Lock')::int AS lock_wait_connections,
  COALESCE(EXTRACT(EPOCH FROM MAX(now() - xact_start) FILTER (WHERE xact_start IS NOT NULL)) * 1000, 0)::float8 AS oldest_xact_ms,
  COALESCE(EXTRACT(EPOCH FROM MAX(now() - query_start) FILTER (WHERE state <> 'idle' AND query_start IS NOT NULL)) * 1000, 0)::float8 AS max_query_age_ms,
  (SELECT COUNT(*)::int FROM pg_locks WHERE NOT granted) AS waiting_locks
FROM pg_stat_activity
WHERE datname = current_database()`)
	var point dbActivityPoint
	if err := row.Scan(
		&point.totalConnections,
		&point.activeConnections,
		&point.idleInTx,
		&point.idleInTxAgeMs,
		&point.waitingConnections,
		&point.lockWaitConns,
		&point.oldestXactMs,
		&point.queryAgeMs,
		&point.waitingLocks,
	); err != nil {
		return dbActivityPoint{}, err
	}
	if point.idleInTx > 0 || point.lockWaitConns > 0 || point.waitingLocks > 0 {
		details, err := readDBActivityDetails(ctx, pool)
		if err != nil {
			return dbActivityPoint{}, err
		}
		point.activity = details
	}
	return point, nil
}

func readDBActivityDetails(ctx context.Context, pool *pgxpool.Pool) ([]dbActivityDetail, error) {
	rows, err := pool.Query(ctx, `
WITH blocking AS (
  SELECT DISTINCT unnest(pg_blocking_pids(pid)) AS pid
  FROM pg_stat_activity
  WHERE datname = current_database()
)
SELECT
  a.pid,
  COALESCE(a.state, '') AS state,
  COALESCE(a.wait_event_type, '') AS wait_event_type,
  COALESCE(a.wait_event, '') AS wait_event,
  COALESCE(EXTRACT(EPOCH FROM now() - a.xact_start) * 1000, 0)::float8 AS xact_age_ms,
  COALESCE(EXTRACT(EPOCH FROM now() - a.query_start) * 1000, 0)::float8 AS query_age_ms,
  LEFT(regexp_replace(COALESCE(a.query, ''), '\s+', ' ', 'g'), 240) AS query
FROM pg_stat_activity a
WHERE a.datname = current_database()
  AND (
    a.state = 'idle in transaction'
    OR a.wait_event_type = 'Lock'
    OR a.pid IN (SELECT pid FROM blocking)
  )
ORDER BY
  (a.wait_event_type = 'Lock') DESC,
  (a.state = 'idle in transaction') DESC,
  a.query_start NULLS LAST,
  a.pid
LIMIT 10`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var details []dbActivityDetail
	for rows.Next() {
		var detail dbActivityDetail
		if err := rows.Scan(
			&detail.PID,
			&detail.State,
			&detail.WaitEventType,
			&detail.WaitEvent,
			&detail.XactAgeMs,
			&detail.QueryAgeMs,
			&detail.Query,
		); err != nil {
			return nil, err
		}
		detail.XactAgeMs = round(detail.XactAgeMs)
		detail.QueryAgeMs = round(detail.QueryAgeMs)
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return details, nil
}

func (s *dbActivitySampler) addError(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.errors) < 10 {
		s.errors = append(s.errors, message)
	}
}

func (s *dbActivitySampler) stopAndReport() map[string]any {
	if s == nil {
		return nil
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	if s.pool != nil {
		s.pool.Close()
	}
	s.end = time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	errorsOut := append([]string{}, s.errors...)
	report := map[string]any{
		"samples":                   s.samples,
		"duration_ms":               ms(s.end.Sub(s.start)),
		"max_total_connections":     s.maxTotalConnections,
		"max_active_connections":    s.maxActiveConnections,
		"max_idle_in_tx":            s.maxIdleInTx,
		"max_idle_in_tx_age_ms":     round(s.maxIdleInTxAgeMs),
		"max_waiting_connections":   s.maxWaitingConnections,
		"max_lock_wait_connections": s.maxLockWaitConns,
		"max_waiting_locks":         s.maxWaitingLocks,
		"max_oldest_xact_ms":        round(s.maxOldestXactMs),
		"max_query_age_ms":          round(s.maxQueryAgeMs),
		"errors":                    errorsOut,
	}
	if len(s.idleInTxPeakActivity) > 0 {
		report["idle_in_tx_peak_activity"] = dbActivityDetailsReport(s.idleInTxPeakActivity)
	}
	if len(s.lockWaitPeakActivity) > 0 {
		report["lock_wait_peak_activity"] = dbActivityDetailsReport(s.lockWaitPeakActivity)
	}
	return report
}

func dbActivityDetailsReport(details []dbActivityDetail) []map[string]any {
	out := make([]map[string]any, 0, len(details))
	for _, detail := range details {
		out = append(out, map[string]any{
			"pid":             detail.PID,
			"state":           detail.State,
			"wait_event_type": detail.WaitEventType,
			"wait_event":      detail.WaitEvent,
			"xact_age_ms":     detail.XactAgeMs,
			"query_age_ms":    detail.QueryAgeMs,
			"query":           detail.Query,
		})
	}
	return out
}

func (m *metrics) recordHTTP(op string, status int, d time.Duration, err error) {
	item := httpMetric{Op: op, StatusCode: status, Duration: d}
	if err != nil {
		item.Err = err.Error()
	}
	m.mu.Lock()
	m.httpOps = append(m.httpOps, item)
	m.mu.Unlock()
}

func (m *metrics) startWSReadyWindow(at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.wsReadyStartedAt = at
}

func (m *metrics) recordWSReady(at time.Time) {
	m.c.wsReady.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wsReadyStartedAt.IsZero() {
		m.wsReadyStartedAt = m.startedAt
	}
	m.wsReadyAt = append(m.wsReadyAt, at)
}

func (m *metrics) wsReadySnapshot() (time.Time, []time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	start := m.wsReadyStartedAt
	if start.IsZero() {
		start = m.startedAt
	}
	out := append([]time.Time{}, m.wsReadyAt...)
	return start, out
}

func (m *metrics) recordWSError(err error) {
	m.c.wsErrors.Add(1)
	if err == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.wsErrorSamples) >= 10 {
		return
	}
	m.wsErrorSamples = append(m.wsErrorSamples, err.Error())
}

func (m *metrics) wsErrorSampleSnapshot() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.wsErrorSamples...)
}

func main() {
	cfg := parseFlags()
	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, "runtime-loadtest:", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	m := &metrics{startedAt: time.Now()}
	ctx = withMetrics(ctx, m)
	tracker := newRunTracker()
	api := newAPIClient(cfg)
	runID := randomSuffix()
	accountRunID := cfg.AccountRunID
	if accountRunID == "" {
		accountRunID = runID
	}

	accounts, err := setupAccounts(ctx, api, cfg, accountRunID, m)
	if err != nil {
		fail(err)
	}
	agents, err := setupAgents(ctx, api, cfg, runID, accounts, m)
	if err != nil {
		fail(err)
	}

	var dbSampler *dbActivitySampler
	if cfg.DatabaseURL != "" {
		dbSampler = startDBActivitySampler(ctx, cfg.DatabaseURL, time.Second)
	}

	workerCtx, stopWorkers := context.WithCancel(ctx)
	var stopWSHeartbeats chan struct{}
	if cfg.Mode == "runtime_ws" && cfg.WSHeartbeat > 0 {
		stopWSHeartbeats = make(chan struct{})
	}
	var workerWG sync.WaitGroup
	if cfg.Mode == "runtime_ws" {
		m.startWSReadyWindow(time.Now())
	}
	workerOrdinal := 0
	for _, agent := range agents {
		for i, token := range agent.RuntimeKeys {
			connectDelay := time.Duration(0)
			if cfg.Mode == "runtime_ws" && cfg.WSConnectStagger > 0 {
				connectDelay = time.Duration(workerOrdinal) * cfg.WSConnectStagger
			}
			workerOrdinal++
			workerWG.Add(1)
			go func(agent agentRef, token string, workerIndex int, connectDelay time.Duration) {
				defer workerWG.Done()
				if cfg.Mode == "runtime_pull" {
					runPullWorker(workerCtx, api, cfg, agent, token, workerIndex, tracker, m)
					return
				}
				if connectDelay > 0 {
					sleepContext(workerCtx, connectDelay)
					if workerCtx.Err() != nil {
						return
					}
				}
				runWebSocketWorker(workerCtx, api, cfg, agent, token, workerIndex, tracker, m, stopWSHeartbeats)
			}(agent, token, i, connectDelay)
		}
	}

	if err := waitForWorkersReady(ctx, cfg, m, len(agents)*cfg.WorkersPerAgent); err != nil {
		stopWorkers()
		workerWG.Wait()
		fail(err)
	}
	if cfg.HistoryPerAgent > 0 {
		if err := submitRuns(ctx, api, cfg, agents, tracker, m, "history", cfg.HistoryPerAgent*len(agents), 1); err != nil {
			stopWorkers()
			workerWG.Wait()
			fail(err)
		}
		if err := waitForPhase(ctx, tracker, "history", cfg.HistoryPerAgent*len(agents), cfg.Timeout); err != nil {
			stopWorkers()
			workerWG.Wait()
			fail(err)
		}
	}

	measuredStart := time.Now()
	if err := submitRuns(ctx, api, cfg, agents, tracker, m, "measured", cfg.Runs, cfg.RunConcurrency); err != nil {
		stopWorkers()
		workerWG.Wait()
		fail(err)
	}
	waitErr := waitForMeasuredPhase(ctx, cfg, tracker, cfg.Runs, cfg.Timeout)
	measuredEnd := time.Now()

	var holdStart, holdEnd time.Time
	var holdErr error
	if waitErr == nil && cfg.HoldAfter > 0 {
		holdStart = time.Now()
		holdErr = holdContext(ctx, cfg.HoldAfter)
		holdEnd = time.Now()
	}
	if stopWSHeartbeats != nil {
		close(stopWSHeartbeats)
		_ = waitForWSHeartbeatAcks(ctx, m, 5*time.Second)
	}

	stopWorkers()
	workerWG.Wait()
	m.endedAt = time.Now()

	report := buildReport(cfg, runID, accountRunID, accounts, agents, tracker, m, measuredStart, measuredEnd, holdStart, holdEnd, waitErr, holdErr)
	if dbSampler != nil {
		report["db_activity_sample"] = dbSampler.stopAndReport()
	}
	if cfg.DatabaseURL != "" {
		dbReport, err := collectDBReport(context.Background(), cfg.DatabaseURL, agents, tracker)
		if err != nil {
			report["db_error"] = err.Error()
		} else {
			report["db"] = dbReport
		}
	}
	if err := writeReport(cfg, report); err != nil {
		fail(err)
	}
	printSummary(report)
	if (waitErr != nil || holdErr != nil) && cfg.FailOnIncomplete {
		os.Exit(1)
	}
	if failures, _ := report["failed"].(int); failures > 0 && cfg.FailOnIncomplete {
		os.Exit(1)
	}
}

func parseFlags() config {
	cfg := config{}
	flag.StringVar(&cfg.APIRoot, "api", envDefault("OPENLINKER_API_ROOT", "http://127.0.0.1:8080/api/v1"), "OpenLinker Core API root")
	flag.StringVar(&cfg.DatabaseURL, "database-url", os.Getenv("DATABASE_URL"), "optional Postgres URL for DB-side counts and query-plan evidence")
	flag.StringVar(&cfg.Mode, "mode", envDefault("OPENLINKER_RUNTIME_LOADTEST_MODE", "runtime_ws"), "runtime_ws or runtime_pull")
	flag.IntVar(&cfg.Users, "users", intEnv("OPENLINKER_RUNTIME_LOADTEST_USERS", 1), "number of creator/caller users")
	flag.IntVar(&cfg.Agents, "agents", intEnv("OPENLINKER_RUNTIME_LOADTEST_AGENTS", 10), "number of runtime agents")
	flag.IntVar(&cfg.WorkersPerAgent, "workers-per-agent", intEnv("OPENLINKER_RUNTIME_LOADTEST_WORKERS_PER_AGENT", 1), "runtime tokens and WS/pull workers per agent")
	flag.IntVar(&cfg.Runs, "runs", intEnv("OPENLINKER_RUNTIME_LOADTEST_RUNS", 100), "measured runs to submit")
	flag.IntVar(&cfg.RunConcurrency, "run-concurrency", intEnv("OPENLINKER_RUNTIME_LOADTEST_RUN_CONCURRENCY", 20), "concurrent run submissions")
	flag.IntVar(&cfg.SetupConcurrency, "setup-concurrency", intEnv("OPENLINKER_RUNTIME_LOADTEST_SETUP_CONCURRENCY", 16), "concurrent disposable user/agent setup operations")
	flag.IntVar(&cfg.SetupUserConcurrency, "setup-user-concurrency", intEnv("OPENLINKER_RUNTIME_LOADTEST_SETUP_USER_CONCURRENCY", 0), "concurrent disposable user setup operations; default min(setup-concurrency, 8)")
	flag.IntVar(&cfg.SetupAgentConcurrency, "setup-agent-concurrency", intEnv("OPENLINKER_RUNTIME_LOADTEST_SETUP_AGENT_CONCURRENCY", 0), "concurrent disposable agent setup operations; default setup-concurrency")
	flag.IntVar(&cfg.EventsPerRun, "events-per-run", intEnv("OPENLINKER_RUNTIME_LOADTEST_EVENTS_PER_RUN", 1), "progress events each worker writes before result")
	flag.DurationVar(&cfg.ResultDelay, "result-delay", durationEnv("OPENLINKER_RUNTIME_LOADTEST_RESULT_DELAY", 0), "artificial worker delay before result, e.g. 50ms")
	flag.DurationVar(&cfg.SubmitDuration, "submit-duration", durationEnv("OPENLINKER_RUNTIME_LOADTEST_SUBMIT_DURATION", 0), "spread measured run submissions across this duration instead of submitting as fast as possible")
	flag.IntVar(&cfg.HistoryPerAgent, "history-per-agent", intEnv("OPENLINKER_RUNTIME_LOADTEST_HISTORY_PER_AGENT", 0), "completed prior A2A-context runs per agent before measured phase")
	flag.BoolVar(&cfg.ContextMode, "a2a-context", boolEnv("OPENLINKER_RUNTIME_LOADTEST_A2A_CONTEXT", true), "include A2A context ids so assignment builds conversation context")
	flag.StringVar(&cfg.AccountRunID, "account-run-id", os.Getenv("OPENLINKER_RUNTIME_LOADTEST_ACCOUNT_RUN_ID"), "reuse accounts from a previous run id while creating fresh agents/runs")
	flag.DurationVar(&cfg.Timeout, "timeout", durationEnv("OPENLINKER_RUNTIME_LOADTEST_TIMEOUT", 2*time.Minute), "overall timeout")
	flag.DurationVar(&cfg.ReadyTimeout, "ready-timeout", durationEnv("OPENLINKER_RUNTIME_LOADTEST_READY_TIMEOUT", 30*time.Second), "timeout for runtime_ws workers to report runtime.ready")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", durationEnv("OPENLINKER_RUNTIME_LOADTEST_REQUEST_TIMEOUT", 20*time.Second), "per HTTP request timeout")
	flag.DurationVar(&cfg.PullClaimWait, "pull-claim-wait", durationEnv("OPENLINKER_RUNTIME_LOADTEST_PULL_CLAIM_WAIT", 25*time.Second), "runtime_pull long-poll wait; capped below request-timeout")
	flag.DurationVar(&cfg.WSHeartbeat, "ws-heartbeat", durationEnv("OPENLINKER_RUNTIME_LOADTEST_WS_HEARTBEAT", 60*time.Second), "runtime_ws application heartbeat interval; set 0 to disable")
	flag.DurationVar(&cfg.WSConnectStagger, "ws-connect-stagger", durationEnv("OPENLINKER_RUNTIME_LOADTEST_WS_CONNECT_STAGGER", 0), "delay increment between runtime_ws worker connection attempts, e.g. 100ms")
	flag.DurationVar(&cfg.HoldAfter, "hold-after-completion", durationEnv("OPENLINKER_RUNTIME_LOADTEST_HOLD_AFTER_COMPLETION", 0), "keep runtime workers connected after measured runs complete, for soak checks")
	flag.StringVar(&cfg.Output, "output", os.Getenv("OPENLINKER_RUNTIME_LOADTEST_OUTPUT"), "JSON report path; default .openlinker-dev/performance/runtime-loadtest-<timestamp>.json")
	flag.BoolVar(&cfg.InsecureTLS, "insecure-tls", boolEnv("OPENLINKER_RUNTIME_LOADTEST_INSECURE_TLS", false), "skip TLS verification for test hosts")
	flag.BoolVar(&cfg.FailOnIncomplete, "fail-on-incomplete", boolEnv("OPENLINKER_RUNTIME_LOADTEST_FAIL_ON_INCOMPLETE", true), "exit non-zero when measured runs do not complete")
	flag.Parse()
	return cfg
}

func (c *config) validate() error {
	c.Mode = strings.TrimSpace(c.Mode)
	if c.Mode != "runtime_ws" && c.Mode != "runtime_pull" {
		return fmt.Errorf("unsupported mode %q", c.Mode)
	}
	if c.Users <= 0 || c.Agents <= 0 || c.WorkersPerAgent <= 0 || c.Runs <= 0 {
		return fmt.Errorf("users, agents, workers-per-agent, and runs must be positive")
	}
	if c.WorkersPerAgent > 10 {
		return fmt.Errorf("workers-per-agent cannot exceed 10 because agent runtime tokens are capped per agent")
	}
	if c.RunConcurrency <= 0 {
		return fmt.Errorf("run-concurrency must be positive")
	}
	if c.SetupConcurrency <= 0 {
		return fmt.Errorf("setup-concurrency must be positive")
	}
	if c.SetupUserConcurrency < 0 || c.SetupAgentConcurrency < 0 {
		return fmt.Errorf("setup user/agent concurrency cannot be negative")
	}
	if c.SetupUserConcurrency == 0 {
		c.SetupUserConcurrency = c.SetupConcurrency
		if c.SetupUserConcurrency > 8 {
			c.SetupUserConcurrency = 8
		}
	}
	if c.SetupAgentConcurrency == 0 {
		c.SetupAgentConcurrency = c.SetupConcurrency
	}
	if c.SetupUserConcurrency <= 0 || c.SetupAgentConcurrency <= 0 {
		return fmt.Errorf("setup user/agent concurrency must be positive")
	}
	if c.Timeout <= 0 || c.RequestTimeout <= 0 {
		return fmt.Errorf("timeouts must be positive")
	}
	if c.HoldAfter < 0 {
		return fmt.Errorf("hold-after-completion cannot be negative")
	}
	if c.SubmitDuration < 0 {
		return fmt.Errorf("submit-duration cannot be negative")
	}
	if c.WSHeartbeat < 0 {
		return fmt.Errorf("ws-heartbeat cannot be negative")
	}
	if c.WSConnectStagger < 0 {
		return fmt.Errorf("ws-connect-stagger cannot be negative")
	}
	if _, err := url.ParseRequestURI(c.APIRoot); err != nil {
		return fmt.Errorf("invalid api root: %w", err)
	}
	return nil
}

func newAPIClient(cfg config) *apiClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.InsecureTLS {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // explicit load-test flag for disposable test hosts.
	}
	return &apiClient{
		root: strings.TrimRight(cfg.APIRoot, "/"),
		client: &http.Client{
			Timeout:   cfg.RequestTimeout,
			Transport: transport,
		},
	}
}

func setupAccounts(ctx context.Context, api *apiClient, cfg config, runID string, m *metrics) ([]account, error) {
	accounts := make([]account, cfg.Users)
	err := runSetupJobs(ctx, cfg.SetupUserConcurrency, cfg.Users, func(ctx context.Context, i int) error {
		email := fmt.Sprintf("openlinker-perf-%s-u%03d@example.local", runID, i)
		password := "password123"
		display := fmt.Sprintf("OpenLinker Perf %s User %03d", runID, i)
		acc, err := ensureAccount(ctx, api, email, password, display, m)
		if err != nil {
			return err
		}
		if _, err := api.do(ctx, "become-creator", http.MethodPost, "/me/become-creator", map[string]any{}, acc.JWT, nil); err != nil {
			return err
		}
		accounts[i] = acc
		return nil
	})
	if err != nil {
		return nil, err
	}
	return accounts, nil
}

func setupAgents(ctx context.Context, api *apiClient, cfg config, runID string, accounts []account, m *metrics) ([]agentRef, error) {
	agents := make([]agentRef, cfg.Agents)
	err := runSetupJobs(ctx, cfg.SetupAgentConcurrency, cfg.Agents, func(ctx context.Context, i int) error {
		creator := &accounts[i%len(accounts)]
		tokenResp := struct {
			PlaintextToken string `json:"plaintext_token"`
		}{}
		if _, err := api.do(ctx, "create-agent-token", http.MethodPost, "/creator/agent-tokens", map[string]any{
			"name":               fmt.Sprintf("runtime-loadtest-%s-agent-%03d", runID, i),
			"expires_in_minutes": 60,
		}, creator.JWT, &tokenResp); err != nil {
			return err
		}
		slug := fmt.Sprintf("perf-%s-a%03d", runID, i)
		reg := struct {
			Agent agentRef `json:"agent"`
		}{}
		if _, err := api.do(ctx, "register-agent", http.MethodPost, "/agent-registration/agents", map[string]any{
			"slug":            slug,
			"name":            fmt.Sprintf("Perf Agent %s %03d", runID, i),
			"description":     "Disposable runtime load-test agent",
			"connection_mode": cfg.Mode,
			"visibility":      "private",
			"tags":            []string{"perf", "runtime"},
		}, tokenResp.PlaintextToken, &reg); err != nil {
			return err
		}
		agent := reg.Agent
		agent.Creator = creator
		agent.RuntimeKeys = append(agent.RuntimeKeys, tokenResp.PlaintextToken)

		for worker := 1; worker < cfg.WorkersPerAgent; worker++ {
			extra := struct {
				PlaintextToken string `json:"plaintext_token"`
			}{}
			if _, err := api.do(ctx, "create-agent-runtime-token", http.MethodPost, "/creator/agent-tokens", map[string]any{
				"name":     fmt.Sprintf("runtime-loadtest-%s-agent-%03d-worker-%02d", runID, i, worker),
				"agent_id": agent.ID,
				"scopes":   []string{"agent:call", "agent:pull"},
			}, creator.JWT, &extra); err != nil {
				return err
			}
			agent.RuntimeKeys = append(agent.RuntimeKeys, extra.PlaintextToken)
		}
		agents[i] = agent
		return nil
	})
	if err != nil {
		return nil, err
	}
	return agents, nil
}

func runSetupJobs(ctx context.Context, concurrency, total int, fn func(context.Context, int) error) error {
	if total <= 0 {
		return nil
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > total {
		concurrency = total
	}

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	setErr := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
	}

	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				if err := fn(workCtx, index); err != nil {
					setErr(err)
					return
				}
			}
		}()
	}

	for i := 0; i < total; i++ {
		select {
		case <-workCtx.Done():
			close(jobs)
			wg.Wait()
			mu.Lock()
			err := firstErr
			mu.Unlock()
			if err != nil {
				return err
			}
			return workCtx.Err()
		case jobs <- i:
		}
	}
	close(jobs)
	wg.Wait()
	mu.Lock()
	err := firstErr
	mu.Unlock()
	return err
}

func ensureAccount(ctx context.Context, api *apiClient, email, password, display string, m *metrics) (account, error) {
	body := map[string]any{"email": email, "password": password}
	var acc account
	status, err := api.do(ctx, "login", http.MethodPost, "/auth/login", body, "", &acc)
	if err == nil && status == http.StatusOK && acc.JWT != "" {
		return acc, nil
	}

	registerBody := map[string]any{
		"email":            email,
		"password":         password,
		"password_confirm": password,
		"display_name":     display,
	}
	var verification verificationCodeResponse
	codeStatus, codeErr := api.do(ctx, "verification-code", http.MethodPost, "/auth/verification-code", map[string]any{
		"email":   email,
		"purpose": "register",
	}, "", &verification)
	if codeErr == nil && codeStatus == http.StatusOK {
		if verification.DebugCode == "" {
			return account{}, fmt.Errorf("ensure account %s failed: verification-code returned no debug_code", email)
		}
		registerBody["verification_code"] = verification.DebugCode
	} else if codeStatus != http.StatusNotFound && codeStatus != http.StatusMethodNotAllowed {
		return account{}, fmt.Errorf("ensure account %s failed: verification-code status=%d err=%v", email, codeStatus, codeErr)
	}

	var registered account
	status, err = api.do(ctx, "register", http.MethodPost, "/auth/register", registerBody, "", &registered)
	if err == nil && status == http.StatusCreated && registered.JWT != "" {
		return registered, nil
	}
	if err == nil && status == http.StatusCreated {
		var loggedIn account
		loginStatus, loginErr := api.do(ctx, "login", http.MethodPost, "/auth/login", body, "", &loggedIn)
		if loginErr == nil && loginStatus == http.StatusOK && loggedIn.JWT != "" {
			return loggedIn, nil
		}
		return account{}, fmt.Errorf("ensure account %s failed: register created account but login status=%d err=%v", email, loginStatus, loginErr)
	}
	return account{}, fmt.Errorf("ensure account %s failed: status=%d err=%v", email, status, err)
}

func (api *apiClient) do(ctx context.Context, op, method, path string, body any, token string, out any) (int, error) {
	status, _, err := api.doWithHeaders(ctx, op, method, path, body, token, out)
	return status, err
}

func (api *apiClient) doWithHeaders(ctx context.Context, op, method, path string, body any, token string, out any) (int, http.Header, error) {
	start := time.Now()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, api.root+path, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := api.client.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			globalMetricsFromContext(ctx).recordHTTP(op, 0, time.Since(start), err)
		}
		return 0, nil, err
	}
	defer res.Body.Close()
	headers := res.Header.Clone()
	raw, readErr := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if readErr != nil {
		globalMetricsFromContext(ctx).recordHTTP(op, res.StatusCode, time.Since(start), readErr)
		return res.StatusCode, headers, readErr
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			wrapped := fmt.Errorf("%s %s decode %d: %w body=%s", method, path, res.StatusCode, err, string(raw))
			globalMetricsFromContext(ctx).recordHTTP(op, res.StatusCode, time.Since(start), wrapped)
			return res.StatusCode, headers, wrapped
		}
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		err := fmt.Errorf("%s %s -> %d: %s", method, path, res.StatusCode, string(raw))
		globalMetricsFromContext(ctx).recordHTTP(op, res.StatusCode, time.Since(start), err)
		return res.StatusCode, headers, err
	}
	globalMetricsFromContext(ctx).recordHTTP(op, res.StatusCode, time.Since(start), nil)
	return res.StatusCode, headers, nil
}

type metricsContextKey struct{}

func withMetrics(ctx context.Context, m *metrics) context.Context {
	return context.WithValue(ctx, metricsContextKey{}, m)
}

func globalMetricsFromContext(ctx context.Context) *metrics {
	if m, ok := ctx.Value(metricsContextKey{}).(*metrics); ok && m != nil {
		return m
	}
	return fallbackMetrics
}

var fallbackMetrics = &metrics{}

func submitRuns(ctx context.Context, api *apiClient, cfg config, agents []agentRef, tracker *runTracker, m *metrics, phase string, total, concurrency int) error {
	ctx = withMetrics(ctx, m)
	jobs := make(chan int)
	errCh := make(chan error, 1)
	recordErr := func(err error) {
		if err == nil {
			return
		}
		select {
		case errCh <- err:
		default:
		}
	}
	var wg sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				agent := agents[index%len(agents)]
				clientID := fmt.Sprintf("%s-%s-%06d", phase, shortID(), index)
				root := ""
				if cfg.ContextMode {
					root = fmt.Sprintf("perf-root-%s", agent.ID)
				}
				submittedAt := time.Now()
				tracker.upsertSubmitted(clientID, agent.ID, agent.Creator.User.ID, root, phase, submittedAt)
				body := map[string]any{
					"agent_id": agent.ID,
					"input": map[string]any{
						"task":           fmt.Sprintf("%s runtime load-test task %d", phase, index),
						"client_task_id": clientID,
						"phase":          phase,
						"agent_id":       agent.ID,
					},
					"metadata": map[string]any{
						"loadtest": true,
						"phase":    phase,
					},
				}
				if cfg.ContextMode {
					body["a2a_context"] = map[string]any{
						"protocol_context_id": root,
						"root_context_id":     root,
						"protocol_task_id":    clientID,
						"target_agent_id":     agent.ID,
						"trace_id":            clientID,
						"source":              "a2a_protocol",
					}
				}
				resp := struct {
					RunID string `json:"run_id"`
				}{}
				status, err := api.do(ctx, "create-run", http.MethodPost, "/runs", body, agent.Creator.JWT, &resp)
				if err != nil {
					tracker.markCreated(clientID, "", time.Now(), err.Error())
					recordErr(err)
					continue
				}
				if status != http.StatusAccepted || resp.RunID == "" {
					err := fmt.Errorf("POST /runs returned status=%d run_id=%q", status, resp.RunID)
					tracker.markCreated(clientID, resp.RunID, time.Now(), err.Error())
					recordErr(err)
					continue
				}
				tracker.markCreated(clientID, resp.RunID, time.Now(), "")
			}
		}()
	}
	pacedStart := time.Now()
	var pacedInterval time.Duration
	if phase == "measured" && cfg.SubmitDuration > 0 && total > 1 {
		pacedInterval = cfg.SubmitDuration / time.Duration(total-1)
	}
	for i := 0; i < total; i++ {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		case jobs <- i:
		}
		if pacedInterval > 0 && i < total-1 {
			next := pacedStart.Add(time.Duration(i+1) * pacedInterval)
			if err := waitUntilContext(ctx, next); err != nil {
				close(jobs)
				wg.Wait()
				return err
			}
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	return nil
}

func waitForPhase(ctx context.Context, tracker *runTracker, phase string, want int, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if tracker.phaseCompleted(phase) >= want {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timeout waiting for %s completion: got=%d want=%d", phase, tracker.phaseCompleted(phase), want)
		case <-ticker.C:
		}
	}
}

func waitForMeasuredPhase(ctx context.Context, cfg config, tracker *runTracker, want int, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	var pool *pgxpool.Pool
	if cfg.DatabaseURL != "" {
		dbCtx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
		var err error
		pool, err = pgxpool.New(dbCtx, cfg.DatabaseURL)
		cancel()
		if err == nil {
			defer pool.Close()
		}
	}
	lastDBCheck := time.Time{}
	for {
		if tracker.phaseCompleted("measured") >= want {
			return nil
		}
		if pool != nil && time.Since(lastDBCheck) >= time.Second {
			lastDBCheck = time.Now()
			ids := tracker.phaseRunIDs("measured")
			if len(ids) >= want {
				success, statuses, err := countRunsByStatus(ctx, pool, ids)
				if err == nil && success >= want {
					return fmt.Errorf("client result ack incomplete after DB success: client_completed=%d db_success=%d want=%d statuses=%s",
						tracker.phaseCompleted("measured"), success, want, formatStatusCounts(statuses))
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timeout waiting for measured completion: got=%d want=%d", tracker.phaseCompleted("measured"), want)
		case <-ticker.C:
		}
	}
}

func waitForWSHeartbeatAcks(ctx context.Context, m *metrics, timeout time.Duration) error {
	if m == nil || timeout <= 0 {
		return nil
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		sent := m.c.wsHeartbeats.Load()
		acked := m.c.wsHeartbeatAcks.Load()
		if acked >= sent {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timeout waiting for websocket heartbeat acks: sent=%d acked=%d", sent, acked)
		case <-ticker.C:
		}
	}
}

func countRunsByStatus(ctx context.Context, pool *pgxpool.Pool, runIDs []string) (int, map[string]int, error) {
	statuses := map[string]int{}
	rows, err := pool.Query(ctx, `
SELECT status, COUNT(*)::int
FROM runs
WHERE id::text = ANY($1)
GROUP BY status`, runIDs)
	if err != nil {
		return 0, statuses, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return 0, statuses, err
		}
		statuses[status] = count
	}
	return statuses["success"], statuses, rows.Err()
}

func formatStatusCounts(statuses map[string]int) string {
	if len(statuses) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(statuses))
	for status := range statuses {
		keys = append(keys, status)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, status := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", status, statuses[status]))
	}
	return strings.Join(parts, ",")
}

type loadtestWSConn struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (c *loadtestWSConn) readJSON(v any) error {
	return c.conn.ReadJSON(v)
}

func (c *loadtestWSConn) writeJSON(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteJSON(v)
}

func (c *loadtestWSConn) writeControl(messageType int, data []byte, deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteControl(messageType, data, deadline)
}

func (c *loadtestWSConn) close() error {
	return c.conn.Close()
}

func runWebSocketWorker(ctx context.Context, api *apiClient, cfg config, agent agentRef, token string, workerIndex int, tracker *runTracker, m *metrics, heartbeatStop <-chan struct{}) {
	reconnectDelay := time.Second
	for ctx.Err() == nil {
		err := runWebSocketSession(ctx, api, cfg, agent, token, workerIndex, tracker, m, heartbeatStop)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			m.recordWSError(err)
			m.c.wsReconnects.Add(1)
			sleepContext(ctx, reconnectDelay)
			continue
		}
		return
	}
}

func runWebSocketSession(ctx context.Context, api *apiClient, cfg config, agent agentRef, token string, workerIndex int, tracker *runTracker, m *metrics, heartbeatStop <-chan struct{}) error {
	wsURL, err := websocketURL(api.root, "/agent-runtime/ws")
	if err != nil {
		m.c.workerErrors.Add(1)
		return err
	}
	dialer := websocket.Dialer{}
	if cfg.InsecureTLS {
		dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // explicit load-test flag for disposable test hosts.
	}
	header := http.Header{"Authorization": []string{"Bearer " + token}}
	rawConn, resp, err := dialer.DialContext(ctx, wsURL, header)
	if err != nil {
		return websocketDialError(err, resp)
	}
	conn := &loadtestWSConn{conn: rawConn}
	defer func() {
		m.c.wsDisconnects.Add(1)
		_ = conn.close()
	}()
	rawConn.SetPingHandler(func(appData string) error {
		m.c.wsPings.Add(1)
		return conn.writeControl(websocket.PongMessage, []byte(appData), time.Now().Add(time.Second))
	})
	go func() {
		<-ctx.Done()
		_ = conn.close()
	}()
	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)
	if cfg.WSHeartbeat > 0 {
		go runWebSocketHeartbeatLoop(ctx, conn, cfg.WSHeartbeat, stopHeartbeat, heartbeatStop, m)
	}
	m.c.wsConnected.Add(1)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		var msg map[string]any
		if err := conn.readJSON(&msg); err != nil {
			return err
		}
		m.c.wsMessages.Add(1)
		switch stringValue(msg["type"]) {
		case "runtime.ready":
			m.recordWSReady(time.Now())
		case "runtime.heartbeat":
			m.c.wsHeartbeatAcks.Add(1)
		case "run.assigned":
			m.c.assignments.Add(1)
			runID := stringValue(msg["run_id"])
			if err := conn.writeJSON(map[string]any{
				"type":   "run.assignment.accepted",
				"id":     uuid.NewString(),
				"run_id": runID,
			}); err != nil {
				m.c.workerErrors.Add(1)
				tracker.markCompleted(runID, time.Now(), err.Error())
				return err
			}
			input := mapValue(msg["input"])
			clientID := stringValue(input["client_task_id"])
			if clientID == "" {
				m.c.unknownAssignment.Add(1)
			}
			tracker.markAssigned(runID, clientID, time.Now())
			if err := completeViaWS(ctx, conn, cfg, runID, clientID, agent.ID, workerIndex); err != nil {
				m.c.workerErrors.Add(1)
				tracker.markCompleted(runID, time.Now(), err.Error())
				return err
			} else {
				m.c.resultAttempts.Add(1)
			}
		case "run.result.accepted":
			runID := stringValue(msg["run_id"])
			m.c.resultAccepted.Add(1)
			tracker.markCompleted(runID, time.Now(), "")
		case "error":
			raw, _ := json.Marshal(msg["error"])
			m.recordWSError(fmt.Errorf("server error message: %s", raw))
		}
	}
}

func websocketDialError(err error, resp *http.Response) error {
	if err == nil || resp == nil {
		return err
	}
	parts := []string{"status=" + resp.Status}
	if retryAfter := strings.TrimSpace(resp.Header.Get("Retry-After")); retryAfter != "" {
		parts = append(parts, "retry_after="+retryAfter)
	}
	if contentType := strings.TrimSpace(resp.Header.Get("Content-Type")); contentType != "" {
		parts = append(parts, "content_type="+contentType)
	}
	if resp.Body != nil {
		defer resp.Body.Close()
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 512))
		if readErr == nil {
			if trimmed := strings.Join(strings.Fields(string(body)), " "); trimmed != "" {
				parts = append(parts, "body="+trimmed)
			}
		}
	}
	return fmt.Errorf("%w (%s)", err, strings.Join(parts, ", "))
}

func runWebSocketHeartbeatLoop(ctx context.Context, conn *loadtestWSConn, interval time.Duration, sessionStop <-chan struct{}, globalStop <-chan struct{}, m *metrics) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sessionStop:
			return
		case <-globalStop:
			return
		case <-ticker.C:
			if err := conn.writeJSON(map[string]any{
				"type": "heartbeat",
				"id":   "heartbeat-" + uuid.NewString(),
			}); err != nil {
				m.c.wsErrors.Add(1)
				return
			}
			m.c.wsHeartbeats.Add(1)
		}
	}
}

func completeViaWS(ctx context.Context, conn *loadtestWSConn, cfg config, runID, clientID, agentID string, workerIndex int) error {
	for i := 0; i < cfg.EventsPerRun; i++ {
		if err := conn.writeJSON(map[string]any{
			"type":       "run.event",
			"id":         uuid.NewString(),
			"run_id":     runID,
			"event_type": "run.message.delta",
			"payload": map[string]any{
				"text":           fmt.Sprintf("worker %d progress %d for %s", workerIndex, i+1, clientID),
				"client_task_id": clientID,
			},
		}); err != nil {
			return err
		}
	}
	if cfg.ResultDelay > 0 {
		timer := time.NewTimer(cfg.ResultDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return conn.writeJSON(map[string]any{
		"type":   "run.result",
		"id":     uuid.NewString(),
		"run_id": runID,
		"status": "success",
		"output": map[string]any{
			"ok":             true,
			"agent_id":       agentID,
			"client_task_id": clientID,
			"worker_index":   workerIndex,
		},
	})
}

func waitForWorkersReady(ctx context.Context, cfg config, m *metrics, want int) error {
	if cfg.Mode == "runtime_pull" {
		time.Sleep(750 * time.Millisecond)
		return nil
	}
	timeout := cfg.ReadyTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if int(m.c.wsReady.Load()) >= want {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timeout waiting for runtime.ready: got=%d want=%d", m.c.wsReady.Load(), want)
		case <-ticker.C:
		}
	}
}

func runPullWorker(ctx context.Context, api *apiClient, cfg config, agent agentRef, token string, workerIndex int, tracker *runTracker, m *metrics) {
	ctx = withMetrics(ctx, m)
	claimWait := effectivePullClaimWait(cfg)
	claimPath := fmt.Sprintf("/agent-runtime/runs/claim?wait=%d", int(claimWait.Seconds()))
	for ctx.Err() == nil {
		resp := struct {
			RunID string         `json:"run_id"`
			Input map[string]any `json:"input"`
		}{}
		status, headers, err := api.doWithHeaders(ctx, "runtime-claim", http.MethodGet, claimPath, nil, token, &resp)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if status == http.StatusTooManyRequests {
				m.c.pullEmpty.Add(1)
				sleepContext(ctx, retryAfterDuration(headers, 5*time.Second))
				continue
			}
			if status == http.StatusNoContent {
				m.c.pullEmpty.Add(1)
				sleepContext(ctx, retryAfterDuration(headers, 30*time.Second))
				continue
			}
			m.c.workerErrors.Add(1)
			sleepContext(ctx, time.Second)
			continue
		}
		if status == http.StatusNoContent || resp.RunID == "" {
			m.c.pullEmpty.Add(1)
			sleepContext(ctx, retryAfterDuration(headers, 30*time.Second))
			continue
		}
		m.c.pullClaims.Add(1)
		m.c.assignments.Add(1)
		clientID := stringValue(resp.Input["client_task_id"])
		tracker.markAssigned(resp.RunID, clientID, time.Now())
		sleepContext(ctx, cfg.ResultDelay)
		m.c.resultAttempts.Add(1)
		events := make([]map[string]any, 0, cfg.EventsPerRun)
		for i := 0; i < cfg.EventsPerRun; i++ {
			events = append(events, map[string]any{
				"event_type": "run.message.delta",
				"payload": map[string]any{
					"text":           fmt.Sprintf("pull worker %d progress %d for %s", workerIndex, i+1, clientID),
					"client_task_id": clientID,
				},
			})
		}
		if _, err := api.do(ctx, "runtime-result", http.MethodPost, "/agent-runtime/runs/"+resp.RunID+"/result", map[string]any{
			"status": "success",
			"output": map[string]any{
				"ok":             true,
				"agent_id":       agent.ID,
				"client_task_id": clientID,
				"worker_index":   workerIndex,
			},
			"events": events,
		}, token, nil); err != nil {
			tracker.markCompleted(resp.RunID, time.Now(), err.Error())
			continue
		}
		m.c.resultAccepted.Add(1)
		tracker.markCompleted(resp.RunID, time.Now(), "")
	}
}

func effectivePullClaimWait(cfg config) time.Duration {
	wait := cfg.PullClaimWait
	if wait <= 0 {
		wait = 25 * time.Second
	}
	if cfg.RequestTimeout > time.Second {
		maxWait := cfg.RequestTimeout / 4
		if maxWait < time.Second {
			maxWait = time.Second
		}
		if wait > maxWait {
			wait = maxWait
		}
	}
	if wait < time.Second {
		wait = time.Second
	}
	return wait.Truncate(time.Second)
}

func retryAfterDuration(headers http.Header, fallback time.Duration) time.Duration {
	if headers != nil {
		raw := strings.TrimSpace(headers.Get("Retry-After"))
		if raw != "" {
			if seconds, err := strconv.Atoi(raw); err == nil && seconds > 0 {
				return time.Duration(seconds) * time.Second
			}
			if at, err := http.ParseTime(raw); err == nil {
				if wait := time.Until(at); wait > 0 {
					return wait
				}
			}
		}
	}
	if fallback <= 0 {
		return time.Second
	}
	return fallback
}

func buildReport(cfg config, runID, accountRunID string, accounts []account, agents []agentRef, tracker *runTracker, m *metrics, measuredStart, measuredEnd, holdStart, holdEnd time.Time, waitErr, holdErr error) map[string]any {
	records := tracker.snapshot()
	var measured []*runRecord
	var allCreateDur, assignDur, completeDur, resultAckDur []float64
	failed := 0
	submitted := 0
	assigned := 0
	completed := 0
	for _, r := range records {
		if r.Phase != "measured" {
			continue
		}
		measured = append(measured, r)
		submitted++
		if r.CreateErr != "" || r.ResultErr != "" {
			failed++
		}
		if !r.CreatedAt.IsZero() && !r.SubmittedAt.IsZero() {
			allCreateDur = append(allCreateDur, ms(r.CreatedAt.Sub(r.SubmittedAt)))
		}
		if !r.AssignedAt.IsZero() {
			assigned++
			if !r.SubmittedAt.IsZero() {
				assignDur = append(assignDur, ms(r.AssignedAt.Sub(r.SubmittedAt)))
			}
		}
		if !r.CompletedAt.IsZero() && r.ResultErr == "" {
			completed++
			if !r.SubmittedAt.IsZero() {
				completeDur = append(completeDur, ms(r.CompletedAt.Sub(r.SubmittedAt)))
			}
			if !r.AssignedAt.IsZero() {
				resultAckDur = append(resultAckDur, ms(r.CompletedAt.Sub(r.AssignedAt)))
			}
		}
	}
	totalWindow := measuredEnd.Sub(measuredStart).Seconds()
	if totalWindow <= 0 {
		totalWindow = 1
	}
	holdActual := 0.0
	if !holdStart.IsZero() && !holdEnd.IsZero() {
		holdActual = ms(holdEnd.Sub(holdStart))
	}
	httpByOp := map[string]any{}
	for op, durations := range httpDurationsByOp(m.httpOps) {
		httpByOp[op] = summaryStats(durations)
	}
	statusByOp := map[string]map[string]int{}
	for _, item := range m.httpOps {
		if statusByOp[item.Op] == nil {
			statusByOp[item.Op] = map[string]int{}
		}
		key := strconv.Itoa(item.StatusCode)
		if item.Err != "" && item.StatusCode == 0 {
			key = "error"
		}
		statusByOp[item.Op][key]++
	}
	wsReadyDur, wsReadyTimeline := wsReadyReport(m)
	report := map[string]any{
		"ok":                      waitErr == nil && holdErr == nil && failed == 0 && completed == cfg.Runs,
		"run_id":                  runID,
		"account_run_id":          accountRunID,
		"api_root":                cfg.APIRoot,
		"mode":                    cfg.Mode,
		"users":                   len(accounts),
		"agents":                  len(agents),
		"workers_per_agent":       cfg.WorkersPerAgent,
		"runtime_connections":     len(agents) * cfg.WorkersPerAgent,
		"setup_concurrency":       cfg.SetupConcurrency,
		"setup_user_concurrency":  cfg.SetupUserConcurrency,
		"setup_agent_concurrency": cfg.SetupAgentConcurrency,
		"history_per_agent":       cfg.HistoryPerAgent,
		"events_per_run":          cfg.EventsPerRun,
		"result_delay_ms":         ms(cfg.ResultDelay),
		"submit_duration_ms":      ms(cfg.SubmitDuration),
		"ready_timeout_ms":        ms(cfg.ReadyTimeout),
		"pull_claim_wait_ms":      ms(effectivePullClaimWait(cfg)),
		"ws_heartbeat_ms":         ms(cfg.WSHeartbeat),
		"ws_connect_stagger_ms":   ms(cfg.WSConnectStagger),
		"hold_after_ms":           ms(cfg.HoldAfter),
		"hold_actual_ms":          holdActual,
		"a2a_context":             cfg.ContextMode,
		"measured_runs_target":    cfg.Runs,
		"submitted":               submitted,
		"assigned":                assigned,
		"completed":               completed,
		"failed":                  failed,
		"throughput_rps":          float64(completed) / totalWindow,
		"measured_seconds":        totalWindow,
		"create_run_ms":           summaryStats(allCreateDur),
		"assign_delay_ms":         summaryStats(assignDur),
		"result_ack_ms":           summaryStats(resultAckDur),
		"completion_ms":           summaryStats(completeDur),
		"measured_timeline":       measuredTimeline(measured, measuredStart, measuredEnd),
		"ws_ready_ms":             summaryStats(wsReadyDur),
		"ws_ready_timeline":       wsReadyTimeline,
		"http_ms_by_op":           httpByOp,
		"http_status_by_op":       statusByOp,
		"worker_counts": map[string]int64{
			"ws_connected":       m.c.wsConnected.Load(),
			"ws_ready":           m.c.wsReady.Load(),
			"ws_reconnects":      m.c.wsReconnects.Load(),
			"ws_disconnects":     m.c.wsDisconnects.Load(),
			"ws_pings":           m.c.wsPings.Load(),
			"ws_heartbeats":      m.c.wsHeartbeats.Load(),
			"ws_heartbeat_acks":  m.c.wsHeartbeatAcks.Load(),
			"ws_messages":        m.c.wsMessages.Load(),
			"ws_errors":          m.c.wsErrors.Load(),
			"pull_claims":        m.c.pullClaims.Load(),
			"pull_empty":         m.c.pullEmpty.Load(),
			"assignments":        m.c.assignments.Load(),
			"result_attempts":    m.c.resultAttempts.Load(),
			"result_accepted":    m.c.resultAccepted.Load(),
			"worker_errors":      m.c.workerErrors.Load(),
			"unknown_assignment": m.c.unknownAssignment.Load(),
		},
		"ws_error_samples": m.wsErrorSampleSnapshot(),
		"started_at":       m.startedAt.UTC().Format(time.RFC3339Nano),
		"ended_at":         m.endedAt.UTC().Format(time.RFC3339Nano),
	}
	if waitErr != nil {
		report["wait_error"] = waitErr.Error()
	}
	if holdErr != nil {
		report["hold_error"] = holdErr.Error()
	}
	if !holdStart.IsZero() {
		report["hold_started_at"] = holdStart.UTC().Format(time.RFC3339Nano)
		report["hold_ended_at"] = holdEnd.UTC().Format(time.RFC3339Nano)
	}
	report["sample_failures"] = sampleFailures(measured, 10)
	return report
}

func collectDBReport(ctx context.Context, databaseURL string, agents []agentRef, tracker *runTracker) (map[string]any, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	defer pool.Close()
	agentIDs := make([]string, 0, len(agents))
	for _, agent := range agents {
		agentIDs = append(agentIDs, agent.ID)
	}
	measuredRunIDs := tracker.phaseRunIDs("measured")
	report := map[string]any{}
	var rows pgx.Rows
	if len(measuredRunIDs) > 0 {
		rows, err = pool.Query(ctx, `
SELECT status, COUNT(*)::int, COALESCE(AVG(duration_ms), 0)::float8
FROM runs
WHERE id::text = ANY($1)
GROUP BY status
ORDER BY status`, measuredRunIDs)
	} else {
		rows, err = pool.Query(ctx, `
SELECT status, COUNT(*)::int, COALESCE(AVG(duration_ms), 0)::float8
FROM runs
WHERE agent_id::text = ANY($1)
GROUP BY status
ORDER BY status`, agentIDs)
	}
	if err != nil {
		return nil, err
	}
	statusCounts := []map[string]any{}
	for rows.Next() {
		var status string
		var count int
		var avg float64
		if err := rows.Scan(&status, &count, &avg); err != nil {
			rows.Close()
			return nil, err
		}
		statusCounts = append(statusCounts, map[string]any{"status": status, "count": count, "avg_duration_ms": avg})
	}
	rows.Close()
	report["run_status_counts"] = statusCounts

	rows, err = pool.Query(ctx, `
SELECT schemaname, relname, indexrelname, idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
WHERE relname IN ('runs', 'run_events', 'run_messages', 'a2a_context_mappings', 'agent_tokens', 'agents')
ORDER BY relname, indexrelname`)
	if err == nil {
		indexStats := []map[string]any{}
		for rows.Next() {
			var schema, rel, index string
			var scan, read, fetch int64
			if err := rows.Scan(&schema, &rel, &index, &scan, &read, &fetch); err != nil {
				rows.Close()
				return nil, err
			}
			indexStats = append(indexStats, map[string]any{
				"schema": schema, "table": rel, "index": index,
				"idx_scan": scan, "idx_tup_read": read, "idx_tup_fetch": fetch,
			})
		}
		rows.Close()
		report["index_stats"] = indexStats
	}

	for _, r := range tracker.snapshot() {
		if r.Phase != "measured" || r.RunID == "" || r.RootContext == "" {
			continue
		}
		plan, err := explainConversationQuery(ctx, pool, r.RunID)
		if err == nil {
			report["conversation_query_explain"] = plan
		}
		break
	}
	return report, nil
}

func explainConversationQuery(ctx context.Context, pool *pgxpool.Pool, runID string) ([]string, error) {
	row := pool.QueryRow(ctx, `
SELECT user_id::text, root_context_id, created_at, run_id::text
FROM a2a_context_mappings
WHERE run_id::text = $1`, runID)
	var userID, root, currentRunID string
	var createdAt time.Time
	if err := row.Scan(&userID, &root, &createdAt, &currentRunID); err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, run_id, user_id, agent_id, protocol_context_id, protocol_task_id,
       root_context_id, parent_context_id, parent_task_id, parent_run_id,
       caller_agent_id, target_agent_id, trace_id, reference_task_ids,
       source, created_at, updated_at
FROM (
    SELECT id, run_id, user_id, agent_id, protocol_context_id, protocol_task_id,
           root_context_id, parent_context_id, parent_task_id, parent_run_id,
           caller_agent_id, target_agent_id, trace_id, reference_task_ids,
           source, created_at, updated_at
    FROM a2a_context_mappings
    WHERE user_id::text = $1
      AND root_context_id = $2
      AND (created_at, run_id::text) < ($3, $4)
    ORDER BY created_at DESC, run_id DESC
    LIMIT 50
) history
ORDER BY created_at ASC, run_id ASC`, userID, root, createdAt, currentRunID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, err
		}
		lines = append(lines, line)
	}
	return lines, rows.Err()
}

func writeReport(cfg config, report map[string]any) error {
	path := cfg.Output
	if path == "" {
		path = filepath.Join(defaultReportBaseDir(), ".openlinker-dev", "performance", fmt.Sprintf("runtime-loadtest-%s.json", time.Now().UTC().Format("20060102T150405Z")))
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o644); err != nil {
		return err
	}
	report["report_path"] = path
	return nil
}

func defaultReportBaseDir() string {
	cwd, err := os.Getwd()
	if err != nil {
		return "."
	}
	if filepath.Base(cwd) == "openlinker-core" {
		if _, err := os.Stat(filepath.Join(cwd, "..", ".git")); err == nil {
			return ".."
		}
	}
	return "."
}

func printSummary(report map[string]any) {
	fmt.Printf("runtime-loadtest ok=%v mode=%v users=%v agents=%v workers_per_agent=%v runs=%v completed=%v failed=%v throughput=%.2f rps\n",
		report["ok"], report["mode"], report["users"], report["agents"], report["workers_per_agent"],
		report["measured_runs_target"], report["completed"], report["failed"], number(report["throughput_rps"]))
	fmt.Printf("latency_ms create=%s assign=%s result_ack=%s complete=%s\n",
		shortStats(report["create_run_ms"]), shortStats(report["assign_delay_ms"]), shortStats(report["result_ack_ms"]), shortStats(report["completion_ms"]))
	if path, ok := report["report_path"].(string); ok {
		fmt.Println("report:", path)
	}
	if errText, ok := report["wait_error"].(string); ok {
		fmt.Println("wait_error:", errText)
	}
}

func httpDurationsByOp(items []httpMetric) map[string][]float64 {
	out := map[string][]float64{}
	for _, item := range items {
		out[item.Op] = append(out[item.Op], ms(item.Duration))
	}
	return out
}

func summaryStats(values []float64) map[string]any {
	if len(values) == 0 {
		return map[string]any{"count": 0}
	}
	sort.Float64s(values)
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return map[string]any{
		"count": len(values),
		"min":   round(values[0]),
		"avg":   round(sum / float64(len(values))),
		"p50":   round(percentile(values, 0.50)),
		"p95":   round(percentile(values, 0.95)),
		"p99":   round(percentile(values, 0.99)),
		"max":   round(values[len(values)-1]),
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	pos := p * float64(len(sorted)-1)
	lower := int(math.Floor(pos))
	upper := int(math.Ceil(pos))
	if lower == upper {
		return sorted[lower]
	}
	weight := pos - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

func shortStats(value any) string {
	m, ok := value.(map[string]any)
	if !ok {
		return "{}"
	}
	return fmt.Sprintf("p50=%v p95=%v p99=%v max=%v count=%v", m["p50"], m["p95"], m["p99"], m["max"], m["count"])
}

func sampleFailures(records []*runRecord, limit int) []map[string]any {
	out := []map[string]any{}
	for _, r := range records {
		if r.CreateErr == "" && r.ResultErr == "" {
			continue
		}
		out = append(out, map[string]any{
			"client_id":  r.ClientID,
			"run_id":     r.RunID,
			"agent_id":   r.AgentID,
			"create_err": r.CreateErr,
			"result_err": r.ResultErr,
		})
		if len(out) >= limit {
			break
		}
	}
	return out
}

type timelineBucket struct {
	start        time.Time
	end          time.Time
	submitted    int
	created      int
	assigned     int
	completed    int
	failed       int
	createDur    []float64
	assignDur    []float64
	resultAckDur []float64
	completeDur  []float64
}

func measuredTimeline(records []*runRecord, start, end time.Time) map[string]any {
	if len(records) == 0 {
		return map[string]any{
			"bucket_ms": 0,
			"buckets":   []map[string]any{},
		}
	}
	if start.IsZero() {
		start = records[0].SubmittedAt
		for _, r := range records {
			if !r.SubmittedAt.IsZero() && (start.IsZero() || r.SubmittedAt.Before(start)) {
				start = r.SubmittedAt
			}
		}
	}
	if end.IsZero() || !end.After(start) {
		for _, r := range records {
			for _, ts := range []time.Time{r.CompletedAt, r.AssignedAt, r.CreatedAt, r.SubmittedAt} {
				if !ts.IsZero() && ts.After(end) {
					end = ts
				}
			}
		}
	}
	if start.IsZero() || !end.After(start) {
		return map[string]any{
			"bucket_ms": 0,
			"buckets":   []map[string]any{},
		}
	}
	bucketSize := timelineBucketSize(end.Sub(start))
	bucketCount := int(math.Ceil(float64(end.Sub(start)) / float64(bucketSize)))
	if bucketCount <= 0 {
		bucketCount = 1
	}
	buckets := make([]timelineBucket, bucketCount)
	for i := range buckets {
		buckets[i].start = start.Add(time.Duration(i) * bucketSize)
		buckets[i].end = buckets[i].start.Add(bucketSize)
	}
	addCount := func(ts time.Time, apply func(*timelineBucket)) {
		if ts.IsZero() {
			return
		}
		idx := int(ts.Sub(start) / bucketSize)
		if idx < 0 {
			idx = 0
		}
		if idx >= len(buckets) {
			idx = len(buckets) - 1
		}
		apply(&buckets[idx])
	}
	for _, r := range records {
		addCount(r.SubmittedAt, func(bucket *timelineBucket) {
			bucket.submitted++
		})
		if !r.CreatedAt.IsZero() {
			addCount(r.CreatedAt, func(bucket *timelineBucket) {
				bucket.created++
				if !r.SubmittedAt.IsZero() {
					bucket.createDur = append(bucket.createDur, ms(r.CreatedAt.Sub(r.SubmittedAt)))
				}
			})
		}
		if !r.AssignedAt.IsZero() {
			addCount(r.AssignedAt, func(bucket *timelineBucket) {
				bucket.assigned++
				if !r.SubmittedAt.IsZero() {
					bucket.assignDur = append(bucket.assignDur, ms(r.AssignedAt.Sub(r.SubmittedAt)))
				}
			})
		}
		if !r.CompletedAt.IsZero() {
			addCount(r.CompletedAt, func(bucket *timelineBucket) {
				if r.ResultErr == "" {
					bucket.completed++
					if !r.AssignedAt.IsZero() {
						bucket.resultAckDur = append(bucket.resultAckDur, ms(r.CompletedAt.Sub(r.AssignedAt)))
					}
					if !r.SubmittedAt.IsZero() {
						bucket.completeDur = append(bucket.completeDur, ms(r.CompletedAt.Sub(r.SubmittedAt)))
					}
				}
				if r.ResultErr != "" {
					bucket.failed++
				}
			})
		} else if r.CreateErr != "" || r.ResultErr != "" {
			failedAt := r.CreatedAt
			if failedAt.IsZero() {
				failedAt = r.SubmittedAt
			}
			addCount(failedAt, func(bucket *timelineBucket) {
				bucket.failed++
			})
		}
	}
	out := make([]map[string]any, 0, len(buckets))
	for _, bucket := range buckets {
		out = append(out, map[string]any{
			"start_ms":        round(ms(bucket.start.Sub(start))),
			"end_ms":          round(ms(bucket.end.Sub(start))),
			"submitted":       bucket.submitted,
			"created":         bucket.created,
			"assigned":        bucket.assigned,
			"completed":       bucket.completed,
			"failed":          bucket.failed,
			"complete_rps":    round(float64(bucket.completed) / bucketSize.Seconds()),
			"create_run_ms":   summaryStats(bucket.createDur),
			"assign_delay_ms": summaryStats(bucket.assignDur),
			"result_ack_ms":   summaryStats(bucket.resultAckDur),
			"completion_ms":   summaryStats(bucket.completeDur),
		})
	}
	return map[string]any{
		"bucket_ms": round(ms(bucketSize)),
		"buckets":   out,
	}
}

func timelineBucketSize(duration time.Duration) time.Duration {
	if duration <= 0 {
		return time.Second
	}
	size := time.Second
	for int(math.Ceil(float64(duration)/float64(size))) > 240 {
		size *= 2
	}
	return size
}

func wsReadyReport(m *metrics) ([]float64, map[string]any) {
	start, readyAt := m.wsReadySnapshot()
	if len(readyAt) == 0 || start.IsZero() {
		return nil, map[string]any{
			"bucket_ms": 0,
			"buckets":   []map[string]any{},
		}
	}
	sort.Slice(readyAt, func(i, j int) bool {
		return readyAt[i].Before(readyAt[j])
	})
	durations := make([]float64, 0, len(readyAt))
	end := readyAt[len(readyAt)-1]
	for _, at := range readyAt {
		durations = append(durations, ms(at.Sub(start)))
	}
	bucketSize := timelineBucketSize(end.Sub(start) + time.Nanosecond)
	bucketCount := int(math.Ceil(float64(end.Sub(start)+time.Nanosecond) / float64(bucketSize)))
	if bucketCount <= 0 {
		bucketCount = 1
	}
	counts := make([]int, bucketCount)
	for _, at := range readyAt {
		idx := int(at.Sub(start) / bucketSize)
		if idx < 0 {
			idx = 0
		}
		if idx >= len(counts) {
			idx = len(counts) - 1
		}
		counts[idx]++
	}
	buckets := make([]map[string]any, 0, len(counts))
	cumulative := 0
	for i, count := range counts {
		cumulative += count
		bucketStart := time.Duration(i) * bucketSize
		bucketEnd := bucketStart + bucketSize
		buckets = append(buckets, map[string]any{
			"start_ms":   round(ms(bucketStart)),
			"end_ms":     round(ms(bucketEnd)),
			"ready":      count,
			"cumulative": cumulative,
		})
	}
	return durations, map[string]any{
		"bucket_ms": round(ms(bucketSize)),
		"buckets":   buckets,
	}
}

func websocketURL(apiRoot, path string) (string, error) {
	parsed, err := url.Parse(apiRoot)
	if err != nil {
		return "", err
	}
	switch parsed.Scheme {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	default:
		return "", fmt.Errorf("unsupported api scheme %q", parsed.Scheme)
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + path
	parsed.RawQuery = ""
	return parsed.String(), nil
}

func stringValue(v any) string {
	switch typed := v.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		if typed == nil {
			return ""
		}
		return fmt.Sprint(typed)
	}
}

func mapValue(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func sleepContext(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func waitUntilContext(ctx context.Context, at time.Time) error {
	wait := time.Until(at)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func holdContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func randomSuffix() string {
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(buf)
}

func shortID() string {
	return uuid.NewString()[:8]
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func round(v float64) float64 {
	return math.Round(v*100) / 100
}

func number(v any) float64 {
	switch typed := v.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	default:
		return 0
	}
}

func envDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func intEnv(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

func boolEnv(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return value
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err == nil {
		return value
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}

func fail(err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	fmt.Fprintln(os.Stderr, "runtime-loadtest:", err)
	os.Exit(1)
}
