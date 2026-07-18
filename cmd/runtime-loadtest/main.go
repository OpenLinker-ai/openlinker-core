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

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type config struct {
	APIRoot               string
	AuthAPIRoot           string
	RuntimeURL            string
	RuntimeURLSecondary   string
	DatabaseURL           string
	Transport             string
	Scenarios             []string
	NodeID                string
	NodeVersion           string
	NodeCapacity          int
	MTLSCertFile          string
	MTLSKeyFile           string
	MTLSCAFile            string
	MTLSServerName        string
	StateDir              string
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
	PullWait              time.Duration
	CommandWait           time.Duration
	HeartbeatInterval     time.Duration
	WSProbeInterval       time.Duration
	ConnectStagger        time.Duration
	ConnectionCapacity    bool
	ConnectionStepSize    int
	ConnectionStepHold    time.Duration
	SwitchAfter           time.Duration
	SwitchBackAfter       time.Duration
	CancelCount           int
	CancelConcurrency     int
	CancelDelay           time.Duration
	DropACKResponses      []string
	DuplicateAssignments  int
	StaleFenceProbes      int
	RedisOutageObserve    time.Duration
	HoldAfter             time.Duration
	Output                string
	APIInsecureTLS        bool
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
	ClientID          string
	RunID             string
	AgentID           string
	UserID            string
	RootContext       string
	Phase             string
	SubmittedAt       time.Time
	CreatedAt         time.Time
	AssignedAt        time.Time
	CompletedAt       time.Time
	CancelRequestedAt time.Time
	CancelCommandAt   time.Time
	CancelAckAt       time.Time
	Outcome           string
	CreateErr         string
	ResultErr         string
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

func (t *runTracker) markOutcome(runID, outcome string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if r := t.byRun[runID]; r != nil {
		r.Outcome = outcome
	}
}

func (t *runTracker) markCancelRequested(runID string, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if r := t.byRun[runID]; r != nil && r.CancelRequestedAt.IsZero() {
		r.CancelRequestedAt = at
	}
}

func (t *runTracker) markCancelCommand(runID string, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if r := t.byRun[runID]; r != nil && r.CancelCommandAt.IsZero() {
		r.CancelCommandAt = at
	}
}

func (t *runTracker) markCancelAck(runID string, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if r := t.byRun[runID]; r != nil && r.CancelAckAt.IsZero() {
		r.CancelAckAt = at
	}
}

func (t *runTracker) cancelRequestedAt(runID string) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	if r := t.byRun[runID]; r != nil {
		return r.CancelRequestedAt
	}
	return time.Time{}
}

func (t *runTracker) submittedAt(clientID, runID string) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	if r := t.byRun[runID]; r != nil && !r.SubmittedAt.IsZero() {
		return r.SubmittedAt
	}
	if r := t.byClient[clientID]; r != nil {
		return r.SubmittedAt
	}
	return time.Time{}
}

func (t *runTracker) runSnapshot(runID string) (runRecord, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := t.byRun[runID]
	if r == nil {
		return runRecord{}, false
	}
	return *r, true
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
	workersReady      atomic.Int64
	workersConnected  atomic.Int64
	assignments       atomic.Int64
	workerErrors      atomic.Int64
	unknownAssignment atomic.Int64
}

type metrics struct {
	startedAt time.Time
	endedAt   time.Time

	mu           sync.Mutex
	httpOps      []httpMetric
	errorSamples []string
	c            counters
	runtime      *runtimeMetrics
	capacity     *connectionCapacityReport
	connectedEnd int64
}

type phaseTimestamps struct {
	setupAccountsStart time.Time
	setupAccountsEnd   time.Time
	setupAgentsStart   time.Time
	setupAgentsEnd     time.Time
	workersStart       time.Time
	workersReady       time.Time
	historyStart       time.Time
	historyEnd         time.Time
	measuredStart      time.Time
	measuredEnd        time.Time
	holdStart          time.Time
	holdEnd            time.Time
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

func (m *metrics) recordWorkerError(err error) {
	m.c.workerErrors.Add(1)
	if err == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.errorSamples) >= 10 {
		return
	}
	m.errorSamples = append(m.errorSamples, err.Error())
}

func (m *metrics) errorSampleSnapshot() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.errorSamples...)
}

func main() {
	cfg := parseFlags()
	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, "runtime-loadtest:", err)
		os.Exit(2)
	}
	if err := preflightRuntimeCredentials(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "runtime-loadtest:", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	m := &metrics{startedAt: time.Now(), runtime: newRuntimeMetrics()}
	ctx = withMetrics(ctx, m)
	tracker := newRunTracker()
	coreAPI := newAPIClient(cfg.APIRoot, cfg)
	authAPI := newAPIClient(cfg.AuthAPIRoot, cfg)
	runID := randomSuffix()
	accountRunID := cfg.AccountRunID
	if accountRunID == "" {
		accountRunID = runID
	}

	phases := phaseTimestamps{}

	phases.setupAccountsStart = time.Now()
	accounts, err := setupAccounts(ctx, authAPI, coreAPI, cfg, accountRunID, m)
	phases.setupAccountsEnd = time.Now()
	if err != nil {
		fail(err)
	}
	phases.setupAgentsStart = time.Now()
	agents, err := setupAgents(ctx, coreAPI, cfg, runID, accounts, m)
	phases.setupAgentsEnd = time.Now()
	if err != nil {
		fail(err)
	}

	var dbSampler *dbActivitySampler
	if cfg.DatabaseURL != "" {
		dbSampler = startDBActivitySampler(ctx, cfg.DatabaseURL, time.Second)
	}

	workerCtx, stopWorkers := context.WithCancel(ctx)
	var workerWG sync.WaitGroup
	phases.workersStart = time.Now()
	specs := makeRuntimeWorkerSpecs(agents)
	var capacityErr error
	if cfg.ConnectionCapacity {
		m.capacity, capacityErr = runConnectionCapacityStages(
			workerCtx, cfg, coreAPI, specs, tracker, m, &workerWG,
		)
	} else {
		for workerOrdinal, spec := range specs {
			connectDelay := time.Duration(workerOrdinal) * cfg.ConnectStagger
			worker, workerErr := newRuntimeWorker(cfg, spec.agent, spec.token, spec.workerIndex, tracker, m)
			if workerErr != nil {
				stopWorkers()
				workerWG.Wait()
				fail(workerErr)
			}
			workerWG.Add(1)
			go func(worker *runtimeWorker, connectDelay time.Duration) {
				defer workerWG.Done()
				if connectDelay > 0 {
					sleepContext(workerCtx, connectDelay)
					if workerCtx.Err() != nil {
						return
					}
				}
				worker.run(workerCtx)
			}(worker, connectDelay)
		}
		if err := waitForWorkersReady(ctx, cfg, m, len(specs)); err != nil {
			stopWorkers()
			workerWG.Wait()
			fail(err)
		}
	}
	if capacityErr == nil {
		phases.workersReady = time.Now()
	}
	if capacityErr == nil && cfg.hasScenario("redis-signal-outage") {
		if err := waitForRedisSignalOutage(ctx, coreAPI, cfg.RedisOutageObserve, m.runtime); err != nil {
			stopWorkers()
			workerWG.Wait()
			fail(err)
		}
	}
	if capacityErr == nil && cfg.HistoryPerAgent > 0 {
		phases.historyStart = time.Now()
		if err := submitRuns(ctx, coreAPI, cfg, agents, tracker, m, "history", cfg.HistoryPerAgent*len(agents), 1); err != nil {
			stopWorkers()
			workerWG.Wait()
			fail(err)
		}
		if err := waitForPhase(ctx, tracker, "history", cfg.HistoryPerAgent*len(agents), cfg.Timeout); err != nil {
			stopWorkers()
			workerWG.Wait()
			fail(err)
		}
		phases.historyEnd = time.Now()
	}

	measuredStart := time.Now()
	phases.measuredStart = measuredStart
	var waitErr error
	if capacityErr != nil {
		waitErr = capacityErr
	} else if err := submitRuns(ctx, coreAPI, cfg, agents, tracker, m, "measured", cfg.Runs, cfg.RunConcurrency); err != nil {
		stopWorkers()
		workerWG.Wait()
		fail(err)
	}
	var cancelErr error
	if waitErr == nil && cfg.CancelCount > 0 {
		cancelErr = driveRuntimeCancellations(ctx, coreAPI, cfg, accounts, tracker, m)
	}
	if waitErr == nil {
		waitErr = waitForMeasuredPhase(ctx, cfg, tracker, cfg.Runs, cfg.Timeout)
	}
	if waitErr == nil && cancelErr != nil {
		waitErr = cancelErr
	}
	measuredEnd := time.Now()
	phases.measuredEnd = measuredEnd

	var holdStart, holdEnd time.Time
	var holdErr error
	if waitErr == nil && cfg.HoldAfter > 0 {
		holdStart = time.Now()
		phases.holdStart = holdStart
		if m.capacity != nil {
			holdErr = confirmConnectionCapacity(ctx, cfg.HoldAfter, coreAPI, m, m.capacity)
		} else {
			holdErr = holdContext(ctx, cfg.HoldAfter)
		}
		holdEnd = time.Now()
		phases.holdEnd = holdEnd
	}
	if m.capacity != nil {
		m.capacity.FinalConnected = m.c.workersConnected.Load()
	}
	m.connectedEnd = m.c.workersConnected.Load()
	stopWorkers()
	workerWG.Wait()
	m.endedAt = time.Now()

	report := buildReport(cfg, runID, accountRunID, accounts, agents, tracker, m, phases, waitErr, holdErr)
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
	var scenarios string
	var dropACKResponses string
	flag.StringVar(&cfg.APIRoot, "api", envDefault("OPENLINKER_API_ROOT", "http://127.0.0.1:8080/api/v1"), "OpenLinker Core user/API root")
	flag.StringVar(&cfg.AuthAPIRoot, "auth-api", os.Getenv("OPENLINKER_AUTH_API_ROOT"), "account auth API root; defaults to -api")
	flag.StringVar(&cfg.RuntimeURL, "runtime-url", os.Getenv("OPENLINKER_RUNTIME_URL"), "required dedicated Runtime mTLS origin (https)")
	flag.StringVar(&cfg.RuntimeURLSecondary, "runtime-url-secondary", os.Getenv("OPENLINKER_RUNTIME_URL_SECONDARY"), "second Runtime mTLS origin for Core A→B resume")
	flag.StringVar(&cfg.DatabaseURL, "database-url", os.Getenv("DATABASE_URL"), "optional Postgres URL for DB-side counts and query-plan evidence")
	flag.StringVar(&cfg.Transport, "transport", envDefault("OPENLINKER_RUNTIME_LOADTEST_TRANSPORT", transportAuto), "Runtime transport: ws, pull, or auto")
	flag.StringVar(&scenarios, "scenarios", envDefault("OPENLINKER_RUNTIME_LOADTEST_SCENARIOS", "baseline"), "comma-separated Runtime scenarios; see cmd/runtime-loadtest/README.md")
	flag.StringVar(&cfg.NodeID, "node-id", os.Getenv("OPENLINKER_NODE_ID"), "required enrolled Runtime Node UUID from the client certificate")
	flag.StringVar(&cfg.NodeVersion, "node-version", envDefault("OPENLINKER_RUNTIME_LOADTEST_NODE_VERSION", runtimeLoadtestNodeVersion), "exact version registered for the Runtime Node")
	flag.IntVar(&cfg.NodeCapacity, "node-capacity", intEnv("OPENLINKER_RUNTIME_LOADTEST_NODE_CAPACITY", 0), "Node capacity; defaults to agents × workers-per-agent")
	flag.StringVar(&cfg.MTLSCertFile, "runtime-mtls-cert", os.Getenv("OPENLINKER_RUNTIME_MTLS_CERT_FILE"), "required Runtime Node client certificate PEM")
	flag.StringVar(&cfg.MTLSKeyFile, "runtime-mtls-key", os.Getenv("OPENLINKER_RUNTIME_MTLS_KEY_FILE"), "required Runtime Node private key PEM")
	flag.StringVar(&cfg.MTLSCAFile, "runtime-mtls-ca", os.Getenv("OPENLINKER_RUNTIME_MTLS_CA_FILE"), "required Runtime server trust CA PEM")
	flag.StringVar(&cfg.MTLSServerName, "runtime-mtls-server-name", os.Getenv("OPENLINKER_RUNTIME_MTLS_SERVER_NAME"), "optional Runtime TLS server-name override")
	flag.StringVar(&cfg.StateDir, "state-dir", envDefault("OPENLINKER_RUNTIME_LOADTEST_STATE_DIR", filepath.Join(defaultReportBaseDir(), ".openlinker-dev", "performance", "runtime-loadtest-state")), "durable Runtime Attempt/Event/Result journal directory")
	flag.IntVar(&cfg.Users, "users", intEnv("OPENLINKER_RUNTIME_LOADTEST_USERS", 1), "number of creator/caller users")
	flag.IntVar(&cfg.Agents, "agents", intEnv("OPENLINKER_RUNTIME_LOADTEST_AGENTS", 10), "number of runtime agents")
	flag.IntVar(&cfg.WorkersPerAgent, "workers-per-agent", intEnv("OPENLINKER_RUNTIME_LOADTEST_WORKERS_PER_AGENT", 1), "Runtime Agent Token workers per Agent")
	flag.IntVar(&cfg.Runs, "runs", intEnv("OPENLINKER_RUNTIME_LOADTEST_RUNS", 100), "measured runs to submit")
	flag.IntVar(&cfg.RunConcurrency, "run-concurrency", intEnv("OPENLINKER_RUNTIME_LOADTEST_RUN_CONCURRENCY", 20), "concurrent run submissions")
	flag.IntVar(&cfg.SetupConcurrency, "setup-concurrency", intEnv("OPENLINKER_RUNTIME_LOADTEST_SETUP_CONCURRENCY", 16), "concurrent disposable user/agent setup operations")
	flag.IntVar(&cfg.SetupUserConcurrency, "setup-user-concurrency", intEnv("OPENLINKER_RUNTIME_LOADTEST_SETUP_USER_CONCURRENCY", 0), "concurrent disposable user setup operations; default min(setup-concurrency, 8)")
	flag.IntVar(&cfg.SetupAgentConcurrency, "setup-agent-concurrency", intEnv("OPENLINKER_RUNTIME_LOADTEST_SETUP_AGENT_CONCURRENCY", 0), "concurrent disposable agent setup operations; default setup-concurrency")
	flag.IntVar(&cfg.EventsPerRun, "events-per-run", intEnv("OPENLINKER_RUNTIME_LOADTEST_EVENTS_PER_RUN", 1), "durable Runtime Events per Run")
	flag.DurationVar(&cfg.ResultDelay, "result-delay", durationEnv("OPENLINKER_RUNTIME_LOADTEST_RESULT_DELAY", 0), "artificial execution delay before Result")
	flag.DurationVar(&cfg.SubmitDuration, "submit-duration", durationEnv("OPENLINKER_RUNTIME_LOADTEST_SUBMIT_DURATION", 0), "spread measured run submissions across this duration")
	flag.IntVar(&cfg.HistoryPerAgent, "history-per-agent", intEnv("OPENLINKER_RUNTIME_LOADTEST_HISTORY_PER_AGENT", 0), "completed prior A2A-context runs per Agent")
	flag.BoolVar(&cfg.ContextMode, "a2a-context", boolEnv("OPENLINKER_RUNTIME_LOADTEST_A2A_CONTEXT", true), "include A2A context IDs")
	flag.StringVar(&cfg.AccountRunID, "account-run-id", os.Getenv("OPENLINKER_RUNTIME_LOADTEST_ACCOUNT_RUN_ID"), "reuse accounts from a previous run ID")
	flag.DurationVar(&cfg.Timeout, "timeout", durationEnv("OPENLINKER_RUNTIME_LOADTEST_TIMEOUT", 2*time.Minute), "overall timeout")
	flag.DurationVar(&cfg.ReadyTimeout, "ready-timeout", durationEnv("OPENLINKER_RUNTIME_LOADTEST_READY_TIMEOUT", 30*time.Second), "timeout for authenticated Runtime hello/ready")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", durationEnv("OPENLINKER_RUNTIME_LOADTEST_REQUEST_TIMEOUT", 20*time.Second), "per user/API request timeout")
	flag.DurationVar(&cfg.PullWait, "pull-wait", durationEnv("OPENLINKER_RUNTIME_LOADTEST_PULL_WAIT", 5*time.Second), "Runtime assignment long-poll wait (1s-30s)")
	flag.DurationVar(&cfg.CommandWait, "command-wait", durationEnv("OPENLINKER_RUNTIME_LOADTEST_COMMAND_WAIT", time.Second), "Runtime command long-poll wait (1s-30s)")
	flag.DurationVar(&cfg.HeartbeatInterval, "heartbeat-interval", durationEnv("OPENLINKER_RUNTIME_LOADTEST_HEARTBEAT_INTERVAL", 15*time.Second), "Pull session heartbeat / WS liveness interval")
	flag.DurationVar(&cfg.WSProbeInterval, "ws-probe-interval", durationEnv("OPENLINKER_RUNTIME_LOADTEST_WS_PROBE_INTERVAL", 10*time.Second), "auto-mode interval before probing WebSocket recovery")
	flag.DurationVar(&cfg.ConnectStagger, "connect-stagger", durationEnv("OPENLINKER_RUNTIME_LOADTEST_CONNECT_STAGGER", 0), "delay increment between Runtime worker connections")
	flag.BoolVar(&cfg.ConnectionCapacity, "connection-capacity", boolEnv("OPENLINKER_RUNTIME_LOADTEST_CONNECTION_CAPACITY", false), "slow stair-step authenticated WebSocket capacity probe")
	flag.IntVar(&cfg.ConnectionStepSize, "connection-step-size", intEnv("OPENLINKER_RUNTIME_LOADTEST_CONNECTION_STEP_SIZE", 0), "workers added per capacity stage; default 25")
	flag.DurationVar(&cfg.ConnectionStepHold, "connection-step-hold", durationEnv("OPENLINKER_RUNTIME_LOADTEST_CONNECTION_STEP_HOLD", 0), "observation hold per capacity stage; default 30s")
	flag.DurationVar(&cfg.SwitchAfter, "switch-after", durationEnv("OPENLINKER_RUNTIME_LOADTEST_SWITCH_AFTER", 5*time.Second), "planned first transport/Core switch after worker start")
	flag.DurationVar(&cfg.SwitchBackAfter, "switch-back-after", durationEnv("OPENLINKER_RUNTIME_LOADTEST_SWITCH_BACK_AFTER", 10*time.Second), "planned Pull→WebSocket switch after worker start")
	flag.IntVar(&cfg.CancelCount, "cancel-count", intEnv("OPENLINKER_RUNTIME_LOADTEST_CANCEL_COUNT", 0), "number of measured Runs to cancel; cancel-race defaults to 1000")
	flag.IntVar(&cfg.CancelConcurrency, "cancel-concurrency", intEnv("OPENLINKER_RUNTIME_LOADTEST_CANCEL_CONCURRENCY", 200), "concurrent owner cancellation requests")
	flag.DurationVar(&cfg.CancelDelay, "cancel-delay", durationEnv("OPENLINKER_RUNTIME_LOADTEST_CANCEL_DELAY", 0), "delay from assignment to owner cancellation")
	flag.StringVar(&dropACKResponses, "drop-ack-responses", os.Getenv("OPENLINKER_RUNTIME_LOADTEST_DROP_ACK_RESPONSES"), "comma-separated Pull ACK responses to lose once: assignment,event,result,cancel")
	flag.IntVar(&cfg.DuplicateAssignments, "duplicate-assignments", intEnv("OPENLINKER_RUNTIME_LOADTEST_DUPLICATE_ASSIGNMENTS", 0), "client duplicate-delivery injections per offer")
	flag.IntVar(&cfg.StaleFenceProbes, "stale-fence-probes", intEnv("OPENLINKER_RUNTIME_LOADTEST_STALE_FENCE_PROBES", 0), "bad fencing-token renew probes per Attempt")
	flag.DurationVar(&cfg.RedisOutageObserve, "redis-outage-observe", durationEnv("OPENLINKER_RUNTIME_LOADTEST_REDIS_OUTAGE_OBSERVE", 0), "timeout waiting for /readyz to prove the operator-controlled Redis signal outage")
	flag.DurationVar(&cfg.HoldAfter, "hold-after-completion", durationEnv("OPENLINKER_RUNTIME_LOADTEST_HOLD_AFTER_COMPLETION", 0), "keep Runtime workers connected after completion")
	flag.StringVar(&cfg.Output, "output", os.Getenv("OPENLINKER_RUNTIME_LOADTEST_OUTPUT"), "JSON report path")
	flag.BoolVar(&cfg.APIInsecureTLS, "api-insecure-tls", boolEnv("OPENLINKER_RUNTIME_LOADTEST_API_INSECURE_TLS", false), "skip TLS verification only for disposable user/API setup")
	flag.BoolVar(&cfg.FailOnIncomplete, "fail-on-incomplete", boolEnv("OPENLINKER_RUNTIME_LOADTEST_FAIL_ON_INCOMPLETE", true), "exit non-zero when measured Runs do not complete")
	flag.Parse()
	cfg.Scenarios = parseCSV(scenarios)
	cfg.DropACKResponses = parseCSV(dropACKResponses)
	return cfg
}

func (c *config) validate() error {
	c.Transport = strings.ToLower(strings.TrimSpace(c.Transport))
	if c.Transport != transportAuto && c.Transport != transportWS && c.Transport != transportPull {
		return fmt.Errorf("unsupported transport %q", c.Transport)
	}
	if c.Users <= 0 || c.Agents <= 0 || c.WorkersPerAgent <= 0 || c.Runs <= 0 {
		return errors.New("users, agents, workers-per-agent, and runs must be positive")
	}
	if c.WorkersPerAgent > 10 {
		return errors.New("workers-per-agent cannot exceed the 10 Agent Token limit")
	}
	if c.NodeCapacity == 0 {
		c.NodeCapacity = c.Agents * c.WorkersPerAgent
	}
	if c.NodeCapacity < 1 || c.NodeCapacity > openlinker.RuntimeMaxNodeCapacity {
		return fmt.Errorf("node-capacity must be between 1 and %d", openlinker.RuntimeMaxNodeCapacity)
	}
	if c.RunConcurrency <= 0 || c.SetupConcurrency <= 0 {
		return errors.New("run/setup concurrency must be positive")
	}
	if c.SetupUserConcurrency < 0 || c.SetupAgentConcurrency < 0 {
		return errors.New("setup user/agent concurrency cannot be negative")
	}
	if c.SetupUserConcurrency == 0 {
		c.SetupUserConcurrency = min(c.SetupConcurrency, 8)
	}
	if c.SetupAgentConcurrency == 0 {
		c.SetupAgentConcurrency = c.SetupConcurrency
	}
	if c.Timeout <= 0 || c.RequestTimeout <= 0 || c.ReadyTimeout <= 0 {
		return errors.New("timeouts must be positive")
	}
	if c.HoldAfter < 0 || c.SubmitDuration < 0 || c.ConnectStagger < 0 {
		return errors.New("hold/submit/connect durations cannot be negative")
	}
	if c.ConnectionStepSize < 0 || c.ConnectionStepHold < 0 {
		return errors.New("connection capacity step size/hold cannot be negative")
	}
	if c.ConnectionCapacity {
		if c.Transport != transportWS {
			return errors.New("connection-capacity requires transport=ws")
		}
		if c.ConnectionStepSize == 0 {
			c.ConnectionStepSize = 25
		}
		if c.ConnectionStepHold == 0 {
			c.ConnectionStepHold = 30 * time.Second
		}
		if c.ConnectStagger == 0 {
			c.ConnectStagger = 500 * time.Millisecond
		}
		if c.HoldAfter == 0 {
			c.HoldAfter = 5 * time.Minute
		}
		minimumTimeout := minimumConnectionCapacityTimeout(*c)
		if c.Timeout < minimumTimeout {
			return fmt.Errorf(
				"connection-capacity timeout must be at least %s for the configured ramp and holds",
				minimumTimeout.Round(time.Second),
			)
		}
	}
	if c.PullWait < time.Second || c.PullWait > time.Duration(openlinker.RuntimeMaxPullWaitSeconds)*time.Second {
		return fmt.Errorf("pull-wait must be between 1s and %ds", openlinker.RuntimeMaxPullWaitSeconds)
	}
	if c.CommandWait < time.Second || c.CommandWait > time.Duration(openlinker.RuntimeMaxPullWaitSeconds)*time.Second {
		return fmt.Errorf("command-wait must be between 1s and %ds", openlinker.RuntimeMaxPullWaitSeconds)
	}
	if c.HeartbeatInterval <= 0 || c.WSProbeInterval <= 0 {
		return errors.New("heartbeat and WebSocket probe intervals must be positive")
	}
	if strings.TrimSpace(c.AuthAPIRoot) == "" {
		c.AuthAPIRoot = c.APIRoot
	}
	if _, err := url.ParseRequestURI(c.APIRoot); err != nil {
		return fmt.Errorf("invalid api root: %w", err)
	}
	if _, err := url.ParseRequestURI(c.AuthAPIRoot); err != nil {
		return fmt.Errorf("invalid auth api root: %w", err)
	}
	if err := validateRuntimeOrigin(c.RuntimeURL, "runtime-url"); err != nil {
		return err
	}
	if c.RuntimeURLSecondary != "" {
		if err := validateRuntimeOrigin(c.RuntimeURLSecondary, "runtime-url-secondary"); err != nil {
			return err
		}
	}
	if _, err := uuid.Parse(c.NodeID); err != nil {
		return errors.New("node-id must be the enrolled Runtime Node UUID")
	}
	if strings.TrimSpace(c.NodeVersion) == "" {
		return errors.New("node-version is required")
	}
	if strings.TrimSpace(c.StateDir) == "" {
		return errors.New("state-dir is required for the persistent_spool contract feature")
	}
	for _, required := range []struct{ name, value string }{
		{"runtime-mtls-cert", c.MTLSCertFile}, {"runtime-mtls-key", c.MTLSKeyFile}, {"runtime-mtls-ca", c.MTLSCAFile},
	} {
		if strings.TrimSpace(required.value) == "" {
			return fmt.Errorf("%s is required; Runtime never falls back to token-only transport", required.name)
		}
		if info, err := os.Stat(required.value); err != nil || info.IsDir() {
			return fmt.Errorf("%s is not a readable file", required.name)
		}
	}
	if err := c.applyScenarioDefaults(); err != nil {
		return err
	}
	if c.CancelCount < 0 || c.CancelCount > c.Runs || c.CancelConcurrency <= 0 {
		return errors.New("cancel-count must be within measured Runs and cancel-concurrency must be positive")
	}
	if c.DuplicateAssignments < 0 || c.StaleFenceProbes < 0 {
		return errors.New("duplicate-assignment and stale-fence counts cannot be negative")
	}
	for _, op := range c.DropACKResponses {
		switch op {
		case "assignment", "event", "result", "cancel":
		default:
			return fmt.Errorf("unsupported dropped ACK response %q", op)
		}
	}
	if len(c.DropACKResponses) > 0 && c.Transport != transportPull {
		return errors.New("drop-ack-responses requires transport=pull; the strict SDK WebSocket dialer requires its direct TLS transport")
	}
	return nil
}

func newAPIClient(root string, cfg config) *apiClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.APIInsecureTLS {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // explicit load-test flag for disposable test hosts.
	}
	return &apiClient{
		root: strings.TrimRight(root, "/"),
		client: &http.Client{
			Timeout:   cfg.RequestTimeout,
			Transport: transport,
		},
	}
}

func setupAccounts(
	ctx context.Context,
	authAPI *apiClient,
	coreAPI *apiClient,
	cfg config,
	runID string,
	m *metrics,
) ([]account, error) {
	accounts := make([]account, cfg.Users)
	err := runSetupJobs(ctx, cfg.SetupUserConcurrency, cfg.Users, func(ctx context.Context, i int) error {
		email := fmt.Sprintf("openlinker-perf-%s-u%03d@example.local", runID, i)
		password := "password123"
		display := fmt.Sprintf("OpenLinker Perf %s User %03d", runID, i)
		acc, err := ensureAccount(ctx, authAPI, email, password, display, m)
		if err != nil {
			return err
		}
		if _, err := coreAPI.do(ctx, "become-creator", http.MethodPost, "/me/become-creator", map[string]any{}, acc.JWT, nil); err != nil {
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
			"connection_mode": "runtime",
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
			if _, err := api.do(ctx, "create-agent-token", http.MethodPost, "/creator/agent-tokens", map[string]any{
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
	status, _, err := api.doRequest(ctx, op, method, path, body, token, nil, out)
	return status, err
}

func (api *apiClient) doWithHeaders(ctx context.Context, op, method, path string, body any, token string, out any) (int, http.Header, error) {
	return api.doRequest(ctx, op, method, path, body, token, nil, out)
}

func (api *apiClient) doWithRequestHeaders(
	ctx context.Context,
	op string,
	method string,
	path string,
	body any,
	token string,
	requestHeaders http.Header,
	out any,
) (int, error) {
	status, _, err := api.doRequest(ctx, op, method, path, body, token, requestHeaders, out)
	return status, err
}

func (api *apiClient) doRequest(
	ctx context.Context,
	op string,
	method string,
	path string,
	body any,
	token string,
	requestHeaders http.Header,
	out any,
) (int, http.Header, error) {
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
	for key, values := range requestHeaders {
		for _, value := range values {
			req.Header.Add(key, value)
		}
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

func isRunCreationStatus(status int) bool {
	switch status {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted:
		return true
	default:
		return false
	}
}

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
				status, err := api.doWithRequestHeaders(
					ctx,
					"create-run",
					http.MethodPost,
					"/runs",
					body,
					agent.Creator.JWT,
					http.Header{"Idempotency-Key": []string{clientID}},
					&resp,
				)
				if err != nil {
					tracker.markCreated(clientID, "", time.Now(), err.Error())
					recordErr(err)
					continue
				}
				if !isRunCreationStatus(status) || resp.RunID == "" {
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
				terminal, statuses, err := countRunsByStatus(ctx, pool, ids)
				if err == nil && terminal >= want {
					return fmt.Errorf("client terminal ACK incomplete after DB terminal state: client_completed=%d db_terminal=%d want=%d statuses=%s",
						tracker.phaseCompleted("measured"), terminal, want, formatStatusCounts(statuses))
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
	return statuses["success"] + statuses["failed"] + statuses["timeout"] + statuses["canceled"], statuses, rows.Err()
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

func buildReport(cfg config, runID, accountRunID string, accounts []account, agents []agentRef, tracker *runTracker, m *metrics, phases phaseTimestamps, waitErr, holdErr error) map[string]any {
	records := tracker.snapshot()
	var measured []*runRecord
	var allCreateDur, assignDur, completeDur, resultAckDur []float64
	failed := 0
	succeeded := 0
	canceled := 0
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
		switch r.Outcome {
		case "success":
			succeeded++
		case "canceled":
			canceled++
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
	totalWindow := phases.measuredEnd.Sub(phases.measuredStart).Seconds()
	if totalWindow <= 0 {
		totalWindow = 1
	}
	holdActual := 0.0
	if !phases.holdStart.IsZero() && !phases.holdEnd.IsZero() {
		holdActual = ms(phases.holdEnd.Sub(phases.holdStart))
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
	runtimeReport := m.runtime.report(cfg)
	safety := runtimeReport["safety"].(map[string]any)
	safetyOK := safety["duplicate_execution"].(int) == 0 && safety["stale_fence_accepts"].(int) == 0 && runtimeReport["all_assertions_passed"].(bool)
	runtimeOrigins := []string{cfg.RuntimeURL}
	if cfg.RuntimeURLSecondary != "" {
		runtimeOrigins = append(runtimeOrigins, cfg.RuntimeURLSecondary)
	}
	report := map[string]any{
		"ok":                      waitErr == nil && holdErr == nil && failed == 0 && completed == cfg.Runs && safetyOK,
		"run_id":                  runID,
		"account_run_id":          accountRunID,
		"api_root":                cfg.APIRoot,
		"auth_api_root":           cfg.AuthAPIRoot,
		"runtime_origins":         runtimeOrigins,
		"transport":               cfg.Transport,
		"scenarios":               cfg.Scenarios,
		"node_id":                 cfg.NodeID,
		"node_version":            cfg.NodeVersion,
		"node_capacity":           cfg.NodeCapacity,
		"state_dir":               cfg.StateDir,
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
		"pull_wait_ms":            ms(cfg.PullWait),
		"command_wait_ms":         ms(cfg.CommandWait),
		"heartbeat_interval_ms":   ms(cfg.HeartbeatInterval),
		"ws_probe_interval_ms":    ms(cfg.WSProbeInterval),
		"connect_stagger_ms":      ms(cfg.ConnectStagger),
		"connection_capacity":     cfg.ConnectionCapacity,
		"connection_step_size":    cfg.ConnectionStepSize,
		"connection_step_hold_ms": ms(cfg.ConnectionStepHold),
		"hold_after_ms":           ms(cfg.HoldAfter),
		"hold_actual_ms":          holdActual,
		"a2a_context":             cfg.ContextMode,
		"measured_runs_target":    cfg.Runs,
		"submitted":               submitted,
		"assigned":                assigned,
		"completed":               completed,
		"succeeded":               succeeded,
		"canceled":                canceled,
		"failed":                  failed,
		"throughput_rps":          float64(completed) / totalWindow,
		"measured_seconds":        totalWindow,
		"create_run_ms":           summaryStats(allCreateDur),
		"assign_delay_ms":         summaryStats(assignDur),
		"result_ack_ms":           summaryStats(resultAckDur),
		"completion_ms":           summaryStats(completeDur),
		"measured_timeline":       measuredTimeline(measured, phases.measuredStart, phases.measuredEnd),
		"runtime":                 runtimeReport,
		"http_ms_by_op":           httpByOp,
		"http_status_by_op":       statusByOp,
		"worker_counts": map[string]int64{
			"ready":              m.c.workersReady.Load(),
			"connected_at_end":   m.connectedEnd,
			"assignments":        m.c.assignments.Load(),
			"worker_errors":      m.c.workerErrors.Load(),
			"unknown_assignment": m.c.unknownAssignment.Load(),
		},
		"worker_error_samples": m.errorSampleSnapshot(),
		"started_at":           m.startedAt.UTC().Format(time.RFC3339Nano),
		"ended_at":             m.endedAt.UTC().Format(time.RFC3339Nano),
	}
	if waitErr != nil {
		report["wait_error"] = waitErr.Error()
	}
	if holdErr != nil {
		report["hold_error"] = holdErr.Error()
	}
	if m.capacity != nil {
		report["connection_capacity_report"] = m.capacity
	}
	report["phase_timestamps"] = phaseTimestampReport(phases)
	report["phase_durations_ms"] = phaseDurationReport(phases)
	if !phases.holdStart.IsZero() {
		report["hold_started_at"] = phases.holdStart.UTC().Format(time.RFC3339Nano)
		report["hold_ended_at"] = phases.holdEnd.UTC().Format(time.RFC3339Nano)
	}
	report["sample_failures"] = sampleFailures(measured, 10)
	return report
}

func phaseTimestampReport(phases phaseTimestamps) map[string]any {
	out := map[string]any{}
	addTime := func(key string, value time.Time) {
		if !value.IsZero() {
			out[key] = value.UTC().Format(time.RFC3339Nano)
		}
	}
	addTime("setup_accounts_started_at", phases.setupAccountsStart)
	addTime("setup_accounts_ended_at", phases.setupAccountsEnd)
	addTime("setup_agents_started_at", phases.setupAgentsStart)
	addTime("setup_agents_ended_at", phases.setupAgentsEnd)
	addTime("workers_started_at", phases.workersStart)
	addTime("workers_ready_at", phases.workersReady)
	addTime("history_started_at", phases.historyStart)
	addTime("history_ended_at", phases.historyEnd)
	addTime("measured_started_at", phases.measuredStart)
	addTime("measured_ended_at", phases.measuredEnd)
	addTime("hold_started_at", phases.holdStart)
	addTime("hold_ended_at", phases.holdEnd)
	return out
}

func phaseDurationReport(phases phaseTimestamps) map[string]any {
	out := map[string]any{}
	addDuration := func(key string, start, end time.Time) {
		if !start.IsZero() && !end.IsZero() && !end.Before(start) {
			out[key] = ms(end.Sub(start))
		}
	}
	addDuration("setup_accounts_ms", phases.setupAccountsStart, phases.setupAccountsEnd)
	addDuration("setup_agents_ms", phases.setupAgentsStart, phases.setupAgentsEnd)
	addDuration("workers_ready_ms", phases.workersStart, phases.workersReady)
	addDuration("history_ms", phases.historyStart, phases.historyEnd)
	addDuration("measured_ms", phases.measuredStart, phases.measuredEnd)
	addDuration("hold_ms", phases.holdStart, phases.holdEnd)
	addDuration("setup_total_ms", phases.setupAccountsStart, phases.setupAgentsEnd)
	addDuration("pre_measured_ms", phases.setupAccountsStart, phases.measuredStart)
	return out
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
	fmt.Printf("runtime-loadtest ok=%v transport=%v users=%v agents=%v workers_per_agent=%v runs=%v completed=%v failed=%v throughput=%.2f rps\n",
		report["ok"], report["transport"], report["users"], report["agents"], report["workers_per_agent"],
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
