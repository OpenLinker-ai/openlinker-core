package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/google/uuid"
)

const (
	runtimeLoadtestNodeVersion = "openlinker-go/runtime-worker"
	transportAuto              = "auto"
	transportWS                = "ws"
	transportPull              = "pull"
)

var errACKResponseLost = errors.New("runtime-loadtest: injected ACK response loss after Core persisted the request")

type runtimeClient interface {
	CreateRuntimeSession(context.Context, openlinker.RuntimeHelloPayload) (*openlinker.RuntimeReadyPayload, error)
	HeartbeatRuntimeSession(context.Context, openlinker.RuntimeHelloPayload) (*openlinker.RuntimeReadyPayload, error)
	CloseRuntimeSession(context.Context, openlinker.RuntimeSessionCloseRequest) error
	ClaimRuntimeRun(context.Context, int, openlinker.RuntimeClaimRequest) (*openlinker.RuntimeRunAssignedPayload, error)
	AckRuntimeAssignment(context.Context, openlinker.RuntimeAssignmentAckPayload) (*openlinker.RuntimeAssignmentConfirmedPayload, error)
	RenewRuntimeLease(context.Context, openlinker.RuntimeLeaseRenewPayload) (*openlinker.RuntimeLeaseRenewedPayload, error)
	AppendRuntimeEvent(context.Context, openlinker.RuntimeRunEventPayload) (*openlinker.RuntimeRunEventAckPayload, error)
	FinalizeRuntimeResult(context.Context, openlinker.RuntimeRunResultPayload) (*openlinker.RuntimeRunResultAckPayload, error)
	ResumeRuntimeRuns(context.Context, openlinker.RuntimeResumePayload) (*openlinker.RuntimeResumeResponse, error)
	PollRuntimeCommands(context.Context, string, int) (*openlinker.RuntimeCommandsResponse, error)
	AckRuntimeCancel(context.Context, openlinker.RuntimeRunCancelAckPayload) (*openlinker.RuntimeRunCancellationState, error)
}

type runtimeEndpoint struct {
	origin     string
	runtime    *openlinker.Runtime
	httpClient *http.Client
}

type runtimeConnection struct {
	kind          string
	endpointIndex int
	client        runtimeClient
	ws            *openlinker.RuntimeWebSocket
	ready         openlinker.RuntimeReadyPayload
	connectedAt   time.Time
	generation    int64
}

type runtimeAttempt struct {
	identity             openlinker.RuntimeAttemptIdentity
	clientID             string
	input                map[string]any
	startedAt            time.Time
	leaseExpiresAt       time.Time
	lastAckedEvent       int64
	pendingEvent         int64
	pendingResult        string
	pendingEventPayload  *openlinker.RuntimeRunEventPayload
	pendingResultPayload *openlinker.RuntimeRunResultPayload
	confirmed            bool
	finished             bool
	canceled             bool
	done                 chan struct{}
	cancel               context.CancelFunc
}

type runtimeSwitch struct {
	At           time.Time
	From         string
	To           string
	FromEndpoint int
	ToEndpoint   int
	Reason       string
	ResumeCount  int
}

type runtimeTransportStats struct {
	Connections             int
	Ready                   int
	Offers                  int
	AssignmentACKs          int
	AssignmentACKRecoveries int
	LeaseRenews             int
	EventACKs               int
	EventACKReplays         int
	ResultACKs              int
	ResultACKReplays        int
	CancelCommands          int
	CancelACKs              int
	EmptyPolls              int
	ReadyMS                 []float64
	OfferToConfirmMS        []float64
	AssignmentMS            []float64
	RenewMS                 []float64
	EventACKMS              []float64
	ResultACKMS             []float64
	CancelACKMS             []float64
	CancelEndToEndMS        []float64
	ErrorsByCode            map[string]int
}

type runtimeMetrics struct {
	mu sync.Mutex

	byTransport  map[string]*runtimeTransportStats
	errorsByCode map[string]int
	coreIDs      map[string]int
	switches     []runtimeSwitch
	resumeMS     []float64
	resumeCount  int
	responseLoss int

	executionStarts      map[string]int
	duplicateAssignments int
	duplicateExecutions  int
	staleFenceRejects    int
	staleFenceAccepts    int
	cancelRequests       int
	cancelAccepted       int
	cancelCommandACKs    int
	dbPollingCompletions int
	redisOutageObserved  bool
	journalWrites        int
	journalErrors        int
}

func newRuntimeMetrics() *runtimeMetrics {
	return &runtimeMetrics{
		byTransport:     map[string]*runtimeTransportStats{transportWS: {}, transportPull: {}},
		errorsByCode:    map[string]int{},
		coreIDs:         map[string]int{},
		executionStarts: map[string]int{},
	}
}

func (m *runtimeMetrics) transport(kind string) *runtimeTransportStats {
	stats := m.byTransport[kind]
	if stats == nil {
		stats = &runtimeTransportStats{}
		m.byTransport[kind] = stats
	}
	return stats
}

func (m *runtimeMetrics) recordReady(kind, coreID string, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := m.transport(kind)
	stats.Connections++
	stats.Ready++
	stats.ReadyMS = append(stats.ReadyMS, ms(duration))
	m.coreIDs[coreID]++
}

func (m *runtimeMetrics) recordError(err error, transports ...string) string {
	code := stableRuntimeErrorCode(err)
	m.mu.Lock()
	m.errorsByCode[code]++
	for _, kind := range transports {
		stats := m.transport(kind)
		if stats.ErrorsByCode == nil {
			stats.ErrorsByCode = map[string]int{}
		}
		stats.ErrorsByCode[code]++
	}
	m.mu.Unlock()
	return code
}

func (m *runtimeMetrics) recordSwitch(item runtimeSwitch) {
	m.mu.Lock()
	m.switches = append(m.switches, item)
	m.mu.Unlock()
}

func (m *runtimeMetrics) recordResume(duration time.Duration, count int) {
	m.mu.Lock()
	m.resumeMS = append(m.resumeMS, ms(duration))
	m.resumeCount += count
	m.mu.Unlock()
}

func (m *runtimeMetrics) recordExecution(attemptID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.executionStarts[attemptID]++
	if m.executionStarts[attemptID] > 1 {
		m.duplicateExecutions++
	}
}

func (m *runtimeMetrics) report(cfg config) map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	transports := map[string]any{}
	aggregate := &runtimeTransportStats{}
	for _, kind := range []string{transportWS, transportPull} {
		stats := m.transport(kind)
		transports[kind] = runtimeTransportReport(stats)
		mergeRuntimeTransportStats(aggregate, stats)
	}
	transports["aggregate"] = runtimeTransportReport(aggregate)
	switches := make([]map[string]any, 0, len(m.switches))
	for _, item := range m.switches {
		switches = append(switches, map[string]any{
			"at": item.At.UTC().Format(time.RFC3339Nano), "from": item.From, "to": item.To,
			"from_endpoint": item.FromEndpoint, "to_endpoint": item.ToEndpoint,
			"reason": item.Reason, "resume_attempts": item.ResumeCount,
		})
	}
	coreIDs := make(map[string]int, len(m.coreIDs))
	for id, count := range m.coreIDs {
		coreIDs[id] = count
	}
	errorsByCode := make(map[string]int, len(m.errorsByCode))
	for code, count := range m.errorsByCode {
		errorsByCode[code] = count
	}
	assertions, allAssertionsPassed := m.scenarioAssertionsLocked(cfg)
	return map[string]any{
		"protocol_version":        openlinker.RuntimeProtocolVersion,
		"runtime_contract_id":     openlinker.RuntimeContractID,
		"runtime_contract_digest": openlinker.RuntimeContractDigest,
		"required_features":       openlinker.RuntimeRequiredFeatures(),
		"transport_policy":        "ws_primary_long_poll_fallback",
		"scenarios":               append([]string(nil), cfg.Scenarios...),
		"scenario_assertions":     assertions,
		"all_assertions_passed":   allAssertionsPassed,
		"transports":              transports,
		"switches":                switches,
		"resume": map[string]any{
			"decisions":  m.resumeCount,
			"latency_ms": summaryStats(append([]float64(nil), m.resumeMS...)),
		},
		"core_instance_ids": coreIDs,
		"errors_by_code":    errorsByCode,
		"safety": map[string]any{
			"ack_response_losses":             m.responseLoss,
			"duplicate_assignments":           m.duplicateAssignments,
			"duplicate_execution":             m.duplicateExecutions,
			"stale_fence_rejects":             m.staleFenceRejects,
			"stale_fence_accepts":             m.staleFenceAccepts,
			"cancel_requests":                 m.cancelRequests,
			"cancel_requests_accepted":        m.cancelAccepted,
			"cancel_command_acks":             m.cancelCommandACKs,
			"redis_signal_outage_observed":    m.redisOutageObserved,
			"db_polling_fallback_completions": m.dbPollingCompletions,
			"persistent_journal_writes":       m.journalWrites,
			"persistent_journal_errors":       m.journalErrors,
		},
	}
}

func (m *runtimeMetrics) scenarioAssertionsLocked(cfg config) (map[string]any, bool) {
	assertions := map[string]any{}
	allPassed := true
	add := func(name string, passed bool, evidence any) {
		assertions[name] = map[string]any{"passed": passed, "evidence": evidence}
		allPassed = allPassed && passed
	}
	ws := m.transport(transportWS)
	pull := m.transport(transportPull)
	add("runtime_ready", ws.Ready+pull.Ready > 0 && m.journalWrites > 0 && m.journalErrors == 0,
		map[string]int{"ws": ws.Ready, "pull": pull.Ready, "journal_writes": m.journalWrites, "journal_errors": m.journalErrors})
	if cfg.hasScenario("ws-only") {
		add("ws_only", ws.Offers > 0 && pull.Connections == 0, map[string]int{"ws_offers": ws.Offers, "pull_connections": pull.Connections})
	}
	if cfg.hasScenario("pull-only") {
		add("pull_only", pull.Offers > 0 && ws.Connections == 0, map[string]int{"pull_offers": pull.Offers, "ws_connections": ws.Connections})
	}
	if cfg.hasScenario("ws-pull-ws") {
		forward, back := false, false
		for _, item := range m.switches {
			forward = forward || (item.From == transportWS && item.To == transportPull)
			back = back || (item.From == transportPull && item.To == transportWS)
		}
		add("ws_pull_ws", forward && back && m.resumeCount > 0, map[string]any{"ws_to_pull": forward, "pull_to_ws": back, "resume_decisions": m.resumeCount})
	}
	if cfg.hasScenario("core-a-b-resume") {
		add("core_a_b_resume", len(m.coreIDs) >= 2 && m.resumeCount > 0, map[string]any{"core_instances": len(m.coreIDs), "resume_decisions": m.resumeCount})
	}
	if cfg.hasScenario("ack-response-loss") {
		passed := m.responseLoss > 0 && pull.AssignmentACKRecoveries > 0 && pull.ResultACKReplays > 0
		if cfg.EventsPerRun > 0 {
			passed = passed && pull.EventACKReplays > 0
		}
		add("ack_response_loss", passed, map[string]int{
			"lost": m.responseLoss, "assignment_recoveries": pull.AssignmentACKRecoveries,
			"event_replays": pull.EventACKReplays, "result_replays": pull.ResultACKReplays,
		})
	}
	if cfg.hasScenario("duplicate-assignment") {
		add("duplicate_assignment", m.duplicateAssignments > 0 && m.duplicateExecutions == 0,
			map[string]int{"deliveries": m.duplicateAssignments, "duplicate_execution": m.duplicateExecutions})
	}
	if cfg.hasScenario("stale-fence") {
		add("stale_fence", m.staleFenceRejects > 0 && m.staleFenceAccepts == 0,
			map[string]int{"rejects": m.staleFenceRejects, "accepts": m.staleFenceAccepts})
	}
	if cfg.hasScenario("cancel-race") {
		raceLost := m.errorsByCode["CANCEL_RACE_LOST_TO_RESULT"]
		add("cancel_race", m.cancelRequests == cfg.CancelCount && m.cancelCommandACKs+raceLost >= cfg.CancelCount,
			map[string]int{"requests": m.cancelRequests, "accepted": m.cancelAccepted, "target": cfg.CancelCount, "command_acks": m.cancelCommandACKs, "result_wins": raceLost})
	}
	if cfg.hasScenario("redis-signal-outage") {
		add("redis_db_polling_fallback", m.redisOutageObserved && m.dbPollingCompletions > 0,
			map[string]any{"outage_observed": m.redisOutageObserved, "pull_completions": m.dbPollingCompletions})
	}
	return assertions, allPassed
}

func runtimeTransportReport(stats *runtimeTransportStats) map[string]any {
	if stats == nil {
		stats = &runtimeTransportStats{}
	}
	return map[string]any{
		"connections": stats.Connections, "ready": stats.Ready, "offers": stats.Offers,
		"assignment_acks": stats.AssignmentACKs, "assignment_ack_recoveries": stats.AssignmentACKRecoveries,
		"lease_renews": stats.LeaseRenews, "event_acks": stats.EventACKs,
		"event_ack_replays": stats.EventACKReplays, "result_acks": stats.ResultACKs,
		"result_ack_replays": stats.ResultACKReplays, "cancel_commands": stats.CancelCommands,
		"cancel_acks": stats.CancelACKs, "empty_polls": stats.EmptyPolls,
		"ready_ms":             summaryStats(append([]float64(nil), stats.ReadyMS...)),
		"offer_to_confirm_ms":  summaryStats(append([]float64(nil), stats.OfferToConfirmMS...)),
		"assignment_ms":        summaryStats(append([]float64(nil), stats.AssignmentMS...)),
		"renew_ms":             summaryStats(append([]float64(nil), stats.RenewMS...)),
		"event_ack_ms":         summaryStats(append([]float64(nil), stats.EventACKMS...)),
		"result_ack_ms":        summaryStats(append([]float64(nil), stats.ResultACKMS...)),
		"cancel_ack_ms":        summaryStats(append([]float64(nil), stats.CancelACKMS...)),
		"cancel_end_to_end_ms": summaryStats(append([]float64(nil), stats.CancelEndToEndMS...)),
		"errors_by_code":       cloneStringIntMap(stats.ErrorsByCode),
	}
}

func mergeRuntimeTransportStats(dst, src *runtimeTransportStats) {
	dst.Connections += src.Connections
	dst.Ready += src.Ready
	dst.Offers += src.Offers
	dst.AssignmentACKs += src.AssignmentACKs
	dst.AssignmentACKRecoveries += src.AssignmentACKRecoveries
	dst.LeaseRenews += src.LeaseRenews
	dst.EventACKs += src.EventACKs
	dst.EventACKReplays += src.EventACKReplays
	dst.ResultACKs += src.ResultACKs
	dst.ResultACKReplays += src.ResultACKReplays
	dst.CancelCommands += src.CancelCommands
	dst.CancelACKs += src.CancelACKs
	dst.EmptyPolls += src.EmptyPolls
	dst.ReadyMS = append(dst.ReadyMS, src.ReadyMS...)
	dst.OfferToConfirmMS = append(dst.OfferToConfirmMS, src.OfferToConfirmMS...)
	dst.AssignmentMS = append(dst.AssignmentMS, src.AssignmentMS...)
	dst.RenewMS = append(dst.RenewMS, src.RenewMS...)
	dst.EventACKMS = append(dst.EventACKMS, src.EventACKMS...)
	dst.ResultACKMS = append(dst.ResultACKMS, src.ResultACKMS...)
	dst.CancelACKMS = append(dst.CancelACKMS, src.CancelACKMS...)
	dst.CancelEndToEndMS = append(dst.CancelEndToEndMS, src.CancelEndToEndMS...)
	if dst.ErrorsByCode == nil {
		dst.ErrorsByCode = map[string]int{}
	}
	for code, count := range src.ErrorsByCode {
		dst.ErrorsByCode[code] += count
	}
}

func cloneStringIntMap(source map[string]int) map[string]int {
	clone := make(map[string]int, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func stableRuntimeErrorCode(err error) string {
	if err == nil {
		return "OK"
	}
	if errors.Is(err, errACKResponseLost) {
		return "ACK_RESPONSE_LOST"
	}
	var runtimeErr *openlinker.Error
	if errors.As(err, &runtimeErr) && strings.TrimSpace(runtimeErr.Code) != "" {
		return strings.ToUpper(strings.TrimSpace(runtimeErr.Code))
	}
	if errors.Is(err, context.Canceled) {
		return "CLIENT_CONTEXT_CANCELED"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "CLIENT_DEADLINE_EXCEEDED"
	}
	return "CLIENT_TRANSPORT_ERROR"
}

type dropACKRoundTripper struct {
	base    http.RoundTripper
	metrics *runtimeMetrics
	mu      sync.Mutex
	remain  map[string]int
}

func (d *dropACKRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := d.base.RoundTrip(request)
	if err != nil || response == nil || response.StatusCode < 200 || response.StatusCode >= 300 {
		return response, err
	}
	op := runtimeACKOperation(request.URL.Path)
	if op == "" || !d.consume(op) {
		return response, nil
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if d.metrics != nil {
		d.metrics.mu.Lock()
		d.metrics.responseLoss++
		d.metrics.mu.Unlock()
	}
	return nil, fmt.Errorf("%w: %s", errACKResponseLost, op)
}

func (d *dropACKRoundTripper) consume(op string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.remain[op] <= 0 {
		return false
	}
	d.remain[op]--
	return true
}

func runtimeACKOperation(path string) string {
	switch {
	case strings.HasSuffix(path, "/assignment-ack"):
		return "assignment"
	case strings.HasSuffix(path, "/events"):
		return "event"
	case strings.HasSuffix(path, "/result"):
		return "result"
	case strings.HasSuffix(path, "/cancel-ack"):
		return "cancel"
	default:
		return ""
	}
}

func newRuntimeEndpoint(cfg config, origin, token string, metrics *runtimeMetrics) (*runtimeEndpoint, error) {
	parsed, err := url.Parse(strings.TrimSpace(origin))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return nil, errors.New("Runtime URL must be an absolute https URL")
	}
	certificate, err := tls.LoadX509KeyPair(cfg.MTLSCertFile, cfg.MTLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load Runtime Node mTLS certificate: %w", err)
	}
	caPEM, err := os.ReadFile(cfg.MTLSCAFile)
	if err != nil {
		return nil, fmt.Errorf("read Runtime server CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("Runtime server CA file contains no certificates")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate},
		RootCAs: roots, ServerName: strings.TrimSpace(cfg.MTLSServerName),
	}
	transport.ResponseHeaderTimeout = time.Duration(openlinker.RuntimeMaxPullWaitSeconds+5) * time.Second
	transport.TLSHandshakeTimeout = 10 * time.Second
	transport.IdleConnTimeout = 90 * time.Second
	var roundTripper http.RoundTripper = transport
	if len(cfg.DropACKResponses) > 0 {
		remain := make(map[string]int, len(cfg.DropACKResponses))
		for _, op := range cfg.DropACKResponses {
			remain[op]++
		}
		roundTripper = &dropACKRoundTripper{base: transport, metrics: metrics, remain: remain}
	}
	httpClient := &http.Client{
		Transport:     roundTripper,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	runtimeClient, err := openlinker.NewRuntime(
		origin,
		openlinker.WithAgentToken(token),
		openlinker.WithHTTPClient(httpClient),
		openlinker.WithSDKAgent("openlinker-runtime-loadtest/reliable-run"),
	)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	return &runtimeEndpoint{origin: origin, runtime: runtimeClient, httpClient: httpClient}, nil
}

func closeRuntimeEndpoint(endpoint *runtimeEndpoint) {
	if endpoint == nil || endpoint.httpClient == nil {
		return
	}
	if transport, ok := endpoint.httpClient.Transport.(interface{ CloseIdleConnections() }); ok {
		transport.CloseIdleConnections()
	}
}

func sortedScenarioList(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

func parseCSV(raw string) []string {
	seen := map[string]struct{}{}
	values := []string{}
	for _, part := range strings.Split(raw, ",") {
		value := strings.ToLower(strings.TrimSpace(part))
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		values = append(values, value)
	}
	return values
}

func (c config) hasScenario(name string) bool {
	for _, scenario := range c.Scenarios {
		if scenario == name {
			return true
		}
	}
	return false
}

func validateRuntimeOrigin(raw, flagName string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("%s must be an absolute https Runtime origin", flagName)
	}
	return nil
}

func preflightRuntimeCredentials(cfg config) error {
	if info, err := os.Stat(cfg.MTLSKeyFile); err != nil {
		return fmt.Errorf("stat Runtime Node private key: %w", err)
	} else if info.Mode().Perm()&0o077 != 0 {
		return errors.New("Runtime Node private key must not be readable or writable by group/other")
	}
	pair, err := tls.LoadX509KeyPair(cfg.MTLSCertFile, cfg.MTLSKeyFile)
	if err != nil {
		return fmt.Errorf("load Runtime Node mTLS certificate and key: %w", err)
	}
	if len(pair.Certificate) == 0 {
		return errors.New("Runtime Node certificate chain is empty")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return fmt.Errorf("parse Runtime Node certificate: %w", err)
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return errors.New("Runtime Node certificate is not currently valid")
	}
	if leaf.IsCA || leaf.KeyUsage&x509.KeyUsageDigitalSignature == 0 || !containsClientAuthEKU(leaf.ExtKeyUsage) {
		return errors.New("Runtime Node certificate must be a non-CA client-auth signing certificate")
	}
	wantNodeID, _ := uuid.Parse(cfg.NodeID)
	found := uuid.Nil
	for _, identity := range leaf.URIs {
		const prefix = "urn:openlinker:runtime-node:"
		if identity == nil || !strings.HasPrefix(identity.String(), prefix) {
			continue
		}
		candidate, parseErr := uuid.Parse(strings.TrimPrefix(identity.String(), prefix))
		if parseErr != nil || found != uuid.Nil {
			return errors.New("Runtime Node certificate contains an invalid or ambiguous Node identity")
		}
		found = candidate
	}
	if found == uuid.Nil || found != wantNodeID {
		return errors.New("node-id does not match the Runtime Node certificate identity")
	}
	caPEM, err := os.ReadFile(cfg.MTLSCAFile)
	if err != nil {
		return fmt.Errorf("read Runtime server CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return errors.New("Runtime server CA file contains no certificates")
	}
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		return fmt.Errorf("create Runtime state directory: %w", err)
	}
	if err := os.Chmod(cfg.StateDir, 0o700); err != nil {
		return fmt.Errorf("secure Runtime state directory: %w", err)
	}
	return nil
}

func containsClientAuthEKU(values []x509.ExtKeyUsage) bool {
	for _, value := range values {
		if value == x509.ExtKeyUsageClientAuth {
			return true
		}
	}
	return false
}

func (c *config) applyScenarioDefaults() error {
	if len(c.Scenarios) == 0 {
		c.Scenarios = []string{"baseline"}
	}
	recognized := map[string]struct{}{
		"baseline": {}, "ws-only": {}, "pull-only": {}, "ws-pull-ws": {},
		"core-a-b-resume": {}, "ack-response-loss": {}, "duplicate-assignment": {},
		"stale-fence": {}, "cancel-race": {}, "redis-signal-outage": {},
	}
	for _, scenario := range c.Scenarios {
		if _, ok := recognized[scenario]; !ok {
			return fmt.Errorf("unsupported Runtime scenario %q", scenario)
		}
	}
	if c.hasScenario("ws-only") && c.hasScenario("pull-only") {
		return errors.New("ws-only and pull-only scenarios are mutually exclusive")
	}
	if c.hasScenario("ws-only") {
		c.Transport = transportWS
	}
	if c.hasScenario("pull-only") {
		c.Transport = transportPull
	}
	if c.hasScenario("ws-pull-ws") {
		c.Transport = transportAuto
		if c.SwitchAfter <= 0 || c.SwitchBackAfter <= c.SwitchAfter {
			return errors.New("ws-pull-ws requires 0 < switch-after < switch-back-after")
		}
	}
	if c.hasScenario("core-a-b-resume") {
		if c.RuntimeURLSecondary == "" {
			return errors.New("core-a-b-resume requires runtime-url-secondary")
		}
		if c.SwitchAfter <= 0 {
			return errors.New("core-a-b-resume requires a positive switch-after")
		}
	}
	if c.hasScenario("ack-response-loss") {
		if c.Transport != transportPull {
			return errors.New("ack-response-loss must run with transport=pull so response loss occurs after an HTTP commit")
		}
		if len(c.DropACKResponses) == 0 {
			c.DropACKResponses = []string{"assignment", "event", "result", "cancel"}
		}
	}
	if c.hasScenario("duplicate-assignment") && c.DuplicateAssignments == 0 {
		c.DuplicateAssignments = 1
	}
	if c.hasScenario("stale-fence") && c.StaleFenceProbes == 0 {
		c.StaleFenceProbes = 1
	}
	if c.hasScenario("cancel-race") {
		if c.CancelCount == 0 {
			c.CancelCount = 1000
		}
		if c.Runs < c.CancelCount {
			return errors.New("cancel-race requires runs >= cancel-count (1000 by default)")
		}
		if c.Agents*c.WorkersPerAgent < c.CancelCount {
			return errors.New("cancel-race requires agents × workers-per-agent >= cancel-count so every cancellation reaches an executing Attempt")
		}
		if c.ResultDelay <= 0 {
			c.ResultDelay = 10 * time.Second
		}
		if c.CancelDelay <= 0 {
			c.CancelDelay = c.ResultDelay
		}
	}
	if c.hasScenario("redis-signal-outage") {
		if c.Transport != transportPull {
			return errors.New("redis-signal-outage must use transport=pull to prove DB long-poll fallback")
		}
		if c.RedisOutageObserve <= 0 {
			c.RedisOutageObserve = 30 * time.Second
		}
	}
	c.Scenarios = sortedScenarioList(c.Scenarios)
	return nil
}

var _ runtimeClient = (*openlinker.Runtime)(nil)

type runtimeWorker struct {
	cfg         config
	agent       agentRef
	workerIndex int
	tracker     *runTracker
	metrics     *metrics
	hello       openlinker.RuntimeHelloPayload
	endpoints   []*runtimeEndpoint

	startedAt time.Time
	readyOnce sync.Once

	connectionMu sync.RWMutex
	connection   *runtimeConnection
	generation   atomic.Int64
	failure      chan error

	attemptsMu sync.Mutex
	attempts   map[string]*runtimeAttempt
	inflight   int64
	journal    *runtimeJournal
}

type runtimeJournal struct {
	mu   sync.Mutex
	path string
}

type runtimeJournalDocument struct {
	ProtocolVersion       int                     `json:"protocol_version"`
	RuntimeContractID     string                  `json:"runtime_contract_id"`
	RuntimeContractDigest string                  `json:"runtime_contract_digest"`
	NodeID                string                  `json:"node_id"`
	AgentID               string                  `json:"agent_id"`
	WorkerID              string                  `json:"worker_id"`
	RuntimeSessionID      string                  `json:"runtime_session_id"`
	UpdatedAt             time.Time               `json:"updated_at"`
	Attempts              []runtimeJournalAttempt `json:"attempts"`
}

type runtimeJournalAttempt struct {
	Identity             openlinker.RuntimeAttemptIdentity   `json:"attempt_identity"`
	ClientID             string                              `json:"client_id"`
	Confirmed            bool                                `json:"confirmed"`
	Finished             bool                                `json:"finished"`
	Canceled             bool                                `json:"canceled"`
	LastAckedEvent       int64                               `json:"last_acked_client_event_seq"`
	PendingEvent         int64                               `json:"pending_client_event_seq,omitempty"`
	PendingResult        string                              `json:"pending_result_id,omitempty"`
	PendingEventPayload  *openlinker.RuntimeRunEventPayload  `json:"pending_event,omitempty"`
	PendingResultPayload *openlinker.RuntimeRunResultPayload `json:"pending_result,omitempty"`
}

func newRuntimeWorker(
	cfg config,
	agent agentRef,
	token string,
	workerIndex int,
	tracker *runTracker,
	metrics *metrics,
) (*runtimeWorker, error) {
	if metrics == nil || metrics.runtime == nil {
		return nil, errors.New("Runtime metrics are required")
	}
	workerID := fmt.Sprintf("loadtest-%s-%02d", agent.ID, workerIndex)
	hello := openlinker.RuntimeHelloPayload{
		NodeID:           cfg.NodeID,
		AgentID:          agent.ID,
		WorkerID:         workerID,
		RuntimeSessionID: uuid.NewString(),
		SessionEpoch:     1,
		NodeVersion:      cfg.NodeVersion,
		Capacity:         int64(cfg.NodeCapacity),
		Features:         openlinker.RuntimeRequiredFeatures(),
		ContractDigest:   openlinker.RuntimeContractDigest,
	}
	origins := []string{cfg.RuntimeURL}
	if cfg.RuntimeURLSecondary != "" {
		origins = append(origins, cfg.RuntimeURLSecondary)
	}
	endpoints := make([]*runtimeEndpoint, 0, len(origins))
	for _, origin := range origins {
		endpoint, err := newRuntimeEndpoint(cfg, origin, token, metrics.runtime)
		if err != nil {
			for _, created := range endpoints {
				closeRuntimeEndpoint(created)
			}
			return nil, err
		}
		endpoints = append(endpoints, endpoint)
	}
	worker := &runtimeWorker{
		cfg: cfg, agent: agent, workerIndex: workerIndex, tracker: tracker, metrics: metrics,
		hello: hello, endpoints: endpoints, startedAt: time.Now(), failure: make(chan error, 1),
		attempts: make(map[string]*runtimeAttempt),
		journal:  &runtimeJournal{path: filepath.Join(cfg.StateDir, hello.RuntimeSessionID+".json")},
	}
	if err := worker.persistState(); err != nil {
		worker.close()
		return nil, err
	}
	return worker, nil
}

func (w *runtimeWorker) close() {
	for _, endpoint := range w.endpoints {
		closeRuntimeEndpoint(endpoint)
	}
}

func (w *runtimeWorker) persistState() error {
	if w == nil || w.journal == nil {
		return errors.New("Runtime durable journal is unavailable")
	}
	w.attemptsMu.Lock()
	keys := make([]string, 0, len(w.attempts))
	for key := range w.attempts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	attempts := make([]runtimeJournalAttempt, 0, len(keys))
	for _, key := range keys {
		attempt := w.attempts[key]
		attempts = append(attempts, runtimeJournalAttempt{
			Identity: attempt.identity, ClientID: attempt.clientID,
			Confirmed: attempt.confirmed, Finished: attempt.finished, Canceled: attempt.canceled,
			LastAckedEvent: attempt.lastAckedEvent, PendingEvent: attempt.pendingEvent,
			PendingResult:        attempt.pendingResult,
			PendingEventPayload:  attempt.pendingEventPayload,
			PendingResultPayload: attempt.pendingResultPayload,
		})
	}
	w.attemptsMu.Unlock()
	document := runtimeJournalDocument{
		ProtocolVersion: openlinker.RuntimeProtocolVersion, RuntimeContractID: openlinker.RuntimeContractID,
		RuntimeContractDigest: openlinker.RuntimeContractDigest,
		NodeID:                w.hello.NodeID, AgentID: w.hello.AgentID, WorkerID: w.hello.WorkerID,
		RuntimeSessionID: w.hello.RuntimeSessionID, UpdatedAt: time.Now().UTC(), Attempts: attempts,
	}
	err := w.journal.write(document)
	w.metrics.runtime.mu.Lock()
	if err != nil {
		w.metrics.runtime.journalErrors++
	} else {
		w.metrics.runtime.journalWrites++
	}
	w.metrics.runtime.mu.Unlock()
	return err
}

func (j *runtimeJournal) write(document runtimeJournalDocument) error {
	if j == nil || strings.TrimSpace(j.path) == "" {
		return errors.New("Runtime journal path is empty")
	}
	raw, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("encode Runtime journal: %w", err)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	temporary := j.path + ".tmp-" + uuid.NewString()
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create Runtime journal: %w", err)
	}
	published := false
	defer func() {
		_ = file.Close()
		if !published {
			_ = os.Remove(temporary)
		}
	}()
	if _, err = file.Write(append(raw, '\n')); err != nil {
		return fmt.Errorf("write Runtime journal: %w", err)
	}
	if err = file.Sync(); err != nil {
		return fmt.Errorf("sync Runtime journal: %w", err)
	}
	if err = file.Close(); err != nil {
		return fmt.Errorf("close Runtime journal: %w", err)
	}
	if err = os.Rename(temporary, j.path); err != nil {
		return fmt.Errorf("publish Runtime journal: %w", err)
	}
	published = true
	directory, err := os.Open(filepath.Dir(j.path))
	if err != nil {
		return fmt.Errorf("open Runtime journal directory: %w", err)
	}
	if err = directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("sync Runtime journal directory: %w", err)
	}
	if err = directory.Close(); err != nil {
		return fmt.Errorf("close Runtime journal directory: %w", err)
	}
	return nil
}

func (w *runtimeWorker) run(ctx context.Context) {
	defer w.close()
	kind := w.initialTransport()
	endpointIndex := 0
	reason := "initial"
	fallbackUntil := time.Time{}
	var previous *runtimeConnection
	for ctx.Err() == nil {
		desiredKind, desiredEndpoint, desiredReason := w.desiredTransport(time.Now(), kind, endpointIndex, fallbackUntil)
		kind, endpointIndex = desiredKind, desiredEndpoint
		if desiredReason != "" {
			reason = desiredReason
		}
		connection, err := w.connect(ctx, kind, endpointIndex)
		if err != nil {
			w.metrics.runtime.recordError(err, kind)
			w.metrics.recordWorkerError(err)
			if w.cfg.Transport == transportAuto && kind == transportWS {
				kind = transportPull
				fallbackUntil = time.Now().Add(w.cfg.WSProbeInterval)
				reason = "websocket_connect_failed"
				continue
			}
			if !sleepRuntimeContext(ctx, time.Second) {
				return
			}
			continue
		}
		if err = w.resume(ctx, connection); err != nil {
			w.metrics.runtime.recordError(err, connection.kind)
			w.clearConnection(connection)
			w.closeConnection(connection, "resume_failed")
			if w.cfg.Transport == transportAuto && kind == transportWS {
				kind = transportPull
				fallbackUntil = time.Now().Add(w.cfg.WSProbeInterval)
				reason = "websocket_resume_failed"
			}
			continue
		}
		if previous != nil {
			w.metrics.runtime.recordSwitch(runtimeSwitch{
				At: time.Now(), From: previous.kind, To: connection.kind,
				FromEndpoint: previous.endpointIndex, ToEndpoint: connection.endpointIndex,
				Reason: reason, ResumeCount: int(w.activeAttemptCount()),
			})
		}
		previous = connection
		// The replacement transport is not visible to Event/Result senders until
		// Core has reconciled every durable Attempt for this Session.
		w.publishConnection(connection)
		w.metrics.c.workersConnected.Add(1)
		w.readyOnce.Do(func() {
			w.metrics.c.workersReady.Add(1)
		})
		serveErr := w.serve(ctx, connection, fallbackUntil)
		w.metrics.c.workersConnected.Add(-1)
		w.clearConnection(connection)
		w.closeConnection(connection, "transport_switch")
		if ctx.Err() != nil {
			return
		}
		if serveErr != nil && !errors.Is(serveErr, errRuntimePlannedSwitch) {
			w.metrics.runtime.recordError(serveErr, connection.kind)
			if w.cfg.Transport == transportAuto && connection.kind == transportWS {
				kind = transportPull
				fallbackUntil = time.Now().Add(w.cfg.WSProbeInterval)
				reason = "websocket_unavailable"
			} else {
				reason = "transport_reconnect"
			}
			continue
		}
		kind, endpointIndex, reason = w.desiredTransport(time.Now(), kind, endpointIndex, fallbackUntil)
	}
}

var errRuntimePlannedSwitch = errors.New("runtime-loadtest: planned Runtime transport switch")

func waitForWorkersReady(ctx context.Context, cfg config, metrics *metrics, want int) error {
	timeout := cfg.ReadyTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if int(metrics.c.workersReady.Load()) >= want {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timeout waiting for Runtime workers: got=%d want=%d", metrics.c.workersReady.Load(), want)
		case <-ticker.C:
		}
	}
}

func (w *runtimeWorker) initialTransport() string {
	if w.cfg.Transport == transportPull {
		return transportPull
	}
	return transportWS
}

func (w *runtimeWorker) desiredTransport(
	now time.Time,
	currentKind string,
	currentEndpoint int,
	fallbackUntil time.Time,
) (string, int, string) {
	elapsed := now.Sub(w.startedAt)
	if w.cfg.hasScenario("core-a-b-resume") && w.cfg.SwitchAfter > 0 && elapsed >= w.cfg.SwitchAfter {
		return currentKind, 1, "core_a_to_b_resume"
	}
	if w.cfg.hasScenario("ws-pull-ws") {
		switch {
		case w.cfg.SwitchBackAfter > 0 && elapsed >= w.cfg.SwitchBackAfter:
			return transportWS, currentEndpoint, "planned_pull_to_websocket"
		case w.cfg.SwitchAfter > 0 && elapsed >= w.cfg.SwitchAfter:
			return transportPull, currentEndpoint, "planned_websocket_to_pull"
		default:
			return transportWS, currentEndpoint, ""
		}
	}
	if w.cfg.Transport == transportAuto && currentKind == transportPull && !fallbackUntil.IsZero() && !now.Before(fallbackUntil) {
		return transportWS, currentEndpoint, "websocket_probe_recovered"
	}
	return currentKind, currentEndpoint, ""
}

func (w *runtimeWorker) connect(ctx context.Context, kind string, endpointIndex int) (*runtimeConnection, error) {
	if endpointIndex < 0 || endpointIndex >= len(w.endpoints) {
		return nil, errors.New("Runtime endpoint index is unavailable")
	}
	endpoint := w.endpoints[endpointIndex]
	connectCtx, cancel := context.WithTimeout(ctx, w.cfg.ReadyTimeout)
	defer cancel()
	started := time.Now()
	connection := &runtimeConnection{
		kind: kind, endpointIndex: endpointIndex, connectedAt: started,
		generation: w.generation.Add(1),
	}
	switch kind {
	case transportWS:
		ws, err := endpoint.runtime.DialRuntimeWebSocket(connectCtx, w.hello)
		if err != nil {
			return nil, err
		}
		connection.client = ws
		connection.ws = ws
		connection.ready = ws.Ready()
	case transportPull:
		ready, err := endpoint.runtime.CreateRuntimeSession(connectCtx, w.hello)
		if err != nil {
			return nil, err
		}
		connection.client = endpoint.runtime
		connection.ready = *ready
	default:
		return nil, fmt.Errorf("unsupported Runtime transport %q", kind)
	}
	w.metrics.runtime.recordReady(kind, connection.ready.CoreInstanceID, time.Since(started))
	return connection, nil
}

func (w *runtimeWorker) publishConnection(connection *runtimeConnection) {
	w.connectionMu.Lock()
	w.connection = connection
	w.connectionMu.Unlock()
}

func (w *runtimeWorker) clearConnection(connection *runtimeConnection) {
	w.connectionMu.Lock()
	if w.connection == connection {
		w.connection = nil
	}
	w.connectionMu.Unlock()
}

func (w *runtimeWorker) currentConnection() *runtimeConnection {
	w.connectionMu.RLock()
	defer w.connectionMu.RUnlock()
	return w.connection
}

func (w *runtimeWorker) closeConnection(connection *runtimeConnection, reason string) {
	if connection == nil || connection.client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = connection.client.CloseRuntimeSession(ctx, openlinker.RuntimeSessionCloseRequest{
		NodeID: w.hello.NodeID, AgentID: w.hello.AgentID, WorkerID: w.hello.WorkerID,
		RuntimeSessionID: w.hello.RuntimeSessionID, SessionEpoch: w.hello.SessionEpoch,
		Status: "offline", Reason: reason,
	})
}

func (w *runtimeWorker) serve(ctx context.Context, connection *runtimeConnection, fallbackUntil time.Time) error {
	if connection.kind == transportWS {
		return w.serveWS(ctx, connection, fallbackUntil)
	}
	return w.servePull(ctx, connection, fallbackUntil)
}

func (w *runtimeWorker) serveWS(ctx context.Context, connection *runtimeConnection, fallbackUntil time.Time) error {
	check := time.NewTicker(100 * time.Millisecond)
	defer check.Stop()
	heartbeat := time.NewTicker(w.cfg.HeartbeatInterval)
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-w.failure:
			return err
		case <-connection.ws.Done():
			if err := connection.ws.Err(); err != nil {
				return err
			}
			return errors.New("Runtime WebSocket closed")
		case assignment := <-connection.ws.Assignments():
			w.handleAssignment(ctx, connection, assignment.Payload, false)
		case command := <-connection.ws.Commands():
			w.handleCommand(ctx, connection, command.Command)
		case <-heartbeat.C:
			if _, err := connection.client.HeartbeatRuntimeSession(ctx, w.hello); err != nil {
				return err
			}
		case <-check.C:
			kind, endpoint, _ := w.desiredTransport(time.Now(), connection.kind, connection.endpointIndex, fallbackUntil)
			if kind != connection.kind || endpoint != connection.endpointIndex {
				return errRuntimePlannedSwitch
			}
		}
	}
}

func (w *runtimeWorker) servePull(ctx context.Context, connection *runtimeConnection, fallbackUntil time.Time) error {
	lastHeartbeat := time.Now()
	for ctx.Err() == nil {
		kind, endpoint, _ := w.desiredTransport(time.Now(), connection.kind, connection.endpointIndex, fallbackUntil)
		if kind != connection.kind || endpoint != connection.endpointIndex {
			if kind == transportWS && w.cfg.Transport == transportAuto && !w.cfg.hasScenario("ws-pull-ws") {
				probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := w.endpoints[connection.endpointIndex].runtime.ProbeRuntimeWebSocket(probeCtx)
				cancel()
				if err != nil {
					fallbackUntil = time.Now().Add(w.cfg.WSProbeInterval)
					w.metrics.runtime.recordError(err, transportWS)
					continue
				}
			}
			return errRuntimePlannedSwitch
		}
		select {
		case err := <-w.failure:
			return err
		default:
		}
		if time.Since(lastHeartbeat) >= w.cfg.HeartbeatInterval {
			if _, err := connection.client.HeartbeatRuntimeSession(ctx, w.hello); err != nil {
				return err
			}
			lastHeartbeat = time.Now()
		}
		if w.activeAttemptCount() == 0 {
			assigned, err := connection.client.ClaimRuntimeRun(ctx, durationSeconds(w.cfg.PullWait), openlinker.RuntimeClaimRequest{
				RuntimeSessionID: w.hello.RuntimeSessionID, Capacity: int64(w.cfg.NodeCapacity), Inflight: 0,
			})
			if err != nil {
				return err
			}
			if assigned == nil {
				w.metrics.runtime.mu.Lock()
				w.metrics.runtime.transport(transportPull).EmptyPolls++
				w.metrics.runtime.mu.Unlock()
			} else {
				w.handleAssignment(ctx, connection, *assigned, false)
			}
		}
		commands, err := connection.client.PollRuntimeCommands(ctx, w.hello.RuntimeSessionID, durationSeconds(w.cfg.CommandWait))
		if err != nil {
			return err
		}
		for _, command := range commands.Commands {
			decoded, decodeErr := command.Decode()
			if decodeErr != nil {
				return decodeErr
			}
			w.handleCommand(ctx, connection, decoded)
		}
	}
	return ctx.Err()
}

func durationSeconds(value time.Duration) int {
	seconds := int(value / time.Second)
	if seconds < 0 {
		return 0
	}
	if seconds > openlinker.RuntimeMaxPullWaitSeconds {
		return openlinker.RuntimeMaxPullWaitSeconds
	}
	return seconds
}

func sleepRuntimeContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

var _ runtimeClient = (*openlinker.RuntimeWebSocket)(nil)

func (w *runtimeWorker) handleAssignment(
	ctx context.Context,
	connection *runtimeConnection,
	assigned openlinker.RuntimeRunAssignedPayload,
	injectedDuplicate bool,
) {
	receivedAt := time.Now()
	clientID := stringValue(assigned.Input["client_task_id"])
	w.metrics.c.assignments.Add(1)
	w.metrics.runtime.mu.Lock()
	stats := w.metrics.runtime.transport(connection.kind)
	stats.Offers++
	if submittedAt := w.tracker.submittedAt(clientID, assigned.AttemptIdentity.RunID); !submittedAt.IsZero() {
		stats.AssignmentMS = append(stats.AssignmentMS, ms(receivedAt.Sub(submittedAt)))
	}
	w.metrics.runtime.mu.Unlock()
	if clientID == "" {
		w.metrics.c.unknownAssignment.Add(1)
	}
	w.tracker.markAssigned(assigned.AttemptIdentity.RunID, clientID, receivedAt)

	attemptID := assigned.AttemptIdentity.AttemptID
	w.attemptsMu.Lock()
	existing := w.attempts[attemptID]
	if existing != nil {
		confirmed := existing.confirmed
		w.metrics.runtime.mu.Lock()
		w.metrics.runtime.duplicateAssignments++
		w.metrics.runtime.mu.Unlock()
		w.attemptsMu.Unlock()
		if !injectedDuplicate {
			if confirmed {
				w.ackDuplicateAssignment(ctx, connection, existing)
			} else {
				w.ackAndStartAssignment(ctx, connection, existing, receivedAt)
			}
		}
		return
	}
	attemptCtx, cancel := context.WithCancel(ctx)
	attempt := &runtimeAttempt{
		identity: assigned.AttemptIdentity, clientID: clientID, input: assigned.Input,
		startedAt: receivedAt, done: make(chan struct{}), cancel: cancel,
	}
	w.attempts[attemptID] = attempt
	w.attemptsMu.Unlock()
	if err := w.persistState(); err != nil {
		w.attemptsMu.Lock()
		delete(w.attempts, attemptID)
		w.attemptsMu.Unlock()
		cancel()
		w.metrics.recordWorkerError(err)
		w.signalFailure(err)
		return
	}
	w.ackAndStartAssignment(attemptCtx, connection, attempt, receivedAt)
	for i := 0; i < w.cfg.DuplicateAssignments; i++ {
		w.handleAssignment(ctx, connection, assigned, true)
	}
}

func (w *runtimeWorker) ackDuplicateAssignment(
	ctx context.Context,
	connection *runtimeConnection,
	attempt *runtimeAttempt,
) {
	request := openlinker.RuntimeAssignmentAckPayload{AttemptIdentity: attempt.identity}
	for ctx.Err() == nil {
		started := time.Now()
		confirmed, err := connection.client.AckRuntimeAssignment(ctx, request)
		if err != nil {
			w.metrics.runtime.recordError(err, connection.kind)
			if errors.Is(err, errACKResponseLost) {
				continue
			}
			w.signalFailure(err)
			return
		}
		if confirmed == nil || confirmed.AttemptIdentity != attempt.identity {
			w.signalFailure(errors.New("duplicate assignment confirmation identity mismatch"))
			return
		}
		w.metrics.runtime.mu.Lock()
		stats := w.metrics.runtime.transport(connection.kind)
		stats.AssignmentACKs++
		stats.OfferToConfirmMS = append(stats.OfferToConfirmMS, ms(time.Since(started)))
		w.metrics.runtime.mu.Unlock()
		return
	}
}

func (w *runtimeWorker) ackAndStartAssignment(
	ctx context.Context,
	connection *runtimeConnection,
	attempt *runtimeAttempt,
	receivedAt time.Time,
) {
	request := openlinker.RuntimeAssignmentAckPayload{AttemptIdentity: attempt.identity}
	var confirmed *openlinker.RuntimeAssignmentConfirmedPayload
	recovered := false
	for ctx.Err() == nil {
		active := w.currentConnection()
		if active == nil {
			if !sleepRuntimeContext(ctx, 25*time.Millisecond) {
				return
			}
			continue
		}
		if active.kind == transportWS && active.generation != connection.generation {
			// A WebSocket ACK is correlated to the concrete offer message on the
			// socket that delivered it. A replacement socket must wait for Core to
			// replay the offer instead of inventing a correlation.
			return
		}
		started := time.Now()
		response, err := active.client.AckRuntimeAssignment(ctx, request)
		if err == nil {
			confirmed = response
			w.metrics.runtime.mu.Lock()
			stats := w.metrics.runtime.transport(active.kind)
			stats.AssignmentACKs++
			stats.OfferToConfirmMS = append(stats.OfferToConfirmMS, ms(time.Since(receivedAt)))
			if recovered {
				stats.AssignmentACKRecoveries++
			}
			w.metrics.runtime.mu.Unlock()
			_ = started
			break
		}
		code := w.metrics.runtime.recordError(err, active.kind)
		if errors.Is(err, errACKResponseLost) {
			recovered = true
			continue
		}
		if runtimeErrorPermanent(code) {
			w.failAttempt(attempt, err)
			return
		}
		w.signalFailure(err)
		if !sleepRuntimeContext(ctx, 50*time.Millisecond) {
			return
		}
	}
	if confirmed == nil || confirmed.AttemptIdentity != attempt.identity {
		w.failAttempt(attempt, errors.New("assignment confirmation identity mismatch"))
		return
	}
	w.attemptsMu.Lock()
	if attempt.confirmed || attempt.finished {
		w.attemptsMu.Unlock()
		return
	}
	attempt.confirmed = true
	attempt.leaseExpiresAt = confirmed.LeaseExpiresAt
	w.inflight++
	w.attemptsMu.Unlock()
	if err := w.persistState(); err != nil {
		w.failAttempt(attempt, err)
		return
	}
	w.metrics.runtime.recordExecution(attempt.identity.AttemptID)
	go w.executeAttempt(ctx, attempt)
}

func (w *runtimeWorker) executeAttempt(ctx context.Context, attempt *runtimeAttempt) {
	defer close(attempt.done)
	defer func() {
		w.attemptsMu.Lock()
		if w.inflight > 0 {
			w.inflight--
		}
		w.attemptsMu.Unlock()
	}()

	if err := w.renewAttempt(ctx, attempt); err != nil {
		if !errors.Is(err, context.Canceled) {
			w.failAttempt(attempt, err)
		}
		return
	}
	if w.cfg.StaleFenceProbes > 0 {
		for i := 0; i < w.cfg.StaleFenceProbes; i++ {
			if err := w.probeStaleFence(ctx, attempt); err != nil {
				w.failAttempt(attempt, err)
				return
			}
		}
	}

	remaining := w.cfg.ResultDelay
	for remaining > 0 {
		interval := w.renewInterval(attempt)
		if interval > remaining {
			interval = remaining
		}
		started := time.Now()
		if !sleepRuntimeContext(ctx, interval) {
			return
		}
		remaining -= time.Since(started)
		if remaining > 0 {
			if err := w.renewAttempt(ctx, attempt); err != nil {
				if !errors.Is(err, context.Canceled) {
					w.failAttempt(attempt, err)
				}
				return
			}
		}
	}
	for sequence := int64(1); sequence <= int64(w.cfg.EventsPerRun); sequence++ {
		if err := w.sendEvent(ctx, attempt, sequence); err != nil {
			if !errors.Is(err, context.Canceled) {
				w.failAttempt(attempt, err)
			}
			return
		}
	}
	if err := w.sendResult(ctx, attempt); err != nil {
		if !errors.Is(err, context.Canceled) {
			w.failAttempt(attempt, err)
		}
		return
	}
}

func (w *runtimeWorker) renewInterval(attempt *runtimeAttempt) time.Duration {
	w.attemptsMu.Lock()
	expiresAt := attempt.leaseExpiresAt
	w.attemptsMu.Unlock()
	remaining := time.Until(expiresAt)
	if remaining <= 0 {
		return time.Millisecond
	}
	interval := remaining / 3
	if interval < 250*time.Millisecond {
		interval = 250 * time.Millisecond
	}
	return interval
}

func (w *runtimeWorker) renewAttempt(ctx context.Context, attempt *runtimeAttempt) error {
	return w.withActiveClient(ctx, func(connection *runtimeConnection) error {
		w.attemptsMu.Lock()
		lastAckedEvent := attempt.lastAckedEvent
		w.attemptsMu.Unlock()
		started := time.Now()
		response, err := connection.client.RenewRuntimeLease(ctx, openlinker.RuntimeLeaseRenewPayload{
			AttemptIdentity: attempt.identity, LastClientEventSeq: lastAckedEvent,
			Capacity: int64(w.cfg.NodeCapacity), Inflight: w.activeAttemptCount(),
		})
		if err != nil {
			return err
		}
		if response == nil || response.AttemptIdentity != attempt.identity {
			return errors.New("lease renewal identity mismatch")
		}
		w.attemptsMu.Lock()
		attempt.leaseExpiresAt = response.LeaseExpiresAt
		w.attemptsMu.Unlock()
		w.metrics.runtime.mu.Lock()
		stats := w.metrics.runtime.transport(connection.kind)
		stats.LeaseRenews++
		stats.RenewMS = append(stats.RenewMS, ms(time.Since(started)))
		w.metrics.runtime.mu.Unlock()
		if response.PendingCommand != nil {
			command, decodeErr := response.PendingCommand.Decode()
			if decodeErr != nil {
				return decodeErr
			}
			go w.handleCommand(ctx, connection, command)
		}
		return nil
	})
}

func (w *runtimeWorker) probeStaleFence(ctx context.Context, attempt *runtimeAttempt) error {
	identity := attempt.identity
	identity.FencingToken++
	w.attemptsMu.Lock()
	lastAckedEvent := attempt.lastAckedEvent
	w.attemptsMu.Unlock()
	connection := w.currentConnection()
	if connection == nil {
		return errors.New("no active Runtime connection for stale-fence probe")
	}
	_, err := connection.client.RenewRuntimeLease(ctx, openlinker.RuntimeLeaseRenewPayload{
		AttemptIdentity: identity, LastClientEventSeq: lastAckedEvent,
		Capacity: int64(w.cfg.NodeCapacity), Inflight: w.activeAttemptCount(),
	})
	if err == nil {
		w.metrics.runtime.mu.Lock()
		w.metrics.runtime.staleFenceAccepts++
		w.metrics.runtime.mu.Unlock()
		return errors.New("Core accepted a stale fencing token")
	}
	code := w.metrics.runtime.recordError(err, connection.kind)
	if code != "STALE_FENCE" && code != "STALE_LEASE" && code != "ATTEMPT_IDENTITY_MISMATCH" {
		return fmt.Errorf("stale-fence probe returned %s: %w", code, err)
	}
	w.metrics.runtime.mu.Lock()
	w.metrics.runtime.staleFenceRejects++
	w.metrics.runtime.mu.Unlock()
	return nil
}

func (w *runtimeWorker) sendEvent(ctx context.Context, attempt *runtimeAttempt, sequence int64) error {
	clientEventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(attempt.identity.AttemptID+fmt.Sprintf(":event:%d", sequence))).String()
	request := openlinker.RuntimeRunEventPayload{
		AttemptIdentity: attempt.identity,
		ClientEventID:   clientEventID, ClientEventSeq: sequence,
		EventType: "run.message.delta",
		Payload: map[string]any{
			"text":           fmt.Sprintf("worker %d progress %d for %s", w.workerIndex, sequence, attempt.clientID),
			"client_task_id": attempt.clientID,
		},
	}
	w.attemptsMu.Lock()
	attempt.pendingEvent = sequence
	attempt.pendingEventPayload = &request
	w.attemptsMu.Unlock()
	if err := w.persistState(); err != nil {
		return err
	}
	return w.withActiveClient(ctx, func(connection *runtimeConnection) error {
		started := time.Now()
		ack, err := connection.client.AppendRuntimeEvent(ctx, request)
		if err != nil {
			return err
		}
		if ack == nil || ack.ClientEventID != clientEventID || ack.ClientEventSeq != sequence {
			return errors.New("Event ACK identity mismatch")
		}
		w.attemptsMu.Lock()
		attempt.lastAckedEvent = sequence
		attempt.pendingEvent = 0
		attempt.pendingEventPayload = nil
		w.attemptsMu.Unlock()
		if err := w.persistState(); err != nil {
			return err
		}
		w.metrics.runtime.mu.Lock()
		stats := w.metrics.runtime.transport(connection.kind)
		stats.EventACKs++
		stats.EventACKMS = append(stats.EventACKMS, ms(time.Since(started)))
		if ack.Replayed {
			stats.EventACKReplays++
		}
		w.metrics.runtime.mu.Unlock()
		return nil
	})
}

func (w *runtimeWorker) sendResult(ctx context.Context, attempt *runtimeAttempt) error {
	resultID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(attempt.identity.AttemptID+":result")).String()
	w.attemptsMu.Lock()
	finalClientEventSeq := attempt.lastAckedEvent
	w.attemptsMu.Unlock()
	request := openlinker.RuntimeRunResultPayload{
		AttemptIdentity: attempt.identity, ResultID: resultID, Status: "success",
		Output: map[string]any{
			"ok": true, "agent_id": w.agent.ID, "client_task_id": attempt.clientID,
			"worker_index": w.workerIndex,
		},
		DurationMS: time.Since(attempt.startedAt).Milliseconds(), FinalClientEventSeq: finalClientEventSeq,
	}
	w.attemptsMu.Lock()
	attempt.pendingResult = resultID
	attempt.pendingResultPayload = &request
	w.attemptsMu.Unlock()
	if err := w.persistState(); err != nil {
		return err
	}
	return w.withActiveClient(ctx, func(connection *runtimeConnection) error {
		started := time.Now()
		ack, err := connection.client.FinalizeRuntimeResult(ctx, request)
		if err != nil {
			return err
		}
		if ack == nil || ack.ResultID != resultID {
			return errors.New("Result ACK identity mismatch")
		}
		w.attemptsMu.Lock()
		attempt.pendingResult = ""
		attempt.pendingResultPayload = nil
		attempt.finished = true
		w.attemptsMu.Unlock()
		if err := w.persistState(); err != nil {
			return err
		}
		w.metrics.runtime.mu.Lock()
		stats := w.metrics.runtime.transport(connection.kind)
		stats.ResultACKs++
		stats.ResultACKMS = append(stats.ResultACKMS, ms(time.Since(started)))
		if ack.Replayed {
			stats.ResultACKReplays++
		}
		if w.cfg.hasScenario("redis-signal-outage") && connection.kind == transportPull && w.metrics.runtime.redisOutageObserved {
			w.metrics.runtime.dbPollingCompletions++
		}
		w.metrics.runtime.mu.Unlock()
		w.tracker.markCompleted(attempt.identity.RunID, time.Now(), "")
		w.tracker.markOutcome(attempt.identity.RunID, "success")
		return nil
	})
}

func (w *runtimeWorker) withActiveClient(
	ctx context.Context,
	call func(*runtimeConnection) error,
) error {
	for ctx.Err() == nil {
		connection := w.currentConnection()
		if connection == nil {
			if !sleepRuntimeContext(ctx, 25*time.Millisecond) {
				return ctx.Err()
			}
			continue
		}
		err := call(connection)
		if err == nil {
			return nil
		}
		code := w.metrics.runtime.recordError(err, connection.kind)
		if errors.Is(err, errACKResponseLost) {
			continue
		}
		if runtimeErrorPermanent(code) {
			return err
		}
		w.signalFailure(err)
		if !sleepRuntimeContext(ctx, 50*time.Millisecond) {
			return ctx.Err()
		}
	}
	return ctx.Err()
}

func runtimeErrorPermanent(code string) bool {
	switch code {
	case "STALE_FENCE", "STALE_LEASE", "LEASE_EXPIRED", "RUN_CANCEL_REQUESTED",
		"RUNTIME_CONTRACT_MISMATCH", "RUNTIME_CLIENT_UPGRADE_REQUIRED",
		"RUNTIME_REQUIRED_FEATURE_MISSING", "ATTEMPT_IDENTITY_MISMATCH",
		"AGENT_TOKEN_INVALID", "NODE_DEVICE_INVALID":
		return true
	default:
		return false
	}
}

func (w *runtimeWorker) signalFailure(err error) {
	select {
	case w.failure <- err:
	default:
	}
}

func (w *runtimeWorker) failAttempt(attempt *runtimeAttempt, err error) {
	w.metrics.recordWorkerError(err)
	w.attemptsMu.Lock()
	attempt.finished = true
	attempt.pendingEvent = 0
	attempt.pendingEventPayload = nil
	attempt.pendingResult = ""
	attempt.pendingResultPayload = nil
	w.attemptsMu.Unlock()
	if persistErr := w.persistState(); persistErr != nil {
		w.metrics.recordWorkerError(persistErr)
	}
	w.tracker.markCompleted(attempt.identity.RunID, time.Now(), err.Error())
	w.tracker.markOutcome(attempt.identity.RunID, "failed")
}

func (w *runtimeWorker) activeAttemptCount() int64 {
	w.attemptsMu.Lock()
	defer w.attemptsMu.Unlock()
	return w.inflight
}

func (w *runtimeWorker) handleCommand(
	ctx context.Context,
	connection *runtimeConnection,
	command openlinker.RuntimeDecodedPendingCommand,
) {
	switch command.Type {
	case openlinker.RuntimeRunCancel:
		if command.Cancel != nil {
			w.handleCancel(ctx, connection, *command.Cancel)
		}
	case openlinker.RuntimeDrain:
		w.signalFailure(errors.New("Runtime Node drain requested"))
	case openlinker.RuntimeLeaseRevoked:
		if command.Revoke != nil {
			w.attemptsMu.Lock()
			attempt := w.attempts[command.Revoke.AttemptIdentity.AttemptID]
			if attempt != nil && !attempt.finished {
				attempt.canceled = true
				attempt.finished = true
				attempt.cancel()
			}
			w.attemptsMu.Unlock()
		}
	}
}

func (w *runtimeWorker) handleCancel(
	ctx context.Context,
	connection *runtimeConnection,
	command openlinker.RuntimeRunCancelPayload,
) {
	commandAt := time.Now()
	w.metrics.runtime.mu.Lock()
	stats := w.metrics.runtime.transport(connection.kind)
	stats.CancelCommands++
	w.metrics.runtime.mu.Unlock()
	w.tracker.markCancelCommand(command.AttemptIdentity.RunID, commandAt)

	w.attemptsMu.Lock()
	attempt := w.attempts[command.AttemptIdentity.AttemptID]
	if attempt == nil || attempt.identity != command.AttemptIdentity {
		w.attemptsMu.Unlock()
		_ = w.ackCancel(ctx, connection, command, openlinker.RuntimeCancelFailed, "ATTEMPT_IDENTITY_MISMATCH")
		return
	}
	if attempt.finished && !attempt.canceled {
		w.attemptsMu.Unlock()
		return
	}
	if attempt.canceled {
		done := attempt.done
		w.attemptsMu.Unlock()
		state := openlinker.RuntimeCancelStopping
		select {
		case <-done:
			state = openlinker.RuntimeCancelStopped
		default:
		}
		_ = w.ackCancel(ctx, connection, command, state, "")
		return
	}
	attempt.canceled = true
	w.attemptsMu.Unlock()
	if err := w.persistState(); err != nil {
		w.metrics.recordWorkerError(err)
		return
	}

	if err := w.ackCancel(ctx, connection, command, openlinker.RuntimeCancelStopping, ""); err != nil {
		w.metrics.recordWorkerError(err)
		return
	}
	attempt.cancel()
	deadline := command.DeadlineAt
	if deadline.IsZero() {
		deadline = time.Now().Add(30 * time.Second)
	}
	timer := time.NewTimer(time.Until(deadline))
	select {
	case <-attempt.done:
		timer.Stop()
	case <-timer.C:
		_ = w.ackCancel(ctx, connection, command, openlinker.RuntimeCancelFailed, "CANCEL_DEADLINE_EXCEEDED")
		return
	case <-ctx.Done():
		timer.Stop()
		return
	}
	if err := w.ackCancel(ctx, connection, command, openlinker.RuntimeCancelStopped, ""); err != nil {
		w.metrics.recordWorkerError(err)
		return
	}
	w.attemptsMu.Lock()
	attempt.finished = true
	attempt.pendingEvent = 0
	attempt.pendingEventPayload = nil
	attempt.pendingResult = ""
	attempt.pendingResultPayload = nil
	w.attemptsMu.Unlock()
	if err := w.persistState(); err != nil {
		w.metrics.recordWorkerError(err)
		return
	}
	ackAt := time.Now()
	w.tracker.markCancelAck(command.AttemptIdentity.RunID, ackAt)
	w.tracker.markCompleted(command.AttemptIdentity.RunID, ackAt, "")
	w.tracker.markOutcome(command.AttemptIdentity.RunID, "canceled")
	w.metrics.runtime.mu.Lock()
	w.metrics.runtime.cancelCommandACKs++
	if requestedAt := w.tracker.cancelRequestedAt(command.AttemptIdentity.RunID); !requestedAt.IsZero() {
		stats := w.metrics.runtime.transport(connection.kind)
		stats.CancelEndToEndMS = append(stats.CancelEndToEndMS, ms(ackAt.Sub(requestedAt)))
	}
	w.metrics.runtime.mu.Unlock()
}

func (w *runtimeWorker) ackCancel(
	ctx context.Context,
	connection *runtimeConnection,
	command openlinker.RuntimeRunCancelPayload,
	state openlinker.RuntimeCancelState,
	errorCode string,
) error {
	request := openlinker.RuntimeRunCancelAckPayload{
		CancellationID: command.CancellationID, AttemptIdentity: command.AttemptIdentity,
		CancelState: state, ErrorCode: errorCode,
	}
	for ctx.Err() == nil {
		started := time.Now()
		response, err := connection.client.AckRuntimeCancel(ctx, request)
		if err != nil {
			w.metrics.runtime.recordError(err, connection.kind)
			if errors.Is(err, errACKResponseLost) {
				continue
			}
			return err
		}
		if response == nil || response.CancellationID != command.CancellationID {
			return errors.New("Cancel ACK identity mismatch")
		}
		w.metrics.runtime.mu.Lock()
		stats := w.metrics.runtime.transport(connection.kind)
		stats.CancelACKs++
		stats.CancelACKMS = append(stats.CancelACKMS, ms(time.Since(started)))
		w.metrics.runtime.mu.Unlock()
		return nil
	}
	return ctx.Err()
}

func (w *runtimeWorker) resume(ctx context.Context, connection *runtimeConnection) error {
	attempts := w.resumeAttempts()
	if len(attempts) == 0 {
		return nil
	}
	started := time.Now()
	response, err := connection.client.ResumeRuntimeRuns(ctx, openlinker.RuntimeResumePayload{
		NodeID: w.hello.NodeID, AgentID: w.hello.AgentID, WorkerID: w.hello.WorkerID,
		RuntimeSessionID: w.hello.RuntimeSessionID, Attempts: attempts,
	})
	if err != nil {
		return err
	}
	if response == nil || len(response.Decisions) != len(attempts) {
		return errors.New("Runtime resume decision count mismatch")
	}
	for index, decision := range response.Decisions {
		if decision.AttemptIdentity != attempts[index].AttemptIdentity {
			return errors.New("Runtime resume decision identity mismatch")
		}
		switch decision.Decision {
		case openlinker.RuntimeResumeContinue, openlinker.RuntimeResumeUploadSpool:
			if decision.LeaseExpiresAt != nil {
				w.attemptsMu.Lock()
				if attempt := w.attempts[decision.AttemptIdentity.AttemptID]; attempt != nil {
					attempt.leaseExpiresAt = *decision.LeaseExpiresAt
				}
				w.attemptsMu.Unlock()
			}
		case openlinker.RuntimeResumeResultAcked:
			w.attemptsMu.Lock()
			if attempt := w.attempts[decision.AttemptIdentity.AttemptID]; attempt != nil {
				attempt.pendingResult = ""
				attempt.pendingResultPayload = nil
				attempt.finished = true
				attempt.cancel()
				w.tracker.markCompleted(attempt.identity.RunID, time.Now(), "")
				w.tracker.markOutcome(attempt.identity.RunID, "success")
			}
			w.attemptsMu.Unlock()
		case openlinker.RuntimeResumeRevoked:
			w.attemptsMu.Lock()
			if attempt := w.attempts[decision.AttemptIdentity.AttemptID]; attempt != nil {
				attempt.canceled = true
				attempt.finished = true
				attempt.pendingEvent = 0
				attempt.pendingEventPayload = nil
				attempt.pendingResult = ""
				attempt.pendingResultPayload = nil
				attempt.cancel()
			}
			w.attemptsMu.Unlock()
		default:
			return fmt.Errorf("unsupported Runtime resume decision %q", decision.Decision)
		}
	}
	if err := w.persistState(); err != nil {
		return err
	}
	w.metrics.runtime.recordResume(time.Since(started), len(response.Decisions))
	return nil
}

func (w *runtimeWorker) resumeAttempts() []openlinker.RuntimeResumeAttempt {
	w.attemptsMu.Lock()
	defer w.attemptsMu.Unlock()
	keys := make([]string, 0, len(w.attempts))
	for key, attempt := range w.attempts {
		if attempt.confirmed && !attempt.finished {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out := make([]openlinker.RuntimeResumeAttempt, 0, len(keys))
	for _, key := range keys {
		attempt := w.attempts[key]
		item := openlinker.RuntimeResumeAttempt{
			AttemptIdentity: attempt.identity, LastAckedClientEventSeq: attempt.lastAckedEvent,
			PendingResultID: attempt.pendingResult,
		}
		if attempt.pendingEvent > 0 {
			item.PendingClientEventRanges = []openlinker.RuntimeEventRange{{Start: attempt.pendingEvent, End: attempt.pendingEvent}}
		} else {
			item.PendingClientEventRanges = []openlinker.RuntimeEventRange{}
		}
		if attempt.pendingResult != "" {
			final := attempt.lastAckedEvent
			item.FinalClientEventSeq = &final
		}
		out = append(out, item)
	}
	return out
}

func driveRuntimeCancellations(
	ctx context.Context,
	api *apiClient,
	cfg config,
	accounts []account,
	tracker *runTracker,
	metrics *metrics,
) error {
	if cfg.CancelCount <= 0 {
		return nil
	}
	tokens := make(map[string]string, len(accounts))
	for _, account := range accounts {
		tokens[account.User.ID] = account.JWT
	}
	records := tracker.snapshot()
	targets := make([]runRecord, 0, cfg.CancelCount)
	for _, record := range records {
		if record.Phase == "measured" && record.RunID != "" {
			targets = append(targets, *record)
			if len(targets) == cfg.CancelCount {
				break
			}
		}
	}
	if len(targets) != cfg.CancelCount {
		return fmt.Errorf("cancel driver found %d measured Runs, want %d", len(targets), cfg.CancelCount)
	}
	jobs := make(chan runRecord)
	var workers sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	setErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}
	concurrency := min(cfg.CancelConcurrency, len(targets))
	for i := 0; i < concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for target := range jobs {
				current, err := waitForRuntimeAssignment(ctx, tracker, target.RunID)
				if err != nil {
					setErr(err)
					continue
				}
				if cfg.CancelDelay > 0 {
					if err = waitRuntimeUntil(ctx, current.AssignedAt.Add(cfg.CancelDelay)); err != nil {
						setErr(err)
						continue
					}
				}
				token := tokens[target.UserID]
				if token == "" {
					setErr(fmt.Errorf("cancel Run %s: owner token unavailable", target.RunID))
					continue
				}
				requestedAt := time.Now()
				tracker.markCancelRequested(target.RunID, requestedAt)
				metrics.runtime.mu.Lock()
				metrics.runtime.cancelRequests++
				metrics.runtime.mu.Unlock()
				status, err := api.do(ctx, "cancel-run-v2-race", http.MethodPost, "/runs/"+target.RunID+"/cancel", map[string]any{}, token, nil)
				if err != nil {
					if status == http.StatusConflict || status == http.StatusBadRequest {
						metrics.runtime.mu.Lock()
						metrics.runtime.errorsByCode["CANCEL_RACE_LOST_TO_RESULT"]++
						metrics.runtime.mu.Unlock()
						continue
					}
					setErr(fmt.Errorf("cancel Run %s: %w", target.RunID, err))
					continue
				}
				metrics.runtime.mu.Lock()
				metrics.runtime.cancelAccepted++
				metrics.runtime.mu.Unlock()
			}
		}()
	}
	for _, target := range targets {
		select {
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return ctx.Err()
		case jobs <- target:
		}
	}
	close(jobs)
	workers.Wait()
	errMu.Lock()
	defer errMu.Unlock()
	return firstErr
}

func waitForRuntimeAssignment(ctx context.Context, tracker *runTracker, runID string) (runRecord, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if record, ok := tracker.runSnapshot(runID); ok {
			if !record.AssignedAt.IsZero() || !record.CompletedAt.IsZero() {
				return record, nil
			}
		}
		select {
		case <-ctx.Done():
			return runRecord{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitRuntimeUntil(ctx context.Context, at time.Time) error {
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

func waitForRedisSignalOutage(
	ctx context.Context,
	api *apiClient,
	timeout time.Duration,
	metrics *runtimeMetrics,
) error {
	if api == nil || api.client == nil || metrics == nil {
		return errors.New("Redis outage observation requires the public Core API client")
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	healthRoot := strings.TrimSuffix(strings.TrimRight(api.root, "/"), "/api/v1")
	readyURL := healthRoot + "/readyz"
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, readyURL, nil)
		if err == nil {
			var response *http.Response
			response, err = api.client.Do(request)
			if err == nil {
				var readiness struct {
					Ready   bool     `json:"ready"`
					Reasons []string `json:"reasons"`
				}
				decodeErr := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&readiness)
				_ = response.Body.Close()
				if decodeErr == nil && !readiness.Ready && containsString(readiness.Reasons, "signal_dependency_unavailable") {
					cancel()
					metrics.mu.Lock()
					metrics.redisOutageObserved = true
					metrics.mu.Unlock()
					return nil
				}
			}
		}
		cancel()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("Redis signal outage was not proven by %s within %s", readyURL, timeout)
		case <-ticker.C:
		}
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
