package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/google/uuid"
)

func TestRuntimeDuplicateAssignmentExecutesExactlyOnce(t *testing.T) {
	fixture := newRuntimeWorkerTestFixture(t)
	fixture.worker.cfg.DuplicateAssignments = 2
	fixture.worker.handleAssignment(context.Background(), fixture.connection, fixture.assignment, false)
	waitForTestRunCompletion(t, fixture.tracker, fixture.assignment.AttemptIdentity.RunID)
	// A real redelivery is ACKed idempotently so Core can stop replaying it,
	// while the client-side duplicate injections intentionally have no second
	// wire correlation to acknowledge.
	fixture.worker.handleAssignment(context.Background(), fixture.connection, fixture.assignment, false)

	fixture.fake.mu.Lock()
	ackCalls := fixture.fake.assignmentACKCalls
	executionIdentities := append([]openlinker.RuntimeAttemptIdentity(nil), fixture.fake.executionIdentities...)
	fixture.fake.mu.Unlock()
	if ackCalls != 2 {
		t.Fatalf("assignment ACK calls = %d, want 2", ackCalls)
	}
	if len(executionIdentities) != 3 {
		t.Fatalf("renew/event/result identities = %d, want 3", len(executionIdentities))
	}
	for _, identity := range executionIdentities {
		if identity != fixture.assignment.AttemptIdentity {
			t.Fatalf("Attempt identity changed across protocol operations: %#v", identity)
		}
	}
	fixture.worker.cfg.Scenarios = []string{"duplicate-assignment"}
	report := fixture.metrics.runtime.report(fixture.worker.cfg)
	safety := report["safety"].(map[string]any)
	if got := safety["duplicate_assignments"]; got != 3 {
		t.Fatalf("duplicate assignments = %v, want 3", got)
	}
	if got := safety["duplicate_execution"]; got != 0 {
		t.Fatalf("duplicate execution = %v, want 0", got)
	}
	info, err := os.Stat(fixture.worker.journal.path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("journal mode = %o, want 600", info.Mode().Perm())
	}
	raw, err := os.ReadFile(fixture.worker.journal.path)
	if err != nil {
		t.Fatal(err)
	}
	var journal runtimeJournalDocument
	if err = json.Unmarshal(raw, &journal); err != nil {
		t.Fatal(err)
	}
	if len(journal.Attempts) != 1 || !journal.Attempts[0].Finished || journal.Attempts[0].PendingResult != "" {
		t.Fatalf("durable journal did not reach Result ACK state: %#v", journal.Attempts)
	}
}

func TestRuntimeAssignmentACKResponseLossRetriesSameIdentity(t *testing.T) {
	fixture := newRuntimeWorkerTestFixture(t)
	fixture.fake.dropAssignmentACK = true
	fixture.worker.handleAssignment(context.Background(), fixture.connection, fixture.assignment, false)
	waitForTestRunCompletion(t, fixture.tracker, fixture.assignment.AttemptIdentity.RunID)

	fixture.fake.mu.Lock()
	ackCalls := fixture.fake.assignmentACKCalls
	fixture.fake.mu.Unlock()
	if ackCalls != 2 {
		t.Fatalf("assignment ACK calls = %d, want 2", ackCalls)
	}
	fixture.worker.cfg.Scenarios = []string{"ack-response-loss"}
	stats := fixture.metrics.runtime.report(fixture.worker.cfg)["transports"].(map[string]any)[transportPull].(map[string]any)
	if got := stats["assignment_ack_recoveries"]; got != 1 {
		t.Fatalf("assignment ACK recoveries = %v, want 1", got)
	}
}

func TestRuntimeReportPinsPublishedContract(t *testing.T) {
	report := newRuntimeMetrics().report(config{Scenarios: []string{"baseline"}})
	if got := report["protocol_version"]; got != openlinker.RuntimeProtocolVersion {
		t.Fatalf("protocol version = %v", got)
	}
	if got := report["runtime_contract_id"]; got != "openlinker.runtime.v2" {
		t.Fatalf("contract ID = %v", got)
	}
	if got := report["runtime_contract_digest"]; got != "4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481" {
		t.Fatalf("contract digest = %v", got)
	}
}

func TestRuntimeCredentialPreflightBindsCertificateNodeAndKeyPermissions(t *testing.T) {
	nodeID := uuid.New()
	cert, key, ca := writeRuntimeTestCredentials(t, nodeID)
	cfg := config{
		NodeID: nodeID.String(), MTLSCertFile: cert, MTLSKeyFile: key, MTLSCAFile: ca,
		StateDir: filepath.Join(t.TempDir(), "state"),
	}
	if err := preflightRuntimeCredentials(cfg); err != nil {
		t.Fatalf("preflight valid credentials: %v", err)
	}
	cfg.NodeID = uuid.NewString()
	if err := preflightRuntimeCredentials(cfg); err == nil {
		t.Fatal("preflight accepted a Node ID that does not match the certificate")
	}
	cfg.NodeID = nodeID.String()
	if err := os.Chmod(key, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := preflightRuntimeCredentials(cfg); err == nil {
		t.Fatal("preflight accepted a group-readable private key")
	}
}

func TestCancelRaceScenarioRequiresOneExecutingWorkerPerCancellation(t *testing.T) {
	cfg := config{
		Transport: transportPull, Scenarios: []string{"cancel-race"},
		Agents: 100, WorkersPerAgent: 10, Runs: 1000,
	}
	if err := cfg.applyScenarioDefaults(); err != nil {
		t.Fatalf("applyScenarioDefaults: %v", err)
	}
	if cfg.CancelCount != 1000 || cfg.ResultDelay != 10*time.Second || cfg.CancelDelay != cfg.ResultDelay {
		t.Fatalf("cancel-race defaults = count:%d result:%s cancel:%s", cfg.CancelCount, cfg.ResultDelay, cfg.CancelDelay)
	}

	cfg.Agents = 99
	if err := cfg.applyScenarioDefaults(); err == nil {
		t.Fatal("cancel-race accepted fewer executing workers than cancellations")
	}
}

func TestRuntimeScenarioTransportConstraintsFailClosed(t *testing.T) {
	tests := []config{
		{Transport: transportWS, Scenarios: []string{"ack-response-loss"}},
		{Transport: transportAuto, Scenarios: []string{"core-a-b-resume"}, SwitchAfter: time.Second},
		{Transport: transportAuto, Scenarios: []string{"ws-pull-ws"}, SwitchAfter: 2 * time.Second, SwitchBackAfter: time.Second},
		{Transport: transportAuto, Scenarios: []string{"redis-signal-outage"}},
	}
	for _, cfg := range tests {
		if err := cfg.applyScenarioDefaults(); err == nil {
			t.Fatalf("scenario %#v did not fail closed", cfg.Scenarios)
		}
	}
}

type runtimeWorkerTestFixture struct {
	worker     *runtimeWorker
	connection *runtimeConnection
	assignment openlinker.RuntimeRunAssignedPayload
	fake       *fakeRuntimeClient
	tracker    *runTracker
	metrics    *metrics
}

func newRuntimeWorkerTestFixture(t *testing.T) runtimeWorkerTestFixture {
	t.Helper()
	now := time.Now().UTC()
	identity := openlinker.RuntimeAttemptIdentity{
		RunID: uuid.NewString(), AttemptID: uuid.NewString(), LeaseID: uuid.NewString(),
		FencingToken: 7, NodeID: uuid.NewString(), AgentID: uuid.NewString(),
		WorkerID: "worker-test", RuntimeSessionID: uuid.NewString(),
	}
	clientID := "client-test"
	tracker := newRunTracker()
	tracker.upsertSubmitted(clientID, identity.AgentID, uuid.NewString(), "", "measured", now.Add(-time.Second))
	tracker.markCreated(clientID, identity.RunID, now.Add(-900*time.Millisecond), "")
	metrics := &metrics{startedAt: now, runtime: newRuntimeMetrics()}
	fake := &fakeRuntimeClient{leaseExpiresAt: now.Add(time.Minute)}
	connection := &runtimeConnection{kind: transportPull, client: fake, generation: 1}
	worker := &runtimeWorker{
		cfg:   config{NodeCapacity: 1, EventsPerRun: 1},
		agent: agentRef{ID: identity.AgentID}, workerIndex: 0, tracker: tracker, metrics: metrics,
		hello: openlinker.RuntimeHelloPayload{
			NodeID: identity.NodeID, AgentID: identity.AgentID, WorkerID: identity.WorkerID,
			RuntimeSessionID: identity.RuntimeSessionID, SessionEpoch: 1,
		},
		failure: make(chan error, 1), attempts: map[string]*runtimeAttempt{},
		journal: &runtimeJournal{path: filepath.Join(t.TempDir(), "worker.json")},
	}
	worker.publishConnection(connection)
	return runtimeWorkerTestFixture{
		worker: worker, connection: connection, fake: fake, tracker: tracker, metrics: metrics,
		assignment: openlinker.RuntimeRunAssignedPayload{
			AttemptIdentity: identity, OfferNo: 1, OfferExpiresAt: now.Add(time.Minute),
			AttemptDeadlineAt: now.Add(time.Minute), RunDeadlineAt: now.Add(time.Minute),
			Input:        map[string]any{"client_task_id": clientID},
			NodeEnvelope: "ol_ctx_v2.test", AgentInvocationToken: "ol_inv_v2.test",
		},
	}
}

func waitForTestRunCompletion(t *testing.T, tracker *runTracker, runID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if record, ok := tracker.runSnapshot(runID); ok && !record.CompletedAt.IsZero() {
			if record.ResultErr != "" {
				t.Fatalf("Run failed: %s", record.ResultErr)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for test Run completion")
}

type fakeRuntimeClient struct {
	mu sync.Mutex

	leaseExpiresAt      time.Time
	assignmentACKCalls  int
	dropAssignmentACK   bool
	executionIdentities []openlinker.RuntimeAttemptIdentity
}

func (f *fakeRuntimeClient) CreateRuntimeSession(context.Context, openlinker.RuntimeHelloPayload) (*openlinker.RuntimeReadyPayload, error) {
	return nil, errors.New("unused")
}
func (f *fakeRuntimeClient) HeartbeatRuntimeSession(context.Context, openlinker.RuntimeHelloPayload) (*openlinker.RuntimeReadyPayload, error) {
	return nil, errors.New("unused")
}
func (f *fakeRuntimeClient) CloseRuntimeSession(context.Context, openlinker.RuntimeSessionCloseRequest) error {
	return nil
}
func (f *fakeRuntimeClient) ClaimRuntimeRun(context.Context, int, openlinker.RuntimeClaimRequest) (*openlinker.RuntimeRunAssignedPayload, error) {
	return nil, errors.New("unused")
}
func (f *fakeRuntimeClient) AckRuntimeAssignment(_ context.Context, request openlinker.RuntimeAssignmentAckPayload) (*openlinker.RuntimeAssignmentConfirmedPayload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.assignmentACKCalls++
	if f.dropAssignmentACK {
		f.dropAssignmentACK = false
		return nil, errACKResponseLost
	}
	return &openlinker.RuntimeAssignmentConfirmedPayload{
		AttemptIdentity: request.AttemptIdentity, AttemptNo: 1, LeaseExpiresAt: f.leaseExpiresAt,
	}, nil
}
func (f *fakeRuntimeClient) RenewRuntimeLease(_ context.Context, request openlinker.RuntimeLeaseRenewPayload) (*openlinker.RuntimeLeaseRenewedPayload, error) {
	f.mu.Lock()
	f.executionIdentities = append(f.executionIdentities, request.AttemptIdentity)
	f.mu.Unlock()
	return &openlinker.RuntimeLeaseRenewedPayload{AttemptIdentity: request.AttemptIdentity, LeaseExpiresAt: f.leaseExpiresAt}, nil
}
func (f *fakeRuntimeClient) AppendRuntimeEvent(_ context.Context, request openlinker.RuntimeRunEventPayload) (*openlinker.RuntimeRunEventAckPayload, error) {
	f.mu.Lock()
	f.executionIdentities = append(f.executionIdentities, request.AttemptIdentity)
	f.mu.Unlock()
	return &openlinker.RuntimeRunEventAckPayload{ClientEventID: request.ClientEventID, ClientEventSeq: request.ClientEventSeq, Sequence: request.ClientEventSeq}, nil
}
func (f *fakeRuntimeClient) FinalizeRuntimeResult(_ context.Context, request openlinker.RuntimeRunResultPayload) (*openlinker.RuntimeRunResultAckPayload, error) {
	f.mu.Lock()
	f.executionIdentities = append(f.executionIdentities, request.AttemptIdentity)
	f.mu.Unlock()
	return &openlinker.RuntimeRunResultAckPayload{
		ResultID: request.ResultID, Classification: openlinker.RuntimeResultSuccess,
		RunStatus: openlinker.RuntimeRunSuccess, DispatchState: openlinker.RuntimeDispatchTerminal,
	}, nil
}
func (f *fakeRuntimeClient) ResumeRuntimeRuns(context.Context, openlinker.RuntimeResumePayload) (*openlinker.RuntimeResumeResponse, error) {
	return &openlinker.RuntimeResumeResponse{Decisions: []openlinker.RuntimeResumeAcceptedPayload{}}, nil
}
func (f *fakeRuntimeClient) PollRuntimeCommands(context.Context, string, int) (*openlinker.RuntimeCommandsResponse, error) {
	return &openlinker.RuntimeCommandsResponse{Commands: []openlinker.RuntimePendingCommand{}, DatabaseTime: time.Now()}, nil
}
func (f *fakeRuntimeClient) AckRuntimeCancel(_ context.Context, request openlinker.RuntimeRunCancelAckPayload) (*openlinker.RuntimeRunCancellationState, error) {
	return &openlinker.RuntimeRunCancellationState{
		CancellationID: request.CancellationID, CancelState: request.CancelState, UpdatedAt: time.Now(),
	}, nil
}

var _ runtimeClient = (*fakeRuntimeClient)(nil)

func writeRuntimeTestCredentials(t *testing.T, nodeID uuid.UUID) (string, string, string) {
	t.Helper()
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Runtime test CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := url.Parse("urn:openlinker:runtime-node:" + nodeID.String())
	if err != nil {
		t.Fatal(err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "Runtime test Node"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, URIs: []*url.URL{identity},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	certPath := filepath.Join(directory, "node.crt")
	keyPath := filepath.Join(directory, "node.key")
	caPath := filepath.Join(directory, "server-ca.crt")
	if err = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}), 0o644); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o644); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath, caPath
}
