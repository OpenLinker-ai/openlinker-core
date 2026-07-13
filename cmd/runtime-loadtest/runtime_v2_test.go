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

func TestRuntimeV2DuplicateAssignmentExecutesExactlyOnce(t *testing.T) {
	fixture := newRuntimeV2WorkerTestFixture(t)
	fixture.worker.cfg.DuplicateAssignments = 2
	fixture.worker.handleAssignment(context.Background(), fixture.connection, fixture.assignment, false)
	waitForTestRunCompletion(t, fixture.tracker, fixture.assignment.AttemptIdentity.RunID)
	// A real redelivery is ACKed idempotently so Core can stop replaying it,
	// while the client-side duplicate injections intentionally have no second
	// wire correlation to acknowledge.
	fixture.worker.handleAssignment(context.Background(), fixture.connection, fixture.assignment, false)

	fixture.fake.mu.Lock()
	ackCalls := fixture.fake.assignmentACKCalls
	executionIdentities := append([]openlinker.RuntimeV2AttemptIdentity(nil), fixture.fake.executionIdentities...)
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
	var journal runtimeV2JournalDocument
	if err = json.Unmarshal(raw, &journal); err != nil {
		t.Fatal(err)
	}
	if len(journal.Attempts) != 1 || !journal.Attempts[0].Finished || journal.Attempts[0].PendingResult != "" {
		t.Fatalf("durable journal did not reach Result ACK state: %#v", journal.Attempts)
	}
}

func TestRuntimeV2AssignmentACKResponseLossRetriesSameIdentity(t *testing.T) {
	fixture := newRuntimeV2WorkerTestFixture(t)
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

func TestRuntimeV2ReportPinsPublishedContract(t *testing.T) {
	report := newRuntimeV2Metrics().report(config{Scenarios: []string{"baseline"}})
	if got := report["protocol_version"]; got != openlinker.RuntimeProtocolVersion {
		t.Fatalf("protocol version = %v", got)
	}
	if got := report["runtime_contract_id"]; got != "openlinker.runtime.v2" {
		t.Fatalf("contract ID = %v", got)
	}
	if got := report["runtime_contract_digest"]; got != "fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53" {
		t.Fatalf("contract digest = %v", got)
	}
}

func TestRuntimeV2CredentialPreflightBindsCertificateNodeAndKeyPermissions(t *testing.T) {
	nodeID := uuid.New()
	cert, key, ca := writeRuntimeV2TestCredentials(t, nodeID)
	cfg := config{
		NodeID: nodeID.String(), MTLSCertFile: cert, MTLSKeyFile: key, MTLSCAFile: ca,
		StateDir: filepath.Join(t.TempDir(), "state"),
	}
	if err := preflightRuntimeV2Credentials(cfg); err != nil {
		t.Fatalf("preflight valid credentials: %v", err)
	}
	cfg.NodeID = uuid.NewString()
	if err := preflightRuntimeV2Credentials(cfg); err == nil {
		t.Fatal("preflight accepted a Node ID that does not match the certificate")
	}
	cfg.NodeID = nodeID.String()
	if err := os.Chmod(key, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := preflightRuntimeV2Credentials(cfg); err == nil {
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

func TestRuntimeV2ScenarioTransportConstraintsFailClosed(t *testing.T) {
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

type runtimeV2WorkerTestFixture struct {
	worker     *runtimeV2Worker
	connection *runtimeV2Connection
	assignment openlinker.RuntimeV2RunAssignedPayload
	fake       *fakeRuntimeV2Client
	tracker    *runTracker
	metrics    *metrics
}

func newRuntimeV2WorkerTestFixture(t *testing.T) runtimeV2WorkerTestFixture {
	t.Helper()
	now := time.Now().UTC()
	identity := openlinker.RuntimeV2AttemptIdentity{
		RunID: uuid.NewString(), AttemptID: uuid.NewString(), LeaseID: uuid.NewString(),
		FencingToken: 7, NodeID: uuid.NewString(), AgentID: uuid.NewString(),
		WorkerID: "worker-test", RuntimeSessionID: uuid.NewString(),
	}
	clientID := "client-test"
	tracker := newRunTracker()
	tracker.upsertSubmitted(clientID, identity.AgentID, uuid.NewString(), "", "measured", now.Add(-time.Second))
	tracker.markCreated(clientID, identity.RunID, now.Add(-900*time.Millisecond), "")
	metrics := &metrics{startedAt: now, runtime: newRuntimeV2Metrics()}
	fake := &fakeRuntimeV2Client{leaseExpiresAt: now.Add(time.Minute)}
	connection := &runtimeV2Connection{kind: transportPull, client: fake, generation: 1}
	worker := &runtimeV2Worker{
		cfg:   config{NodeCapacity: 1, EventsPerRun: 1},
		agent: agentRef{ID: identity.AgentID}, workerIndex: 0, tracker: tracker, metrics: metrics,
		hello: openlinker.RuntimeV2HelloPayload{
			NodeID: identity.NodeID, AgentID: identity.AgentID, WorkerID: identity.WorkerID,
			RuntimeSessionID: identity.RuntimeSessionID, SessionEpoch: 1,
		},
		failure: make(chan error, 1), attempts: map[string]*runtimeV2Attempt{},
		journal: &runtimeV2Journal{path: filepath.Join(t.TempDir(), "worker.json")},
	}
	worker.publishConnection(connection)
	return runtimeV2WorkerTestFixture{
		worker: worker, connection: connection, fake: fake, tracker: tracker, metrics: metrics,
		assignment: openlinker.RuntimeV2RunAssignedPayload{
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

type fakeRuntimeV2Client struct {
	mu sync.Mutex

	leaseExpiresAt      time.Time
	assignmentACKCalls  int
	dropAssignmentACK   bool
	executionIdentities []openlinker.RuntimeV2AttemptIdentity
}

func (f *fakeRuntimeV2Client) CreateRuntimeV2Session(context.Context, openlinker.RuntimeV2HelloPayload) (*openlinker.RuntimeV2ReadyPayload, error) {
	return nil, errors.New("unused")
}
func (f *fakeRuntimeV2Client) HeartbeatRuntimeV2Session(context.Context, openlinker.RuntimeV2HelloPayload) (*openlinker.RuntimeV2ReadyPayload, error) {
	return nil, errors.New("unused")
}
func (f *fakeRuntimeV2Client) CloseRuntimeV2Session(context.Context, openlinker.RuntimeV2SessionCloseRequest) error {
	return nil
}
func (f *fakeRuntimeV2Client) ClaimRuntimeV2Run(context.Context, int, openlinker.RuntimeV2ClaimRequest) (*openlinker.RuntimeV2RunAssignedPayload, error) {
	return nil, errors.New("unused")
}
func (f *fakeRuntimeV2Client) AckRuntimeV2Assignment(_ context.Context, request openlinker.RuntimeV2AssignmentAckPayload) (*openlinker.RuntimeV2AssignmentConfirmedPayload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.assignmentACKCalls++
	if f.dropAssignmentACK {
		f.dropAssignmentACK = false
		return nil, errACKResponseLost
	}
	return &openlinker.RuntimeV2AssignmentConfirmedPayload{
		AttemptIdentity: request.AttemptIdentity, AttemptNo: 1, LeaseExpiresAt: f.leaseExpiresAt,
	}, nil
}
func (f *fakeRuntimeV2Client) RenewRuntimeV2Lease(_ context.Context, request openlinker.RuntimeV2LeaseRenewPayload) (*openlinker.RuntimeV2LeaseRenewedPayload, error) {
	f.mu.Lock()
	f.executionIdentities = append(f.executionIdentities, request.AttemptIdentity)
	f.mu.Unlock()
	return &openlinker.RuntimeV2LeaseRenewedPayload{AttemptIdentity: request.AttemptIdentity, LeaseExpiresAt: f.leaseExpiresAt}, nil
}
func (f *fakeRuntimeV2Client) AppendRuntimeV2Event(_ context.Context, request openlinker.RuntimeV2RunEventPayload) (*openlinker.RuntimeV2RunEventAckPayload, error) {
	f.mu.Lock()
	f.executionIdentities = append(f.executionIdentities, request.AttemptIdentity)
	f.mu.Unlock()
	return &openlinker.RuntimeV2RunEventAckPayload{ClientEventID: request.ClientEventID, ClientEventSeq: request.ClientEventSeq, Sequence: request.ClientEventSeq}, nil
}
func (f *fakeRuntimeV2Client) FinalizeRuntimeV2Result(_ context.Context, request openlinker.RuntimeV2RunResultPayload) (*openlinker.RuntimeV2RunResultAckPayload, error) {
	f.mu.Lock()
	f.executionIdentities = append(f.executionIdentities, request.AttemptIdentity)
	f.mu.Unlock()
	return &openlinker.RuntimeV2RunResultAckPayload{
		ResultID: request.ResultID, Classification: openlinker.RuntimeV2ResultSuccess,
		RunStatus: openlinker.RuntimeV2RunSuccess, DispatchState: openlinker.RuntimeV2DispatchTerminal,
	}, nil
}
func (f *fakeRuntimeV2Client) ResumeRuntimeV2Runs(context.Context, openlinker.RuntimeV2ResumePayload) (*openlinker.RuntimeV2ResumeResponse, error) {
	return &openlinker.RuntimeV2ResumeResponse{Decisions: []openlinker.RuntimeV2ResumeAcceptedPayload{}}, nil
}
func (f *fakeRuntimeV2Client) PollRuntimeV2Commands(context.Context, string, int) (*openlinker.RuntimeV2CommandsResponse, error) {
	return &openlinker.RuntimeV2CommandsResponse{Commands: []openlinker.RuntimeV2PendingCommand{}, DatabaseTime: time.Now()}, nil
}
func (f *fakeRuntimeV2Client) AckRuntimeV2Cancel(_ context.Context, request openlinker.RuntimeV2RunCancelAckPayload) (*openlinker.RuntimeV2RunCancellationState, error) {
	return &openlinker.RuntimeV2RunCancellationState{
		CancellationID: request.CancellationID, CancelState: request.CancelState, UpdatedAt: time.Now(),
	}, nil
}

var _ runtimeV2Client = (*fakeRuntimeV2Client)(nil)

func writeRuntimeV2TestCredentials(t *testing.T, nodeID uuid.UUID) (string, string, string) {
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
