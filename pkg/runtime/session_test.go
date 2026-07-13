package runtime

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"math/big"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestMTLSRuntimeDeviceAuthenticatorUsesOnlyVerifiedPeerCertificate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 3, 4, 5, 0, time.UTC)
	leaf := &x509.Certificate{
		SerialNumber:            big.NewInt(0xabc),
		Raw:                     []byte("verified-leaf-certificate"),
		RawSubjectPublicKeyInfo: []byte("verified-leaf-public-key"),
		NotBefore:               now.Add(-time.Hour),
		NotAfter:                now.Add(time.Hour),
	}
	presented := runtimePresentedCertificate(leaf)
	nodeID := uuid.New()
	verifier := &sessionCredentialVerifierFake{identity: RuntimeDeviceIdentity{
		NodeID:                       nodeID,
		CertificateSerial:            presented.Serial,
		CertificateFingerprintSHA256: presented.FingerprintSHA256,
		PublicKeyThumbprintSHA256:    presented.PublicKeyThumbprintSHA256,
	}}
	authenticator := NewMTLSRuntimeDeviceAuthenticator(verifier)
	req, err := http.NewRequest(http.MethodPost, "https://core.test/runtime", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Client-Cert", "spoofed")
	req.Header.Set("X-Node-ID", uuid.NewString())
	req.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leaf},
		VerifiedChains:   [][]*x509.Certificate{{leaf}},
	}

	identity, err := authenticator.AuthenticateHTTP(context.Background(), req)
	if err != nil {
		t.Fatalf("AuthenticateHTTP() error = %v", err)
	}
	if identity.NodeID != nodeID || verifier.calls != 1 || verifier.presented != presented {
		t.Fatalf("identity/verifier = %#v calls=%d presented=%#v", identity, verifier.calls, verifier.presented)
	}

	spoofOnly, _ := http.NewRequest(http.MethodPost, "https://core.test/runtime", nil)
	spoofOnly.Header.Set("X-Client-Cert", "spoofed")
	if _, err = authenticator.AuthenticateHTTP(context.Background(), spoofOnly); !IsRuntimeSessionError(err, RuntimeSessionErrorAuthenticationFailed) {
		t.Fatalf("spoof-only error = %v", err)
	}
	if verifier.calls != 1 {
		t.Fatalf("verifier called for spoofed header: %d", verifier.calls)
	}

	unverified := req.Clone(context.Background())
	unverified.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}
	if _, err = authenticator.AuthenticateHTTP(context.Background(), unverified); !IsRuntimeSessionError(err, RuntimeSessionErrorAuthenticationFailed) {
		t.Fatalf("unverified error = %v", err)
	}
}

func TestDBRuntimeNodeCredentialVerifierUsesDBTimeAndFailsClosed(t *testing.T) {
	t.Parallel()

	databaseNow := time.Date(2026, 7, 11, 5, 0, 0, 0, time.UTC)
	nodeID := uuid.New()
	presented := RuntimePresentedCertificate{
		Serial:                    "abc",
		FingerprintSHA256:         repeatedHex("a"),
		PublicKeyThumbprintSHA256: repeatedHex("b"),
		NotBefore:                 databaseNow.Add(-time.Minute),
		NotAfter:                  databaseNow.Add(time.Minute),
	}
	queries := &sessionNodeCredentialQueriesFake{node: db.RuntimeNode{
		NodeID:                    nodeID,
		DeviceCertificateSerial:   presented.Serial,
		DevicePublicKeyThumbprint: presented.PublicKeyThumbprintSHA256,
		Status:                    "draining",
	}}
	clock := &sessionClockFake{now: databaseNow}
	verifier := newDBRuntimeNodeCredentialVerifier(queries, clock)

	identity, err := verifier.VerifyRuntimeNodeCredential(context.Background(), presented)
	if err != nil || identity.NodeID != nodeID {
		t.Fatalf("VerifyRuntimeNodeCredential() = %#v, %v", identity, err)
	}
	if queries.params.DeviceCertificateSerial != presented.Serial ||
		queries.params.DevicePublicKeyThumbprint != presented.PublicKeyThumbprintSHA256 {
		t.Fatalf("certificate lookup = %#v", queries.params)
	}

	expired := presented
	expired.NotAfter = databaseNow
	if _, err = verifier.VerifyRuntimeNodeCredential(context.Background(), expired); !IsRuntimeSessionError(err, RuntimeSessionErrorAuthenticationFailed) {
		t.Fatalf("expired error = %v", err)
	}
	queries.node.Status = "revoked"
	if _, err = verifier.VerifyRuntimeNodeCredential(context.Background(), presented); !IsRuntimeSessionError(err, RuntimeSessionErrorAuthenticationFailed) {
		t.Fatalf("revoked error = %v", err)
	}
}

func TestRuntimeSessionServiceCreateLocksPrincipalAndPersistsAuthenticatedIdentity(t *testing.T) {
	t.Parallel()

	fixture := newSessionFixture()
	tx := newSessionTransactionFake(fixture)
	tx.getErr = pgx.ErrNoRows
	repository := &sessionRepositoryFake{tx: tx}
	service := newRuntimeSessionService(repository, fixture.coreID)

	state, err := service.CreateOrAttachSession(context.Background(), fixture.principal, fixture.request)
	if err != nil {
		t.Fatalf("CreateOrAttachSession() error = %v", err)
	}
	wantOrder := []string{
		"lock_session_identity", "lock_sessions", "lock_nodes", "lock_tokens",
		"lock_attachments", "get_session_for_update", "cluster_gate", "get_node", "list_active", "create_session", "create_attachment",
	}
	if !reflect.DeepEqual(tx.operations, wantOrder) {
		t.Fatalf("operation order = %#v, want %#v", tx.operations, wantOrder)
	}
	if tx.createParams.AgentID != fixture.principal.AgentID ||
		tx.createParams.CredentialID != fixture.principal.CredentialID ||
		tx.createParams.NodeID != fixture.principal.Device.NodeID ||
		tx.createParams.DeviceCertificateSerial != fixture.principal.Device.CertificateSerial {
		t.Fatalf("created identity = %#v", tx.createParams)
	}
	if !sortStringsEqual(tx.createParams.Features, RuntimeRequiredFeatures()) {
		t.Fatalf("created features = %#v", tx.createParams.Features)
	}
	if state.Replayed || state.Resumed || state.Attachment == nil ||
		state.DatabaseTime != tx.attachment.AttachedAt {
		t.Fatalf("state = %#v", state)
	}
	if tx.clusterGateOperation != RuntimeClusterNewSession {
		t.Fatalf("cluster gate operation = %q", tx.clusterGateOperation)
	}
}

func TestRuntimeSessionServiceHardMaintenanceRejectsOnlyNewSession(t *testing.T) {
	t.Parallel()

	gateErr := errors.New("hard maintenance")
	fixture := newSessionFixture()
	newTx := newSessionTransactionFake(fixture)
	newTx.getErr = pgx.ErrNoRows
	newTx.clusterGateErr = gateErr
	service := newRuntimeSessionService(&sessionRepositoryFake{tx: newTx}, fixture.coreID)
	if _, err := service.CreateOrAttachSession(context.Background(), fixture.principal, fixture.request); !errors.Is(err, gateErr) {
		t.Fatalf("new session error = %v", err)
	}
	if newTx.createCalls != 0 {
		t.Fatalf("new session writes = %d", newTx.createCalls)
	}

	existingTx := newSessionTransactionFake(fixture)
	existingTx.clusterGateErr = gateErr
	service = newRuntimeSessionService(&sessionRepositoryFake{tx: existingTx}, fixture.coreID)
	if _, err := service.CreateOrAttachSession(context.Background(), fixture.principal, fixture.request); err != nil {
		t.Fatalf("existing session replay error = %v", err)
	}
	if existingTx.clusterGateOperation != "" {
		t.Fatalf("existing session unexpectedly gated as %q", existingTx.clusterGateOperation)
	}
}

func TestRuntimeSessionServiceReconnectRotatesAttachmentGeneration(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name           string
		status         string
		attached       bool
		wantResumed    bool
		wantAttachKind string
	}{
		{name: "same active session gets a new attachment", status: "active", attached: true, wantResumed: true, wantAttachKind: "resumed"},
		{name: "offline reconnect", status: "offline", wantResumed: true, wantAttachKind: "resumed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newSessionFixture()
			tx := newSessionTransactionFake(fixture)
			tx.session.Status = test.status
			if !test.attached {
				tx.session.AttachedCoreInstanceID = nil
			}
			repository := &sessionRepositoryFake{tx: tx}
			service := newRuntimeSessionService(repository, fixture.coreID)

			previousAttachmentID := tx.attachment.ID
			state, err := service.CreateOrAttachSession(context.Background(), fixture.principal, fixture.request)
			if err != nil {
				t.Fatalf("CreateOrAttachSession() error = %v", err)
			}
			if state.Replayed || state.Resumed != test.wantResumed {
				t.Fatalf("state = %#v", state)
			}
			if state.Attachment == nil || state.Attachment.ID == previousAttachmentID {
				t.Fatalf("attachment generation was not rotated: previous=%s state=%#v", previousAttachmentID, state)
			}
			if test.wantAttachKind != "" && tx.createAttachmentParams.AttachmentKind != test.wantAttachKind {
				t.Fatalf("attachment kind = %q", tx.createAttachmentParams.AttachmentKind)
			}
			if tx.heartbeatParams.Capacity != fixture.request.Capacity {
				t.Fatalf("heartbeat capacity = %d", tx.heartbeatParams.Capacity)
			}
		})
	}
}

func TestRuntimeSessionServiceRejectsSecondEpochAndInactiveCredential(t *testing.T) {
	t.Parallel()

	t.Run("same worker second epoch", func(t *testing.T) {
		fixture := newSessionFixture()
		tx := newSessionTransactionFake(fixture)
		tx.getErr = pgx.ErrNoRows
		other := tx.session
		other.RuntimeSessionID = uuid.New()
		other.SessionEpoch++
		tx.active = []db.RuntimeSession{other}
		service := newRuntimeSessionService(&sessionRepositoryFake{tx: tx}, fixture.coreID)

		_, err := service.CreateOrAttachSession(context.Background(), fixture.principal, fixture.request)
		if !IsRuntimeSessionError(err, RuntimeSessionErrorSessionConflict) {
			t.Fatalf("error = %v", err)
		}
		if tx.createCalls != 0 {
			t.Fatalf("CreateRuntimeSession calls = %d", tx.createCalls)
		}
	})

	for _, name := range []string{"revoked", "expired"} {
		t.Run(name+" Agent credential", func(t *testing.T) {
			fixture := newSessionFixture()
			tx := newSessionTransactionFake(fixture)
			tx.getErr = pgx.ErrNoRows
			tx.createErr = pgx.ErrNoRows
			service := newRuntimeSessionService(&sessionRepositoryFake{tx: tx}, fixture.coreID)

			_, err := service.CreateOrAttachSession(context.Background(), fixture.principal, fixture.request)
			if !IsRuntimeSessionError(err, RuntimeSessionErrorPrincipalInactive) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestRuntimeSessionServiceResolveHeartbeatAndCloseFailClosed(t *testing.T) {
	t.Parallel()

	t.Run("resolve active principal", func(t *testing.T) {
		fixture := newSessionFixture()
		tx := newSessionTransactionFake(fixture)
		repository := &sessionRepositoryFake{tx: tx, resolved: RuntimeSessionPrincipal{
			RuntimeSessionID:                fixture.request.RuntimeSessionID,
			NodeID:                          fixture.principal.Device.NodeID,
			AgentID:                         fixture.principal.AgentID,
			CredentialID:                    fixture.principal.CredentialID,
			WorkerID:                        fixture.request.WorkerID,
			SessionEpoch:                    fixture.request.SessionEpoch,
			CoreInstanceID:                  fixture.coreID,
			AttachmentID:                    tx.attachment.ID,
			DeviceCertificateSerial:         fixture.principal.Device.CertificateSerial,
			DevicePublicKeyThumbprintSHA256: fixture.principal.Device.PublicKeyThumbprintSHA256,
			Status:                          "active",
			DatabaseTime:                    fixture.databaseNow,
		}}
		service := newRuntimeSessionService(repository, fixture.coreID)
		resolved, err := service.ResolveSessionPrincipal(context.Background(), fixture.principal, fixture.request.RuntimeSessionID)
		eventPrincipal := resolved.EventPrincipal()
		if err != nil || eventPrincipal.AgentID != fixture.principal.AgentID ||
			eventPrincipal.CredentialID == nil || *eventPrincipal.CredentialID != fixture.principal.CredentialID ||
			eventPrincipal.DeviceCertificateSerial == nil || *eventPrincipal.DeviceCertificateSerial != fixture.principal.Device.CertificateSerial {
			t.Fatalf("ResolveSessionPrincipal() = %#v, %v", resolved, err)
		}
		if repository.resolveParams.CredentialID != fixture.principal.CredentialID ||
			repository.resolveParams.CoreInstanceID != fixture.coreID {
			t.Fatalf("resolve params = %#v", repository.resolveParams)
		}

		repository.workerResolved = resolved
		workerResolved, err := service.ResolveWorkerSessionPrincipal(
			context.Background(), fixture.principal, fixture.request.WorkerID,
		)
		if err != nil || workerResolved.RuntimeSessionID != fixture.request.RuntimeSessionID {
			t.Fatalf("ResolveWorkerSessionPrincipal() = %#v, %v", workerResolved, err)
		}
		if repository.workerResolveParams.WorkerID != fixture.request.WorkerID ||
			repository.workerResolveParams.CoreInstanceID != fixture.coreID ||
			repository.workerResolveParams.CertificateSerial != fixture.principal.Device.CertificateSerial {
			t.Fatalf("worker resolve params = %#v", repository.workerResolveParams)
		}

		repository.resolveErr = pgx.ErrNoRows
		if _, err = service.ResolveSessionPrincipal(context.Background(), fixture.principal, fixture.request.RuntimeSessionID); !IsRuntimeSessionError(err, RuntimeSessionErrorPrincipalInactive) {
			t.Fatalf("inactive resolve error = %v", err)
		}
	})

	t.Run("heartbeat uses stored DB timestamp and principal query", func(t *testing.T) {
		fixture := newSessionFixture()
		tx := newSessionTransactionFake(fixture)
		repository := &sessionRepositoryFake{tx: tx}
		service := newRuntimeSessionService(repository, fixture.coreID)
		heartbeat := fixture.request
		heartbeat.AttachmentID = tx.attachment.ID
		state, err := service.HeartbeatSession(context.Background(), fixture.principal, heartbeat)
		if err != nil || state.DatabaseTime != fixture.databaseNow.Add(2*time.Second) {
			t.Fatalf("HeartbeatSession() = %#v, %v", state, err)
		}
		if tx.heartbeatNodeParams.DevicePublicKeyThumbprint != fixture.principal.Device.PublicKeyThumbprintSHA256 {
			t.Fatalf("node heartbeat did not preserve authenticated device: %#v", tx.heartbeatNodeParams)
		}
		tx.heartbeatErr = pgx.ErrNoRows
		if _, err = service.HeartbeatSession(context.Background(), fixture.principal, heartbeat); !IsRuntimeSessionError(err, RuntimeSessionErrorPrincipalInactive) {
			t.Fatalf("inactive heartbeat error = %v", err)
		}
	})

	t.Run("close detaches before disconnect", func(t *testing.T) {
		fixture := newSessionFixture()
		tx := newSessionTransactionFake(fixture)
		service := newRuntimeSessionService(&sessionRepositoryFake{tx: tx}, fixture.coreID)
		state, err := service.CloseSession(context.Background(), fixture.principal, RuntimeSessionCloseRequest{
			RuntimeSessionIdentity: fixture.request.RuntimeSessionIdentity,
			Status:                 "offline",
			Reason:                 "transport disconnected",
			AttachmentID:           tx.attachment.ID,
		})
		if err != nil || state.Session.Status != "offline" {
			t.Fatalf("CloseSession() = %#v, %v", state, err)
		}
		wantTail := []string{"close_attachment", "close_session"}
		if len(tx.operations) < 2 || !reflect.DeepEqual(tx.operations[len(tx.operations)-2:], wantTail) {
			t.Fatalf("close operation order = %#v", tx.operations)
		}
	})

	t.Run("stale attachment cleanup cannot close a replacement transport", func(t *testing.T) {
		fixture := newSessionFixture()
		tx := newSessionTransactionFake(fixture)
		service := newRuntimeSessionService(&sessionRepositoryFake{tx: tx}, fixture.coreID)
		staleAttachmentID := tx.attachment.ID
		state, err := service.CreateOrAttachSession(context.Background(), fixture.principal, fixture.request)
		if err != nil || state.Attachment == nil || state.Attachment.ID == staleAttachmentID {
			t.Fatalf("replacement attach = %#v, %v", state, err)
		}
		_, err = service.CloseSession(context.Background(), fixture.principal, RuntimeSessionCloseRequest{
			RuntimeSessionIdentity: fixture.request.RuntimeSessionIdentity,
			Status:                 "offline",
			Reason:                 "stale websocket cleanup",
			AttachmentID:           staleAttachmentID,
		})
		if !IsRuntimeSessionError(err, RuntimeSessionErrorNotAttached) {
			t.Fatalf("stale cleanup error = %v", err)
		}
		if tx.closeSessionCalls != 0 {
			t.Fatalf("stale cleanup closed the replacement Session %d time(s)", tx.closeSessionCalls)
		}
	})
}

func TestRuntimeSessionValidationRejectsBodyPrincipalAndContractDowngrade(t *testing.T) {
	t.Parallel()

	fixture := newSessionFixture()
	service := newRuntimeSessionService(&sessionRepositoryFake{tx: newSessionTransactionFake(fixture)}, fixture.coreID)

	wrongAgent := fixture.request
	wrongAgent.AgentID = uuid.New()
	if _, err := service.CreateOrAttachSession(context.Background(), fixture.principal, wrongAgent); !IsRuntimeSessionError(err, RuntimeSessionErrorAgentMismatch) {
		t.Fatalf("wrong Agent error = %v", err)
	}
	wrongNode := fixture.request
	wrongNode.NodeID = uuid.New()
	if _, err := service.CreateOrAttachSession(context.Background(), fixture.principal, wrongNode); !IsRuntimeSessionError(err, RuntimeSessionErrorDeviceMismatch) {
		t.Fatalf("wrong Node error = %v", err)
	}
	oldProtocol := fixture.request
	oldProtocol.ProtocolVersion = 1
	if _, err := service.CreateOrAttachSession(context.Background(), fixture.principal, oldProtocol); !IsRuntimeSessionError(err, RuntimeSessionErrorProtocolUnsupported) {
		t.Fatalf("old protocol error = %v", err)
	}
	missingFeature := fixture.request
	missingFeature.Features = missingFeature.Features[:len(missingFeature.Features)-1]
	if _, err := service.CreateOrAttachSession(context.Background(), fixture.principal, missingFeature); !IsRuntimeSessionError(err, RuntimeSessionErrorRequiredFeatureMissing) {
		t.Fatalf("missing feature error = %v", err)
	}
}

type sessionFixture struct {
	coreID      uuid.UUID
	databaseNow time.Time
	principal   AuthenticatedRuntimePrincipal
	request     RuntimeSessionRequest
}

func newSessionFixture() sessionFixture {
	agentID := uuid.New()
	nodeID := uuid.New()
	return sessionFixture{
		coreID:      uuid.New(),
		databaseNow: time.Date(2026, 7, 11, 7, 8, 9, 0, time.UTC),
		principal: AuthenticatedRuntimePrincipal{
			AgentID:      agentID,
			CredentialID: uuid.New(),
			Device: RuntimeDeviceIdentity{
				NodeID:                       nodeID,
				CertificateSerial:            "abc",
				CertificateFingerprintSHA256: repeatedHex("a"),
				PublicKeyThumbprintSHA256:    repeatedHex("b"),
			},
		},
		request: RuntimeSessionRequest{
			RuntimeSessionIdentity: RuntimeSessionIdentity{
				RuntimeSessionID: uuid.New(),
				NodeID:           nodeID,
				AgentID:          agentID,
				WorkerID:         "worker-installation-1",
				SessionEpoch:     11,
			},
			NodeVersion:           "2.0.0",
			ProtocolVersion:       RuntimeProtocolVersion,
			RuntimeContractID:     RuntimeContractID,
			RuntimeContractDigest: RuntimeContractDigest,
			Features:              RuntimeRequiredFeatures(),
			Capacity:              4,
		},
	}
}

type sessionRepositoryFake struct {
	txMu                sync.Mutex
	staleMu             sync.Mutex
	tx                  *sessionTransactionFake
	resolved            RuntimeSessionPrincipal
	resolveErr          error
	resolveParams       runtimeSessionResolveParams
	workerResolved      RuntimeSessionPrincipal
	workerResolveErr    error
	workerResolveParams runtimeWorkerSessionResolveParams
	staleCandidates     []db.RuntimeSession
	staleListErr        error
	staleTTL            time.Duration
	staleLimit          int
}

func (r *sessionRepositoryFake) WithTransaction(ctx context.Context, fn func(runtimeSessionTransaction) error) error {
	r.txMu.Lock()
	defer r.txMu.Unlock()
	return fn(r.tx)
}

func (r *sessionRepositoryFake) ListStaleRuntimeSessionCandidates(_ context.Context, ttl time.Duration, limit int) ([]db.RuntimeSession, error) {
	r.staleMu.Lock()
	defer r.staleMu.Unlock()
	r.staleTTL = ttl
	r.staleLimit = limit
	return append([]db.RuntimeSession(nil), r.staleCandidates...), r.staleListErr
}

func (r *sessionRepositoryFake) ResolveRuntimeSessionPrincipal(_ context.Context, params runtimeSessionResolveParams) (RuntimeSessionPrincipal, error) {
	r.resolveParams = params
	return r.resolved, r.resolveErr
}

func (r *sessionRepositoryFake) ResolveRuntimeWorkerSessionPrincipal(_ context.Context, params runtimeWorkerSessionResolveParams) (RuntimeSessionPrincipal, error) {
	r.workerResolveParams = params
	return r.workerResolved, r.workerResolveErr
}

type sessionTransactionFake struct {
	operations             []string
	clusterGateErr         error
	clusterGateOperation   RuntimeClusterOperation
	fixture                sessionFixture
	session                db.RuntimeSession
	getErr                 error
	node                   db.RuntimeNode
	active                 []db.RuntimeSession
	createErr              error
	createCalls            int
	createParams           db.CreateRuntimeSessionParams
	claimParams            db.ClaimRuntimeSessionForCoreParams
	heartbeatParams        db.HeartbeatRuntimeSessionParams
	heartbeatNodeParams    db.HeartbeatRuntimeNodeParams
	heartbeatErr           error
	attachment             db.RuntimeSessionAttachment
	createAttachmentParams db.CreateRuntimeSessionAttachmentParams
	closeAttachmentParams  db.CloseRuntimeSessionAttachmentParams
	closeSessionCalls      int
}

func newSessionTransactionFake(fixture sessionFixture) *sessionTransactionFake {
	coreID := fixture.coreID
	session := db.RuntimeSession{
		RuntimeSessionID:        fixture.request.RuntimeSessionID,
		NodeID:                  fixture.request.NodeID,
		AgentID:                 fixture.request.AgentID,
		CredentialID:            fixture.principal.CredentialID,
		WorkerID:                fixture.request.WorkerID,
		SessionEpoch:            fixture.request.SessionEpoch,
		DeviceCertificateSerial: fixture.principal.Device.CertificateSerial,
		NodeVersion:             fixture.request.NodeVersion,
		ProtocolVersion:         fixture.request.ProtocolVersion,
		RuntimeContractID:       fixture.request.RuntimeContractID,
		RuntimeContractDigest:   fixture.request.RuntimeContractDigest,
		Features:                fixture.request.Features,
		Capacity:                fixture.request.Capacity,
		Status:                  "active",
		AttachedCoreInstanceID:  &coreID,
		ConnectedAt:             fixture.databaseNow.Add(-time.Minute),
		HeartbeatAt:             fixture.databaseNow,
		CreatedAt:               fixture.databaseNow.Add(-time.Minute),
		UpdatedAt:               fixture.databaseNow,
	}
	return &sessionTransactionFake{
		fixture: fixture,
		session: session,
		node: db.RuntimeNode{
			NodeID:                    fixture.principal.Device.NodeID,
			DeviceCertificateSerial:   fixture.principal.Device.CertificateSerial,
			DevicePublicKeyThumbprint: fixture.principal.Device.PublicKeyThumbprintSHA256,
			NodeVersion:               fixture.request.NodeVersion,
			ProtocolVersion:           fixture.request.ProtocolVersion,
			RuntimeContractID:         fixture.request.RuntimeContractID,
			RuntimeContractDigest:     fixture.request.RuntimeContractDigest,
			Features:                  fixture.request.Features,
			Status:                    "active",
		},
		attachment: db.RuntimeSessionAttachment{
			ID:               uuid.New(),
			RuntimeSessionID: fixture.request.RuntimeSessionID,
			CoreInstanceID:   fixture.coreID,
			AttachmentKind:   "connected",
			AttachedAt:       fixture.databaseNow.Add(time.Second),
		},
	}
}

func (f *sessionTransactionFake) op(name string) { f.operations = append(f.operations, name) }

func (f *sessionTransactionFake) RequireRuntimeClusterOperation(_ context.Context, operation RuntimeClusterOperation) error {
	f.op("cluster_gate")
	f.clusterGateOperation = operation
	return f.clusterGateErr
}

func (f *sessionTransactionFake) LockSessionIdentity(context.Context, uuid.UUID) error {
	f.op("lock_session_identity")
	return nil
}

func (f *sessionTransactionFake) GetRuntimeSessionForUpdate(context.Context, uuid.UUID) (db.RuntimeSession, error) {
	f.op("get_session_for_update")
	return f.session, f.getErr
}

func (f *sessionTransactionFake) LockRuntimeSessionsForPrincipalRevocation(context.Context, db.LockRuntimeSessionsForPrincipalRevocationParams) ([]db.LockRuntimeSessionsForPrincipalRevocationRow, error) {
	f.op("lock_sessions")
	return []db.LockRuntimeSessionsForPrincipalRevocationRow{{RuntimeSessionID: f.session.RuntimeSessionID}}, nil
}

func (f *sessionTransactionFake) LockRuntimeNodesForPrincipalRevocation(_ context.Context, ids []uuid.UUID) ([]uuid.UUID, error) {
	f.op("lock_nodes")
	return ids, nil
}

func (f *sessionTransactionFake) LockAgentTokensForPrincipalRevocation(_ context.Context, ids []uuid.UUID) ([]uuid.UUID, error) {
	f.op("lock_tokens")
	return ids, nil
}

func (f *sessionTransactionFake) LockActiveRuntimeSessionAttachmentsForPrincipalRevocation(context.Context, []uuid.UUID) ([]uuid.UUID, error) {
	f.op("lock_attachments")
	return []uuid.UUID{f.attachment.ID}, nil
}

func (f *sessionTransactionFake) GetRuntimeNode(context.Context, uuid.UUID) (db.RuntimeNode, error) {
	f.op("get_node")
	return f.node, nil
}

func (f *sessionTransactionFake) HeartbeatRuntimeNode(_ context.Context, params db.HeartbeatRuntimeNodeParams) (db.RuntimeNode, error) {
	f.op("heartbeat_node")
	f.heartbeatNodeParams = params
	if f.heartbeatErr != nil {
		return db.RuntimeNode{}, f.heartbeatErr
	}
	return f.node, nil
}

func (f *sessionTransactionFake) ListActiveRuntimeSessionsByNode(context.Context, uuid.UUID) ([]db.RuntimeSession, error) {
	f.op("list_active")
	return f.active, nil
}

func (f *sessionTransactionFake) CreateRuntimeSession(_ context.Context, params db.CreateRuntimeSessionParams) (db.RuntimeSession, error) {
	f.op("create_session")
	f.createCalls++
	f.createParams = params
	if f.createErr != nil {
		return db.RuntimeSession{}, f.createErr
	}
	created := f.session
	created.RuntimeSessionID = params.RuntimeSessionID
	created.Features = params.Features
	return created, nil
}

func (f *sessionTransactionFake) ClaimRuntimeSessionForCore(_ context.Context, params db.ClaimRuntimeSessionForCoreParams) (db.RuntimeSession, error) {
	f.op("claim_session")
	f.claimParams = params
	claimed := f.session
	claimed.Status = "active"
	claimed.AttachedCoreInstanceID = &f.fixture.coreID
	return claimed, nil
}

func (f *sessionTransactionFake) HeartbeatRuntimeSession(_ context.Context, params db.HeartbeatRuntimeSessionParams) (db.RuntimeSession, error) {
	f.op("heartbeat_session")
	f.heartbeatParams = params
	if f.heartbeatErr != nil {
		return db.RuntimeSession{}, f.heartbeatErr
	}
	heartbeat := f.session
	heartbeat.Status = "active"
	heartbeat.AttachedCoreInstanceID = &f.fixture.coreID
	heartbeat.Capacity = params.Capacity
	heartbeat.HeartbeatAt = f.fixture.databaseNow.Add(2 * time.Second)
	return heartbeat, nil
}

func (f *sessionTransactionFake) GetActiveRuntimeSessionAttachment(context.Context, uuid.UUID) (db.RuntimeSessionAttachment, error) {
	f.op("get_attachment")
	return f.attachment, nil
}

func (f *sessionTransactionFake) CreateRuntimeSessionAttachment(_ context.Context, params db.CreateRuntimeSessionAttachmentParams) (db.RuntimeSessionAttachment, error) {
	f.op("create_attachment")
	f.createAttachmentParams = params
	attachment := f.attachment
	attachment.ID = uuid.New()
	attachment.AttachmentKind = params.AttachmentKind
	attachment.AttachedAt = attachment.AttachedAt.Add(time.Second)
	f.attachment = attachment
	return attachment, nil
}

func (f *sessionTransactionFake) CloseRuntimeSessionAttachment(_ context.Context, params db.CloseRuntimeSessionAttachmentParams) (db.RuntimeSessionAttachment, error) {
	f.op("close_attachment")
	f.closeAttachmentParams = params
	if params.AttachmentID != f.attachment.ID {
		return db.RuntimeSessionAttachment{}, pgx.ErrNoRows
	}
	return f.attachment, nil
}

func (f *sessionTransactionFake) CloseRuntimeSession(_ context.Context, params db.CloseRuntimeSessionParams) (db.RuntimeSession, error) {
	f.op("close_session")
	f.closeSessionCalls++
	closed := f.session
	closed.Status = params.Status
	closed.AttachedCoreInstanceID = nil
	closed.HeartbeatAt = f.fixture.databaseNow.Add(2 * time.Second)
	return closed, nil
}

func (f *sessionTransactionFake) CloseStaleRuntimeSession(_ context.Context, params db.CloseStaleRuntimeSessionParams) (db.RuntimeSession, error) {
	f.op("close_stale_session")
	if f.session.RuntimeSessionID != params.RuntimeSessionID ||
		!f.session.HeartbeatAt.Equal(params.HeartbeatAt) ||
		f.session.AttachedCoreInstanceID == nil ||
		*f.session.AttachedCoreInstanceID != params.CoreInstanceID ||
		(f.session.Status != "active" && f.session.Status != "draining") {
		return db.RuntimeSession{}, pgx.ErrNoRows
	}
	closed := f.session
	closed.Status = "offline"
	closed.AttachedCoreInstanceID = nil
	f.session = closed
	return closed, nil
}

type sessionCredentialVerifierFake struct {
	identity  RuntimeDeviceIdentity
	err       error
	presented RuntimePresentedCertificate
	calls     int
}

func (f *sessionCredentialVerifierFake) VerifyRuntimeNodeCredential(_ context.Context, presented RuntimePresentedCertificate) (RuntimeDeviceIdentity, error) {
	f.calls++
	f.presented = presented
	return f.identity, f.err
}

type sessionNodeCredentialQueriesFake struct {
	node   db.RuntimeNode
	err    error
	params db.GetRuntimeNodeByCertificateParams
}

func (f *sessionNodeCredentialQueriesFake) GetRuntimeNodeByCertificate(_ context.Context, params db.GetRuntimeNodeByCertificateParams) (db.RuntimeNode, error) {
	f.params = params
	return f.node, f.err
}

type sessionClockFake struct {
	now time.Time
	err error
}

func (f *sessionClockFake) QueryRow(context.Context, string, ...any) pgx.Row {
	return sessionClockRow{now: f.now, err: f.err}
}

type sessionClockRow struct {
	now time.Time
	err error
}

func (r sessionClockRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 1 {
		return errors.New("unexpected scan destination")
	}
	value, ok := dest[0].(*time.Time)
	if !ok {
		return errors.New("unexpected scan type")
	}
	*value = r.now
	return nil
}

func repeatedHex(digit string) string {
	return digit + digit + digit + digit + digit + digit + digit + digit +
		digit + digit + digit + digit + digit + digit + digit + digit +
		digit + digit + digit + digit + digit + digit + digit + digit +
		digit + digit + digit + digit + digit + digit + digit + digit +
		digit + digit + digit + digit + digit + digit + digit + digit +
		digit + digit + digit + digit + digit + digit + digit + digit +
		digit + digit + digit + digit + digit + digit + digit + digit +
		digit + digit + digit + digit + digit + digit + digit + digit
}

func sortStringsEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftDigest := sha256.Sum256([]byte(stringsJoinSorted(left)))
	rightDigest := sha256.Sum256([]byte(stringsJoinSorted(right)))
	return leftDigest == rightDigest
}

func stringsJoinSorted(values []string) string {
	copyValues := append([]string(nil), values...)
	sort.Strings(copyValues)
	return strings.Join(copyValues, "\x00")
}
