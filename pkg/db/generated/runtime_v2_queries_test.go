package db

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRuntimeV2AttemptAndCancellationQueries(t *testing.T) {
	now := time.Date(2026, 7, 11, 8, 0, 0, 0, time.UTC)
	runID, agentID, attemptID := uuid.New(), uuid.New(), uuid.New()
	leaseID, coreID := uuid.New(), uuid.New()
	lockDBTX := &fakeDBTX{row: fakeRow{values: []any{runID}}}
	lockQueries := New(lockDBTX)
	lockedRunID, err := lockQueries.LockNextPendingRuntimeRun(context.Background())
	if err != nil || lockedRunID != runID {
		t.Fatalf("LockNextPendingRuntimeRun = %s, %v", lockedRunID, err)
	}
	requireSQLName(t, lockDBTX.queryRowSQL, "LockNextPendingRuntimeRun")
	if !strings.Contains(lockDBTX.queryRowSQL, "ORDER BY started_at ASC, id ASC") ||
		!strings.Contains(lockDBTX.queryRowSQL, "FOR UPDATE SKIP LOCKED") {
		t.Fatal("LockNextPendingRuntimeRun must match the global pending index")
	}

	lockDBTX.row = fakeRow{values: []any{runID}}
	lockedRunID, err = lockQueries.LockNextDueRetryRuntimeRun(context.Background())
	if err != nil || lockedRunID != runID {
		t.Fatalf("LockNextDueRetryRuntimeRun = %s, %v", lockedRunID, err)
	}
	requireSQLName(t, lockDBTX.queryRowSQL, "LockNextDueRetryRuntimeRun")
	if !strings.Contains(lockDBTX.queryRowSQL, "ORDER BY next_attempt_at ASC, started_at ASC, id ASC") ||
		!strings.Contains(lockDBTX.queryRowSQL, "FOR UPDATE SKIP LOCKED") {
		t.Fatal("LockNextDueRetryRuntimeRun must match the global retry index")
	}

	attemptValues := []any{
		attemptID, runID, agentID, int32(1), (*int32)(nil), "core_http",
		leaseID, int64(1), (*uuid.UUID)(nil), (*string)(nil), (*uuid.UUID)(nil),
		(*uuid.UUID)(nil), coreID, coreID, now, now.Add(time.Minute),
		(*time.Time)(nil), (*time.Time)(nil), now.Add(time.Minute),
		now.Add(5 * time.Minute), (*time.Time)(nil), (*string)(nil),
		(*uuid.UUID)(nil), []byte(nil), (*string)(nil), (*time.Time)(nil),
		int64(0), (*int64)(nil), (*string)(nil), (*string)(nil), now,
	}
	dbtx := &fakeDBTX{row: fakeRow{values: attemptValues}}
	q := New(dbtx)

	attempt, err := q.CreateRunAttempt(context.Background(), CreateRunAttemptParams{
		ID: attemptID, RunID: runID, AgentID: agentID, OfferNo: 1,
		ExecutorType: "core_http", LeaseID: leaseID, FencingToken: 1,
		OfferedByCoreInstanceID: coreID, AttachedCoreInstanceID: coreID,
		OfferExpiresAt: now.Add(time.Minute), LeaseExpiresAt: now.Add(time.Minute),
		AttemptDeadlineAt: now.Add(5 * time.Minute),
	})
	if err != nil || attempt.ID != attemptID || attempt.FencingToken != 1 {
		t.Fatalf("CreateRunAttempt = %#v, %v", attempt, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRunAttempt")
	if len(dbtx.queryRowArgs) != 16 {
		t.Fatalf("CreateRunAttempt arg count = %d", len(dbtx.queryRowArgs))
	}

	dbtx.queryRows = &fakeRows{rows: [][]any{attemptValues}}
	listed, err := q.ListRunAttemptsByRun(context.Background(), runID)
	if err != nil || len(listed) != 1 || listed[0].LeaseID != leaseID {
		t.Fatalf("ListRunAttemptsByRun = %#v, %v", listed, err)
	}

	cancellationID := uuid.New()
	cancellationValues := []any{
		cancellationID, runID, &attemptID, "requested", "user", uuid.New(),
		(*string)(nil), now, (*time.Time)(nil), (*time.Time)(nil),
		(*time.Time)(nil), (*time.Time)(nil), (*string)(nil), now,
	}
	dbtx.row = fakeRow{values: cancellationValues}
	cancellation, err := q.CreateRunCancellation(context.Background(), CreateRunCancellationParams{
		ID: cancellationID, RunID: runID, TargetAttemptID: &attemptID,
		RequestedByType: "user", RequestedByID: cancellationValues[5].(uuid.UUID),
	})
	if err != nil || cancellation.ID != cancellationID || cancellation.TargetAttemptID == nil {
		t.Fatalf("CreateRunCancellation = %#v, %v", cancellation, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRunCancellation")
}

func TestRuntimeV2OutboxLedgerAndDLQQueries(t *testing.T) {
	now := time.Date(2026, 7, 11, 8, 0, 0, 0, time.UTC)
	runID, agentID, eventID := uuid.New(), uuid.New(), uuid.New()
	owner := uuid.New()

	signalID := uuid.New()
	signalValues := []any{
		signalID, "run.available", agentID, &runID, []byte(`{"run_id":"x"}`),
		now, now, "processing", &owner, ptrTime(now.Add(time.Minute)),
		(*time.Time)(nil), int32(1), (*string)(nil),
	}
	dbtx := &fakeDBTX{queryRows: &fakeRows{rows: [][]any{signalValues}}}
	q := New(dbtx)
	signals, err := q.ClaimRuntimeSignals(context.Background(), ClaimRuntimeSignalsParams{
		LeaseOwner: owner, LeaseDurationMs: 30_000, Limit: 10,
	})
	if err != nil || len(signals) != 1 || signals[0].ID != signalID {
		t.Fatalf("ClaimRuntimeSignals = %#v, %v", signals, err)
	}
	requireSQLName(t, dbtx.querySQL, "ClaimRuntimeSignals")
	if !strings.Contains(dbtx.querySQL, "FOR UPDATE SKIP LOCKED") {
		t.Fatal("ClaimRuntimeSignals must use SKIP LOCKED")
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{owner, int64(30_000), int32(10)}) {
		t.Fatalf("ClaimRuntimeSignals args = %#v", dbtx.queryArgs)
	}

	effectID := uuid.New()
	effectValues := []any{
		effectID, runID, eventID, "delivery.webhook", "agent:" + agentID.String(),
		[]byte(`{}`), "processing", now, &owner, ptrTime(now.Add(time.Minute)),
		int32(1), int32(12), (*time.Time)(nil), (*string)(nil), now,
	}
	dbtx.queryRows = &fakeRows{rows: [][]any{effectValues}}
	effects, err := q.ClaimRunEffects(context.Background(), ClaimRunEffectsParams{
		LeaseOwner: owner, LeaseDurationMs: 30_000, Limit: 5,
	})
	if err != nil || len(effects) != 1 || effects[0].TerminalEventID != eventID {
		t.Fatalf("ClaimRunEffects = %#v, %v", effects, err)
	}
	requireSQLName(t, dbtx.querySQL, "ClaimRunEffects")

	ledgerValues := []any{runID, eventID, agentID, int32(1), int64(25), now}
	dbtx.row = fakeRow{values: ledgerValues}
	ledger, err := q.InsertRunAccountingLedger(context.Background(), InsertRunAccountingLedgerParams{
		RunID: runID, TerminalEventID: eventID, AgentID: agentID,
		SuccessDelta: 1, RevenueDeltaCents: 25,
	})
	if err != nil || ledger.RevenueDeltaCents != 25 {
		t.Fatalf("InsertRunAccountingLedger = %#v, %v", ledger, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "InsertRunAccountingLedger")

	deadLetterID := uuid.New()
	dbtx.row = fakeRow{values: []any{deadLetterID, runID, int32(3), "RETRY_EXHAUSTED", (*string)(nil), now}}
	deadLetter, err := q.CreateRunDeadLetter(context.Background(), CreateRunDeadLetterParams{
		RunID: runID, FinalAttemptNo: 3, ReasonCode: "RETRY_EXHAUSTED",
	})
	if err != nil || deadLetter.ID != deadLetterID {
		t.Fatalf("CreateRunDeadLetter = %#v, %v", deadLetter, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRunDeadLetter")
}

func TestRuntimeV2NodeSessionAndClusterQueries(t *testing.T) {
	now := time.Date(2026, 7, 11, 8, 0, 0, 0, time.UTC)
	digest := "d83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f"
	features := []string{
		"lease_fence", "assignment_confirm", "renew", "resume",
		"event_ack", "result_ack", "cancel", "persistent_spool",
	}
	dbtx := &fakeDBTX{}
	q := New(dbtx)

	dbtx.row = fakeRow{values: []any{int32(63), "063_reliable_runtime_v2", "openlinker.runtime.v2", digest, now, true}}
	contract, err := q.CreateRuntimeSchemaContract(context.Background(), CreateRuntimeSchemaContractParams{
		SchemaVersion: 63, MigrationName: "063_reliable_runtime_v2",
		RuntimeContractID: "openlinker.runtime.v2", RuntimeContractDigest: digest,
		IsCurrent: true,
	})
	if err != nil || contract.SchemaVersion != 63 {
		t.Fatalf("CreateRuntimeSchemaContract = %#v, %v", contract, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRuntimeSchemaContract")

	cutoverID := uuid.New()
	dbtx.row = fakeRow{values: []any{
		int16(1), "hard_maintenance", int32(2), cutoverID, (*time.Time)(nil),
		(*time.Time)(nil), now, (*time.Time)(nil), int64(1), "migration",
		(*uuid.UUID)(nil), now,
	}}
	control, err := q.UpsertRuntimeClusterControl(context.Background(), UpsertRuntimeClusterControlParams{
		Mode: "hard_maintenance", ExpectedReplicas: 2, CutoverID: cutoverID,
		HardMaintenanceAt: now, UpdatedByType: "migration",
	})
	if err != nil || control.CutoverID != cutoverID || control.ExpectedReplicas != 2 {
		t.Fatalf("UpsertRuntimeClusterControl = %#v, %v", control, err)
	}

	nodeID := uuid.New()
	nodeValues := []any{
		nodeID, "node-a", "serial-a", "thumb-a", "0.2.0", int32(2),
		"openlinker.runtime.v2", digest, features, int32(2),
		int32(0), "active", &now, (*time.Time)(nil), (*time.Time)(nil),
		(*string)(nil), now, now,
	}
	dbtx.row = fakeRow{values: nodeValues}
	node, err := q.UpsertRuntimeNode(context.Background(), UpsertRuntimeNodeParams{
		NodeID: nodeID, DisplayName: "node-a", DeviceCertificateSerial: "serial-a",
		DevicePublicKeyThumbprint: "thumb-a", NodeVersion: "0.2.0",
		ProtocolVersion: 2, RuntimeContractID: "openlinker.runtime.v2",
		RuntimeContractDigest: digest, Features: features, Capacity: 2,
	})
	if err != nil || node.NodeID != nodeID || node.LastSeenAt == nil {
		t.Fatalf("UpsertRuntimeNode = %#v, %v", node, err)
	}

	sessionID, agentID, credentialID, coreID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	sessionValues := []any{
		sessionID, nodeID, agentID, credentialID, "worker-a", int64(1),
		"serial-a", "0.2.0", int32(2), "openlinker.runtime.v2", digest,
		features, int32(2), int32(0), "active", &coreID,
		now, now, (*time.Time)(nil), now, now,
	}
	dbtx.row = fakeRow{values: sessionValues}
	session, err := q.CreateRuntimeSession(context.Background(), CreateRuntimeSessionParams{
		RuntimeSessionID: sessionID, NodeID: nodeID, AgentID: agentID,
		CredentialID: credentialID, WorkerID: "worker-a", SessionEpoch: 1,
		DeviceCertificateSerial: "serial-a", NodeVersion: "0.2.0",
		ProtocolVersion: 2, RuntimeContractID: "openlinker.runtime.v2",
		RuntimeContractDigest: digest, Features: features,
		Capacity: 2, AttachedCoreInstanceID: coreID,
	})
	if err != nil || session.RuntimeSessionID != sessionID || session.AttachedCoreInstanceID == nil {
		t.Fatalf("CreateRuntimeSession = %#v, %v", session, err)
	}

	attachmentID := uuid.New()
	dbtx.row = fakeRow{values: []any{attachmentID, sessionID, coreID, "connected", now, (*time.Time)(nil), (*string)(nil)}}
	attachment, err := q.CreateRuntimeSessionAttachment(context.Background(), CreateRuntimeSessionAttachmentParams{
		RuntimeSessionID: sessionID, CoreInstanceID: coreID, AttachmentKind: "connected",
	})
	if err != nil || attachment.ID != attachmentID {
		t.Fatalf("CreateRuntimeSessionAttachment = %#v, %v", attachment, err)
	}
}

func TestRuntimeV2PrincipalRevocationLockOrderQueries(t *testing.T) {
	nodeA, nodeB := uuid.New(), uuid.New()
	tokenA, tokenB := uuid.New(), uuid.New()
	sessionA, sessionB := uuid.New(), uuid.New()
	attachmentA, attachmentB := uuid.New(), uuid.New()

	sessionRows := &fakeRows{rows: [][]any{
		{sessionA, nodeA, tokenA, "active"},
		{sessionB, nodeB, tokenB, "offline"},
	}}
	dbtx := &fakeDBTX{queryRows: sessionRows}
	q := New(dbtx)

	nodeScope := []uuid.UUID{nodeB, nodeA}
	tokenScope := []uuid.UUID{tokenB, tokenA}
	lockedSessions, err := q.LockRuntimeSessionsForPrincipalRevocation(
		context.Background(),
		LockRuntimeSessionsForPrincipalRevocationParams{
			NodeIDs:  nodeScope,
			TokenIDs: tokenScope,
		},
	)
	if err != nil || len(lockedSessions) != 2 || lockedSessions[0].RuntimeSessionID != sessionA || lockedSessions[1].CredentialID != tokenB {
		t.Fatalf("LockRuntimeSessionsForPrincipalRevocation = %#v, %v", lockedSessions, err)
	}
	requireSQLName(t, dbtx.querySQL, "LockRuntimeSessionsForPrincipalRevocation")
	if !sessionRows.closed ||
		!strings.Contains(dbtx.querySQL, "node_id = ANY($1::uuid[])") ||
		!strings.Contains(dbtx.querySQL, "credential_id = ANY($2::uuid[])") ||
		!strings.Contains(dbtx.querySQL, "ORDER BY runtime_session_id ASC\nFOR UPDATE") {
		t.Fatalf("Session revocation lock must cover the node/token union in UUID order: %s", dbtx.querySQL)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{nodeScope, tokenScope}) {
		t.Fatalf("LockRuntimeSessionsForPrincipalRevocation args = %#v", dbtx.queryArgs)
	}

	nodeRows := &fakeRows{rows: [][]any{{nodeA}, {nodeB}}}
	dbtx.queryRows = nodeRows
	lockedNodes, err := q.LockRuntimeNodesForPrincipalRevocation(context.Background(), nodeScope)
	if err != nil || len(lockedNodes) != 2 || lockedNodes[0] != nodeA || lockedNodes[1] != nodeB {
		t.Fatalf("LockRuntimeNodesForPrincipalRevocation = %#v, %v", lockedNodes, err)
	}
	requireSQLName(t, dbtx.querySQL, "LockRuntimeNodesForPrincipalRevocation")
	if !nodeRows.closed || !strings.Contains(dbtx.querySQL, "ORDER BY node_id ASC\nFOR UPDATE") {
		t.Fatalf("Node revocation locks must be ordered by node_id: %s", dbtx.querySQL)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{nodeScope}) {
		t.Fatalf("LockRuntimeNodesForPrincipalRevocation args = %#v", dbtx.queryArgs)
	}

	tokenRows := &fakeRows{rows: [][]any{{tokenA}, {tokenB}}}
	dbtx.queryRows = tokenRows
	lockedTokens, err := q.LockAgentTokensForPrincipalRevocation(context.Background(), tokenScope)
	if err != nil || len(lockedTokens) != 2 || lockedTokens[0] != tokenA || lockedTokens[1] != tokenB {
		t.Fatalf("LockAgentTokensForPrincipalRevocation = %#v, %v", lockedTokens, err)
	}
	requireSQLName(t, dbtx.querySQL, "LockAgentTokensForPrincipalRevocation")
	if !tokenRows.closed || !strings.Contains(dbtx.querySQL, "ORDER BY id ASC\nFOR UPDATE") {
		t.Fatalf("Token revocation locks must be ordered by token id: %s", dbtx.querySQL)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{tokenScope}) {
		t.Fatalf("LockAgentTokensForPrincipalRevocation args = %#v", dbtx.queryArgs)
	}

	attachmentRows := &fakeRows{rows: [][]any{{attachmentA}, {attachmentB}}}
	dbtx.queryRows = attachmentRows
	sessionScope := []uuid.UUID{sessionB, sessionA}
	lockedAttachments, err := q.LockActiveRuntimeSessionAttachmentsForPrincipalRevocation(context.Background(), sessionScope)
	if err != nil || len(lockedAttachments) != 2 || lockedAttachments[0] != attachmentA || lockedAttachments[1] != attachmentB {
		t.Fatalf("LockActiveRuntimeSessionAttachmentsForPrincipalRevocation = %#v, %v", lockedAttachments, err)
	}
	requireSQLName(t, dbtx.querySQL, "LockActiveRuntimeSessionAttachmentsForPrincipalRevocation")
	if !attachmentRows.closed ||
		!strings.Contains(dbtx.querySQL, "detached_at IS NULL") ||
		!strings.Contains(dbtx.querySQL, "ORDER BY id ASC\nFOR UPDATE") {
		t.Fatalf("Attachment revocation locks must cover active rows in attachment id order: %s", dbtx.querySQL)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{sessionScope}) {
		t.Fatalf("LockActiveRuntimeSessionAttachmentsForPrincipalRevocation args = %#v", dbtx.queryArgs)
	}
}

func ptrTime(value time.Time) *time.Time { return &value }
