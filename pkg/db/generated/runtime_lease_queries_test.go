package db

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRuntimePrincipalValidationUsesStagedBlockingLocks(t *testing.T) {
	t.Parallel()

	for _, check := range []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "session first",
			query: lockRuntimeSessionForPrincipalValidation,
			want: []string{
				"s.runtime_session_id = $1", "s.node_id = $2",
				"s.agent_id = $3", "s.credential_id = $4",
				"s.worker_id = $5", "s.attached_core_instance_id = $6",
				"FOR UPDATE OF s",
			},
		},
		{
			name:  "node second",
			query: lockRuntimeNodeForPrincipalValidation,
			want: []string{
				"n.device_certificate_serial = $2",
				"n.device_public_key_thumbprint = $3",
				"n.revoked_at IS NULL", "FOR UPDATE OF n",
			},
		},
		{
			name:  "credential third",
			query: lockRuntimeCredentialForPrincipalValidation,
			want: []string{
				"t.id = $1", "t.agent_id = $2", "t.status = 'active_runtime'",
				"t.revoked_at IS NULL", "t.expires_at > clock_timestamp()",
				"FOR SHARE OF t",
			},
		},
		{
			name:  "attachment fourth",
			query: lockRuntimeSessionAttachmentForPrincipalValidation,
			want: []string{
				"id = $1", "runtime_session_id = $2", "core_instance_id = $3",
				"detached_at IS NULL", "FOR UPDATE",
			},
		},
	} {
		check := check
		t.Run(check.name, func(t *testing.T) {
			for _, fragment := range check.want {
				if !strings.Contains(check.query, fragment) {
					t.Fatalf("query missing %q", fragment)
				}
			}
		})
	}
}

func TestEveryRuntimeSessionEntryPointRequiresPullScope(t *testing.T) {
	t.Parallel()

	for name, query := range map[string]string{
		"create":    createRuntimeSession,
		"claim":     claimRuntimeSessionForCore,
		"heartbeat": heartbeatRuntimeSession,
		"slot":      claimRuntimeSessionSlot,
	} {
		if !strings.Contains(query, "t.scopes @> ARRAY['agent:pull']::text[]") {
			t.Fatalf("%s Session query does not fail closed on agent:pull scope", name)
		}
	}
}

func TestResolveRuntimeWorkerSessionPrincipalIsFailClosed(t *testing.T) {
	t.Parallel()

	for _, fragment := range []string{
		"s.node_id = $1", "s.agent_id = $2", "s.credential_id = $3",
		"s.worker_id = $4", "s.device_certificate_serial = $5",
		"n.device_public_key_thumbprint = $6",
		"s.attached_core_instance_id = $7",
		"s.status IN ('active', 'draining')",
		"n.status IN ('active', 'draining')", "n.revoked_at IS NULL",
		"t.status = 'active_runtime'", "t.revoked_at IS NULL",
		"t.scopes @> ARRAY['agent:pull']::text[]",
		"t.expires_at > clock_timestamp()",
		"clock_timestamp() AS database_now",
	} {
		if !strings.Contains(resolveRuntimeWorkerSessionPrincipal, fragment) {
			t.Fatalf("ResolveRuntimeWorkerSessionPrincipal missing %q", fragment)
		}
	}

	now := time.Date(2026, 7, 11, 15, 0, 0, 0, time.UTC)
	sessionID, nodeID, agentID := uuid.New(), uuid.New(), uuid.New()
	credentialID, coreID := uuid.New(), uuid.New()
	attachmentID := uuid.New()
	features := append([]string(nil), runtimeRequiredFeatures...)
	dbtx := &fakeDBTX{row: fakeRow{values: []any{
		sessionID, nodeID, agentID, credentialID, "worker-resolve", int64(7), coreID,
		"cert-resolve", "spki-resolve", "v2", int32(2),
		"openlinker.runtime.v2", runtimeContractDigest, features,
		"active", now, attachmentID, now,
	}}}
	q := New(dbtx)
	principal, err := q.ResolveRuntimeWorkerSessionPrincipal(context.Background(), ResolveRuntimeWorkerSessionPrincipalParams{
		NodeID: nodeID, AgentID: agentID, CredentialID: credentialID,
		WorkerID: "worker-resolve", DeviceCertificateSerial: "cert-resolve",
		DevicePublicKeyThumbprint: "spki-resolve", CoreInstanceID: coreID,
	})
	if err != nil || principal.RuntimeSessionID != sessionID || principal.SessionEpoch != 7 ||
		principal.AttachmentID != attachmentID || principal.DatabaseNow != now {
		t.Fatalf("ResolveRuntimeWorkerSessionPrincipal = %#v, %v", principal, err)
	}
	if len(dbtx.queryRowArgs) != 7 || dbtx.queryRowArgs[6] != coreID {
		t.Fatalf("ResolveRuntimeWorkerSessionPrincipal args = %#v", dbtx.queryRowArgs)
	}
}

func TestRuntimeOfferReleaseSupportsOnlyExactDetachedSession(t *testing.T) {
	t.Parallel()

	for _, fragment := range []string{
		"s.status IN ('offline', 'closed')",
		"s.attached_core_instance_id IS NULL",
		"FOR UPDATE OF s",
	} {
		if !strings.Contains(lockRuntimeSessionForOfferRelease, fragment) {
			t.Fatalf("LockRuntimeSessionForOfferRelease missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"id = $1", "runtime_session_id = $2", "core_instance_id = $3",
		"$4::boolean AND detached_at IS NOT NULL",
		"NOT $4::boolean AND detached_at IS NULL", "FOR UPDATE",
	} {
		if !strings.Contains(lockRuntimeSessionAttachmentForOfferRelease, fragment) {
			t.Fatalf("LockRuntimeSessionAttachmentForOfferRelease missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"a.accepted_at IS NULL",
		"a.attempt_no IS NULL",
		"a.attached_core_instance_id = $12",
		"detached.core_instance_id = $12",
		"detached.detached_at IS NOT NULL",
	} {
		if !strings.Contains(finishUnacceptedRunOffer, fragment) {
			t.Fatalf("FinishUnacceptedRunOffer detached cleanup missing %q", fragment)
		}
	}
}

func TestRuntimeOfferAndLeaseQueriesAreFencedAndDatabaseTimed(t *testing.T) {
	t.Parallel()

	for _, fragment := range []string{
		"a.offer_expires_at > clock_timestamp()",
		"r.dispatch_deadline_at > clock_timestamp()",
		"r.run_deadline_at > clock_timestamp()",
		"FOR UPDATE OF r",
		"FOR UPDATE OF a",
	} {
		if !strings.Contains(getExistingUnacceptedRunOfferForSession, fragment) {
			t.Fatalf("existing-offer query missing %q", fragment)
		}
	}

	for _, fragment := range []string{
		"r.agent_id = $1",
		"r.connection_mode_snapshot = 'runtime'",
		"r.dispatch_state = 'pending'",
		"r.next_attempt_at <= clock_timestamp()",
		"r.offer_count < r.max_offer_count",
		"r.attempt_count < r.max_attempts",
		"r.dispatch_deadline_at > clock_timestamp()",
		"r.run_deadline_at > clock_timestamp()",
		"FOR UPDATE OF r SKIP LOCKED",
	} {
		if !strings.Contains(lockNextClaimableRuntimeRunForAgent, fragment) {
			t.Fatalf("claim query missing %q", fragment)
		}
	}

	for _, fragment := range []string{
		"WITH database_clock AS MATERIALIZED",
		"r.offer_count + 1",
		"r.fencing_token + 1",
		"s.attached_core_instance_id = $3",
		"t.expires_at > c.database_now",
		"outstanding.accepted_at IS NULL",
		"outstanding.finished_at IS NULL",
	} {
		if !strings.Contains(createRuntimeRunOffer, fragment) {
			t.Fatalf("offer insert missing %q", fragment)
		}
	}

	for _, query := range []string{confirmRunAssignment, renewRuntimeRunAttempt} {
		if strings.Contains(query, "offer_expires_at >=") ||
			strings.Contains(query, "lease_expires_at >=") {
			t.Fatal("ACK/renew must reject the exact expiry boundary")
		}
		for _, fragment := range []string{
			"a.lease_id =", "a.fencing_token =", "a.runtime_session_id =",
			"a.node_id =", "a.runtime_token_id =", "a.runtime_worker_id =",
			"t.expires_at > c.database_now",
		} {
			if !strings.Contains(query, fragment) {
				t.Fatalf("lease mutation missing fence %q", fragment)
			}
		}
	}
	if !strings.Contains(confirmRunAssignment, "a.offer_expires_at > c.database_now") {
		t.Fatal("assignment ACK does not use strict database offer expiry")
	}
	for _, fragment := range []string{
		"runtime_attachment_id = attachment.id",
		"attachment.id = $11",
		"attachment.runtime_session_id = s.runtime_session_id",
		"attachment.core_instance_id = $2",
		"attachment.detached_at IS NULL",
		"attachment.transport IN ('websocket', 'long_poll')",
		"attachment.transport_reason IS NOT NULL",
	} {
		if !strings.Contains(confirmRunAssignment, fragment) {
			t.Fatalf("assignment ACK transport evidence missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"a.lease_expires_at > c.database_now",
		"r.lease_expires_at > c.database_now",
		"GREATEST(",
		"LEAST(",
	} {
		if !strings.Contains(renewRuntimeRunAttempt, fragment) {
			t.Fatalf("renew query missing %q", fragment)
		}
	}

	for _, fragment := range []string{
		"a.accepted_at IS NULL", "a.attempt_no IS NULL",
		"a.finished_at IS NULL", "IN ('offer_rejected', 'offer_expired')",
	} {
		if !strings.Contains(finishUnacceptedRunOffer, fragment) {
			t.Fatalf("finish-offer query missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"dispatch_state = 'pending'", "active_attempt_id = NULL",
		"lease_id = NULL", "runtime_session_id = NULL",
		"r.latest_attempt_id = a.id",
	} {
		if !strings.Contains(resetRunAfterUnacceptedOffer, fragment) {
			t.Fatalf("offer reset query missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"a.run_id = $1", "a.id = $2", "a.lease_id = $3",
		"a.fencing_token = $4", "a.slot_acquired_at IS NOT NULL",
		"a.slot_released_at IS NULL", "a.active_runtime_session_id IS NOT NULL",
		"SET slot_released_at = clock_timestamp()",
		"active_runtime_session_id = NULL", "FOR UPDATE OF a",
	} {
		if !strings.Contains(markRunAttemptCapacityReleased, fragment) {
			t.Fatalf("capacity release CAS missing %q", fragment)
		}
	}
}

func TestRuntimeResumeGrantQueriesSeparateActiveWritesFromStoredReplay(t *testing.T) {
	t.Parallel()

	for _, fragment := range []string{
		"target_token.rotation_predecessor_id = source_token.id",
		"source_token.revocation_kind = 'planned_rotation'",
		"source.status IN ('offline', 'closed')",
		"a.lease_expires_at > c.database_now",
		"r.run_deadline_at > c.database_now",
		"$2 = 'upload_spool_only'",
		"denied_grant.revoked_at IS NOT NULL",
		"denied_grant.revoke_reason = 'expired'",
	} {
		if !strings.Contains(createRuntimeResumeGrant, fragment) {
			t.Fatalf("resume grant creation missing %q", fragment)
		}
	}
	if !strings.Contains(createRuntimeResumeGrant, "ELSE c.database_now") {
		t.Fatal("spool-only grant must not be incorrectly bounded by the execution deadline")
	}

	for _, query := range []string{
		lockActiveRuntimeResumeGrant,
		lockActiveRuntimeResumeGrantForAttemptTarget,
	} {
		for _, fragment := range []string{
			"g.revoked_at IS NULL", "g.expires_at > clock_timestamp()",
			"target.attached_core_instance_id =", "FOR UPDATE OF g",
		} {
			if !strings.Contains(query, fragment) {
				t.Fatalf("active grant query missing %q", fragment)
			}
		}
	}
	for _, fragment := range []string{
		"g.first_used_at IS NOT NULL",
		"g.revoked_at IS NULL",
		"g.revoke_reason = 'expired'",
		"g.revoked_by_type = 'system'",
		"g.expires_at <= g.revoked_at",
		"FOR SHARE OF g",
	} {
		if !strings.Contains(lockConsumedRuntimeResumeGrantForStoredReplay, fragment) {
			t.Fatalf("stored-replay grant query missing %q", fragment)
		}
	}
	if strings.Contains(lockConsumedRuntimeResumeGrantForStoredReplay, "expires_at >") {
		t.Fatal("stored ACK replay must survive natural grant expiry")
	}
}

func TestRuntimeLeaseGeneratedMethodsScanAndPreserveArgumentOrder(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	attemptID, runID, agentID := uuid.New(), uuid.New(), uuid.New()
	leaseID, tokenID, sessionID := uuid.New(), uuid.New(), uuid.New()
	nodeID, coreID := uuid.New(), uuid.New()
	workerID := "worker-runtime-v2"
	attemptValues := []any{
		attemptID, runID, agentID, int32(1), (*int32)(nil), "runtime",
		leaseID, int64(1), &tokenID, &workerID, &sessionID, &nodeID,
		coreID, coreID, now, now.Add(30 * time.Second), (*time.Time)(nil),
		(*time.Time)(nil), now.Add(time.Minute), now.Add(5 * time.Minute),
		(*time.Time)(nil), (*string)(nil), (*uuid.UUID)(nil), []byte(nil),
		(*string)(nil), (*time.Time)(nil), int64(0), (*int64)(nil),
		(*string)(nil), (*string)(nil), now,
		&now, (*time.Time)(nil), &sessionID, (*uuid.UUID)(nil),
	}
	dbtx := &fakeDBTX{row: fakeRow{values: attemptValues}}
	q := New(dbtx)
	got, err := q.CreateRuntimeRunOffer(context.Background(), CreateRuntimeRunOfferParams{
		AttemptID: attemptID, LeaseID: leaseID, CoreInstanceID: coreID,
		OfferTtlMs: 30_000, LeaseTtlMs: 60_000, AttemptTtlMs: 300_000,
		RuntimeSessionID: sessionID, RunID: runID, NodeID: nodeID,
		CredentialID: tokenID, WorkerID: workerID,
	})
	if err != nil || got.ID != attemptID || got.RuntimeSessionID == nil ||
		*got.RuntimeSessionID != sessionID {
		t.Fatalf("CreateRuntimeRunOffer = %#v, %v", got, err)
	}
	wantArgs := []any{
		attemptID, leaseID, coreID, int64(30_000), int64(60_000),
		int64(300_000), sessionID, runID, nodeID, tokenID, workerID,
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, wantArgs) {
		t.Fatalf("CreateRuntimeRunOffer args = %#v", dbtx.queryRowArgs)
	}

	attachmentID := uuid.New()
	attemptNo := int32(1)
	acceptedAt := now
	attemptValues[4] = &attemptNo
	attemptValues[16] = &acceptedAt
	attemptValues[17] = &acceptedAt
	attemptValues[34] = &attachmentID
	dbtx.row = fakeRow{values: attemptValues}
	confirmed, err := q.ConfirmRunAssignment(context.Background(), ConfirmRunAssignmentParams{
		LeaseTtlMs: 60_000, CoreInstanceID: coreID, RunID: runID,
		AttemptID: attemptID, LeaseID: leaseID, FencingToken: 1,
		RuntimeSessionID: &sessionID, NodeID: &nodeID, CredentialID: &tokenID,
		WorkerID: &workerID, AttachmentID: attachmentID,
	})
	if err != nil || confirmed.RuntimeAttachmentID == nil || *confirmed.RuntimeAttachmentID != attachmentID {
		t.Fatalf("ConfirmRunAssignment = %#v, %v", confirmed, err)
	}
	wantConfirmArgs := []any{
		int64(60_000), coreID, runID, attemptID, leaseID, int64(1),
		&sessionID, &nodeID, &tokenID, &workerID, attachmentID,
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, wantConfirmArgs) {
		t.Fatalf("ConfirmRunAssignment args = %#v", dbtx.queryRowArgs)
	}

	grantID, targetSessionID := uuid.New(), uuid.New()
	permission := "continue_execution"
	grantValues := []any{
		grantID, runID, attemptID, leaseID, int64(1), agentID, nodeID,
		workerID, sessionID, tokenID, targetSessionID, tokenID, permission,
		coreID, now, now.Add(time.Minute), (*time.Time)(nil),
		(*time.Time)(nil), (*string)(nil), (*uuid.UUID)(nil), (*string)(nil),
	}
	dbtx.row = fakeRow{values: grantValues}
	grant, err := q.CreateRuntimeResumeGrant(context.Background(), CreateRuntimeResumeGrantParams{
		GrantID: grantID, Permission: permission, CoreInstanceID: coreID,
		GrantTtlMs: 60_000, TargetSessionID: targetSessionID, RunID: runID,
		AttemptID: attemptID, LeaseID: leaseID, FencingToken: 1,
	})
	if err != nil || grant.ID != grantID || grant.TargetSessionID != targetSessionID {
		t.Fatalf("CreateRuntimeResumeGrant = %#v, %v", grant, err)
	}
	if len(dbtx.queryRowArgs) != 9 || dbtx.queryRowArgs[1] != permission ||
		dbtx.queryRowArgs[4] != targetSessionID {
		t.Fatalf("CreateRuntimeResumeGrant args = %#v", dbtx.queryRowArgs)
	}
}

func TestRuntimeLeaseResumeMigrationShape(t *testing.T) {
	t.Parallel()

	up, err := os.ReadFile("../../../migrations/064_runtime_lease_resume_primitives.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile("../../../migrations/064_runtime_lease_resume_primitives.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	verify, err := os.ReadFile("../../../migrations/064_runtime_lease_resume_primitives_verify.sql")
	if err != nil {
		t.Fatal(err)
	}

	for _, fragment := range []string{
		"CREATE UNIQUE INDEX idx_run_attempts_unaccepted_session",
		"accepted_at IS NULL",
		"finished_at IS NULL",
		"CREATE TABLE runtime_resume_grants",
		"idx_runtime_resume_grants_unrevoked_attempt",
		"runtime_resume_grants_source_session_fk",
		"runtime_resume_grants_target_session_fk",
		"runtime_resume_grants_identity_immutable",
		"NOT ('agent:pull' = ANY(token_record.scopes))",
		"ADD COLUMN slot_acquired_at TIMESTAMPTZ",
		"ADD COLUMN slot_released_at TIMESTAMPTZ",
		"ADD COLUMN active_runtime_session_id UUID",
		"run_attempts_slot_evidence_forward_only",
		"run_attempts_slot_release_on_finish",
		"SET schema_version = 64",
		"migration_name = '064_runtime_lease_resume_primitives'",
	} {
		if !strings.Contains(string(up), fragment) {
			t.Fatalf("up migration missing %q", fragment)
		}
	}
	if !strings.Contains(string(down), "down refuses runtime resume grant evidence") {
		t.Fatal("down migration must not discard resume authorization evidence")
	}
	for _, fragment := range []string{
		"SET schema_version = 63",
		"migration_name = '063_reliable_runtime_v2'",
	} {
		if !strings.Contains(string(down), fragment) {
			t.Fatalf("down migration does not restore schema contract: %q", fragment)
		}
	}
	for _, fragment := range []string{
		"runtime resume grant columns are missing or mismatched",
		"single unaccepted Session offer index is missing or mismatched",
		"runtime resume grant identity trigger is missing",
		"runtime Session principal does not enforce agent:pull scope",
		"run Attempt slot evidence columns are missing or mismatched",
		"run Attempt slot evidence triggers are missing or mismatched",
		"runtime schema contract 64 is missing or mismatched",
	} {
		if !strings.Contains(string(verify), fragment) {
			t.Fatalf("verify migration missing %q", fragment)
		}
	}
}
