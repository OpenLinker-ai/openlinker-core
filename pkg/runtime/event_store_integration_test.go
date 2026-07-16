package runtime_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestEventStoreReliableAppendAndContinuity(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	store := runtime.NewEventStore(pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)

	firstRequest := runtime.RuntimeEventRequest{
		ClientEventID:  uuid.New(),
		ClientEventSeq: 1,
		EventType:      "run.message.delta",
		Payload: map[string]any{
			"text":  "one durable event",
			"index": 1,
		},
	}

	const workers = 100
	type appendResult struct {
		ack runtime.RuntimeEventAck
		err error
	}
	results := make(chan appendResult, workers)
	start := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			<-start
			ack, err := store.Append(ctx, fixture.principal, fixture.identity, firstRequest)
			results <- appendResult{ack: ack, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	inserted, replayed := 0, 0
	var firstAck runtime.RuntimeEventAck
	for result := range results {
		require.NoError(t, result.err)
		if firstAck.EventID == uuid.Nil {
			firstAck = result.ack
		}
		require.Equal(t, firstAck.EventID, result.ack.EventID)
		require.Equal(t, firstAck.Sequence, result.ack.Sequence)
		if result.ack.Inserted {
			inserted++
		} else {
			replayed++
			require.True(t, result.ack.Replayed)
		}
	}
	require.Equal(t, 1, inserted)
	require.Equal(t, workers-1, replayed)
	require.Equal(t, int32(1), firstAck.Sequence)

	var eventCount int
	var lastClientEventSeq int64
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT
			(SELECT COUNT(*) FROM run_events WHERE run_id = $1 AND client_event_id = $2),
			(SELECT last_client_event_seq FROM run_attempts WHERE run_id = $1 AND id = $3)`,
		fixture.identity.RunID, firstRequest.ClientEventID, fixture.identity.AttemptID,
	).Scan(&eventCount, &lastClientEventSeq))
	require.Equal(t, 1, eventCount)
	require.Equal(t, int64(1), lastClientEventSeq)

	t.Run("Agent ownership precedes stored event lookup", func(t *testing.T) {
		wrongPrincipal := fixture.principal
		wrongPrincipal.AgentID = uuid.New()
		conflicting := firstRequest
		conflicting.Payload = map[string]any{"text": "probe a stored event"}
		_, err := store.Append(context.Background(), wrongPrincipal, fixture.identity, conflicting)
		require.True(t, runtime.IsRuntimeEventError(err, runtime.RuntimeEventErrorLeaseIdentityMismatch), "%v", err)

		wrongIdentity := fixture.identity
		wrongIdentity.AgentID = uuid.New()
		_, err = store.Append(context.Background(), fixture.principal, wrongIdentity, conflicting)
		require.True(t, runtime.IsRuntimeEventError(err, runtime.RuntimeEventErrorLeaseIdentityMismatch), "%v", err)
	})

	t.Run("event ID and client sequence conflicts are stable", func(t *testing.T) {
		conflictingPayload := firstRequest
		conflictingPayload.Payload = map[string]any{"text": "different"}
		_, err := store.Append(context.Background(), fixture.principal, fixture.identity, conflictingPayload)
		require.True(t, runtime.IsRuntimeEventError(err, runtime.RuntimeEventErrorIDConflict), "%v", err)

		conflictingSequence := firstRequest
		conflictingSequence.ClientEventID = uuid.New()
		_, err = store.Append(context.Background(), fixture.principal, fixture.identity, conflictingSequence)
		require.True(t, runtime.IsRuntimeEventError(err, runtime.RuntimeEventErrorIDConflict), "%v", err)

		staleIdentity := fixture.identity
		staleIdentity.LeaseID = uuid.New()
		newRequest := eventStoreRequest(2)
		_, err = store.Append(context.Background(), fixture.principal, staleIdentity, newRequest)
		require.True(t, runtime.IsRuntimeEventError(err, runtime.RuntimeEventErrorStaleLease), "%v", err)
	})

	t.Run("missing ranges are exact without sequence expansion", func(t *testing.T) {
		for _, sequence := range []int64{3, 6} {
			ack, err := store.Append(context.Background(), fixture.principal, fixture.identity, eventStoreRequest(sequence))
			require.NoError(t, err)
			require.True(t, ack.Inserted)
		}

		missing, err := store.MissingClientEventRanges(
			context.Background(), fixture.identity.RunID, fixture.identity.AttemptID, 7,
		)
		require.NoError(t, err)
		require.Equal(t, []runtime.EventRange{
			{Start: 2, End: 2},
			{Start: 4, End: 5},
			{Start: 7, End: 7},
		}, missing)

		err = store.RequireCompleteClientEvents(
			context.Background(), fixture.identity.RunID, fixture.identity.AttemptID, 7,
		)
		var eventErr *runtime.RuntimeEventError
		require.ErrorAs(t, err, &eventErr)
		require.Equal(t, runtime.RuntimeEventErrorEventsMissing, eventErr.Code)
		require.Equal(t, missing, eventErr.MissingRanges)

		for _, sequence := range []int64{2, 4, 5, 7} {
			_, err := store.Append(context.Background(), fixture.principal, fixture.identity, eventStoreRequest(sequence))
			require.NoError(t, err)
		}
		missing, err = store.MissingClientEventRanges(
			context.Background(), fixture.identity.RunID, fixture.identity.AttemptID, 7,
		)
		require.NoError(t, err)
		require.Empty(t, missing)
		require.NoError(t, store.RequireCompleteClientEvents(
			context.Background(), fixture.identity.RunID, fixture.identity.AttemptID, 7,
		))
	})

	t.Run("stored replay wins after database lease expiry", func(t *testing.T) {
		shortFixture := insertEventStoreExecutingAttempt(t, pool, time.Second)
		request := eventStoreRequest(1)
		created, err := store.Append(context.Background(), shortFixture.principal, shortFixture.identity, request)
		require.NoError(t, err)
		require.True(t, created.Inserted)

		require.Eventually(t, func() bool {
			var expired bool
			err := pool.QueryRow(context.Background(), `SELECT clock_timestamp() >= $1`, shortFixture.leaseExpiresAt).Scan(&expired)
			return err == nil && expired
		}, 3*time.Second, 20*time.Millisecond)

		replayedAck, err := store.Append(context.Background(), shortFixture.principal, shortFixture.identity, request)
		require.NoError(t, err)
		require.True(t, replayedAck.Replayed)
		require.False(t, replayedAck.Inserted)
		require.Equal(t, created.EventID, replayedAck.EventID)
		require.Equal(t, created.Sequence, replayedAck.Sequence)

		_, err = store.Append(context.Background(), shortFixture.principal, shortFixture.identity, eventStoreRequest(2))
		require.True(t, runtime.IsRuntimeEventError(err, runtime.RuntimeEventErrorLeaseExpired), "%v", err)
	})

	t.Run("stored replay wins after fence rotation", func(t *testing.T) {
		oldFixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
		oldRequest := eventStoreRequest(1)
		oldAck, err := store.Append(context.Background(), oldFixture.principal, oldFixture.identity, oldRequest)
		require.NoError(t, err)
		require.True(t, oldAck.Inserted)

		newIdentity := rotateEventStoreAttempt(t, pool, oldFixture)

		_, err = store.Append(context.Background(), oldFixture.principal, oldFixture.identity, eventStoreRequest(2))
		require.True(t, runtime.IsRuntimeEventError(err, runtime.RuntimeEventErrorStaleLease), "%v", err)

		replayed, err := store.Append(context.Background(), oldFixture.principal, oldFixture.identity, oldRequest)
		require.NoError(t, err)
		require.True(t, replayed.Replayed)
		require.False(t, replayed.Inserted)
		require.Equal(t, oldAck.EventID, replayed.EventID)
		require.Equal(t, oldAck.Sequence, replayed.Sequence)

		newAck, err := store.Append(context.Background(), oldFixture.principal, newIdentity, eventStoreRequest(1))
		require.NoError(t, err)
		require.True(t, newAck.Inserted)
		require.Greater(t, newAck.Sequence, oldAck.Sequence)
	})

	t.Run("service projections run only for the insert winner", func(t *testing.T) {
		projectionFixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
		svc := newTestService(t, pool)
		callbackEvents := &recordingRuntimeTaskCallbackEnqueuer{events: make(chan db.RunEvent, 2)}
		svc.SetTaskCallbackEnqueuer(callbackEvents)
		request := runtime.RuntimeEventRequest{
			ClientEventID:  uuid.New(),
			ClientEventSeq: 1,
			EventType:      "run.message.delta",
			Payload:        map[string]any{"text": "project exactly once"},
		}

		created, err := svc.AppendRuntimeEvent(
			context.Background(), projectionFixture.principal, projectionFixture.identity, request,
		)
		require.NoError(t, err)
		require.True(t, created.Inserted)
		select {
		case event := <-callbackEvents.events:
			require.Equal(t, created.EventID, event.ID)
		case <-time.After(time.Second):
			t.Fatal("inserted runtime event did not trigger its callback projection")
		}

		replayed, err := svc.AppendRuntimeEvent(
			context.Background(), projectionFixture.principal, projectionFixture.identity, request,
		)
		require.NoError(t, err)
		require.True(t, replayed.Replayed)
		require.False(t, replayed.Inserted)
		select {
		case duplicate := <-callbackEvents.events:
			t.Fatalf("event replay duplicated callback projection: %#v", duplicate)
		case <-time.After(100 * time.Millisecond):
		}

		var messageCount int
		require.NoError(t, pool.QueryRow(context.Background(), `
			SELECT COUNT(*)
			FROM run_messages
			WHERE run_id = $1 AND event_sequence = $2`,
			projectionFixture.identity.RunID, created.Sequence,
		).Scan(&messageCount))
		require.Equal(t, 1, messageCount)

		artifactRequest := runtime.RuntimeEventRequest{
			ClientEventID:  uuid.New(),
			ClientEventSeq: 2,
			EventType:      "run.artifact.delta",
			Payload: map[string]any{
				"artifact_id":   "projected-artifact",
				"artifact_type": "text",
				"title":         "Projected artifact",
				"append":        true,
				"last_chunk":    true,
				"parts":         []any{map[string]any{"type": "text", "text": "artifact body"}},
			},
		}
		artifactAck, err := svc.AppendRuntimeEvent(
			context.Background(), projectionFixture.principal, projectionFixture.identity, artifactRequest,
		)
		require.NoError(t, err)
		require.True(t, artifactAck.Inserted)
		replayedArtifact, err := svc.AppendRuntimeEvent(
			context.Background(), projectionFixture.principal, projectionFixture.identity, artifactRequest,
		)
		require.NoError(t, err)
		require.True(t, replayedArtifact.Replayed)
		require.False(t, replayedArtifact.Inserted)

		var artifactCount, chunkCount int
		require.NoError(t, pool.QueryRow(context.Background(), `
			SELECT COUNT(*)
			FROM run_artifacts
			WHERE run_id = $1 AND source_artifact_id = 'projected-artifact'`,
			projectionFixture.identity.RunID,
		).Scan(&artifactCount))
		require.Equal(t, 1, artifactCount)
		require.NoError(t, pool.QueryRow(context.Background(), `
			SELECT COUNT(*)
			FROM run_artifact_chunks
			WHERE run_id = $1 AND event_sequence = $2`,
			projectionFixture.identity.RunID, artifactAck.Sequence,
		).Scan(&chunkCount))
		require.Equal(t, 1, chunkCount)
	})
}

func TestRuntimeSessionReaperClosesCrashedPullGenerationWithoutFinishingAttempt(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	require.NotNil(t, fixture.identity.RuntimeSessionID)
	sessionID := *fixture.identity.RuntimeSessionID

	// The immutable clock trigger correctly forbids manufacturing staleness by
	// moving heartbeat_at backwards. A millisecond liveness window makes the
	// already-committed fixture stale without weakening that production fence.
	time.Sleep(2 * time.Millisecond)
	reaped, err := runtime.NewRuntimeSessionReaper(pool, time.Millisecond).ReapStaleSessions(context.Background(), 32)
	require.NoError(t, err)
	require.Equal(t, 1, reaped)

	var sessionStatus, dispatchState string
	var attachedCoreID *uuid.UUID
	var detachedAt *time.Time
	var attemptFinishedAt *time.Time
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT session.status, session.attached_core_instance_id,
		       attachment.detached_at, run.dispatch_state, attempt.finished_at
		FROM runtime_sessions session
		JOIN runtime_session_attachments attachment
		  ON attachment.runtime_session_id = session.runtime_session_id
		JOIN runs run ON run.id = $2
		JOIN run_attempts attempt ON attempt.id = $3
		WHERE session.runtime_session_id = $1`,
		sessionID, fixture.identity.RunID, fixture.identity.AttemptID,
	).Scan(&sessionStatus, &attachedCoreID, &detachedAt, &dispatchState, &attemptFinishedAt))
	require.Equal(t, "offline", sessionStatus)
	require.Nil(t, attachedCoreID)
	require.NotNil(t, detachedAt)
	require.Equal(t, "executing", dispatchState)
	require.Nil(t, attemptFinishedAt)
}

func eventStoreRequest(sequence int64) runtime.RuntimeEventRequest {
	return runtime.RuntimeEventRequest{
		ClientEventID:  uuid.New(),
		ClientEventSeq: sequence,
		EventType:      "run.progress.changed",
		Payload: map[string]any{
			"sequence": sequence,
		},
	}
}

type eventStoreFixture struct {
	identity       runtime.RuntimeAttemptIdentity
	principal      runtime.RuntimeEventPrincipal
	leaseExpiresAt time.Time
	credentialID   uuid.UUID
	coreInstanceID uuid.UUID
}

func insertEventStoreExecutingAttempt(t *testing.T, pool *pgxpool.Pool, leaseTTL time.Duration) eventStoreFixture {
	t.Helper()
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/event-store", 0, "approved")

	runID := uuid.New()
	attemptID := uuid.New()
	leaseID := uuid.New()
	nodeID := uuid.New()
	sessionID := uuid.New()
	attachmentID := uuid.New()
	credentialID := uuid.New()
	coreInstanceID := uuid.New()
	workerID := "event-worker-" + uuid.NewString()[:8]
	certificateSerial := strings.ReplaceAll(nodeID.String(), "-", "")
	publicKeyThumbprint := fmt.Sprintf("%x", sha256.Sum256([]byte("event-store-node/"+nodeID.String())))
	features := []string{
		"lease_fence",
		"assignment_confirm",
		"renew",
		"resume",
		"event_ack",
		"result_ack",
		"cancel",
		"persistent_spool",
		"session_drain",
	}
	keyHash := sha256.Sum256([]byte("event-store-key/" + runID.String()))
	fingerprint := sha256.Sum256([]byte("event-store-fingerprint/" + runID.String()))
	prefix := "ol_agent_" + strings.ReplaceAll(credentialID.String(), "-", "")[:12]

	var leaseExpiresAt time.Time
	var contractDigest string
	err := pgx.BeginTxFunc(context.Background(), pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		var databaseNow time.Time
		if err := tx.QueryRow(context.Background(), `
			SELECT clock_timestamp(), runtime_contract_digest
			FROM runtime_schema_contracts
			WHERE runtime_contract_id = 'openlinker.runtime.v2' AND is_current`).Scan(&databaseNow, &contractDigest); err != nil {
			return err
		}

		offeredAt := databaseNow
		acceptedAt := offeredAt
		offerExpiresAt := offeredAt.Add(30 * time.Second)
		leaseExpiresAt = offeredAt.Add(leaseTTL)
		attemptDeadlineAt := offeredAt.Add(5 * time.Minute)
		dispatchDeadlineAt := offeredAt.Add(time.Minute)
		runDeadlineAt := offeredAt.Add(10 * time.Minute)

		if _, err := tx.Exec(context.Background(), `
			INSERT INTO agent_tokens (
				id, agent_id, creator_user_id, name, prefix, token_hash, scopes,
				status, redeemed_at
			) VALUES ($1, $2, $3, 'Event Store credential', $4, 'test-hash',
				ARRAY['agent:call','agent:pull']::text[], 'active_runtime', $5)`,
			credentialID, agentID, creatorID, prefix, databaseNow,
		); err != nil {
			return fmt.Errorf("insert event-store credential: %w", err)
		}
		if _, err := tx.Exec(context.Background(), `
			INSERT INTO runtime_nodes (
				node_id, display_name, device_certificate_serial,
				device_public_key_thumbprint, node_version, protocol_version,
				runtime_contract_id, runtime_contract_digest, features,
				capacity, inflight, status, last_seen_at
			) VALUES ($1, 'Event Store Node', $2, $3, 'test-v2', 2,
				'openlinker.runtime.v2', $4, $5, 1, 1, 'active', $6)`,
			nodeID,
			certificateSerial,
			publicKeyThumbprint,
			contractDigest,
			features,
			databaseNow,
		); err != nil {
			return fmt.Errorf("insert event-store node: %w", err)
		}
		if _, err := tx.Exec(context.Background(), `
			INSERT INTO runtime_sessions (
				runtime_session_id, node_id, agent_id, credential_id, worker_id,
				session_epoch, device_certificate_serial, node_version,
				protocol_version, runtime_contract_id, runtime_contract_digest,
				features, capacity, inflight, status, attached_core_instance_id,
				connected_at, heartbeat_at
			) VALUES ($1, $2, $3, $4, $5, 1, $6, 'test-v2', 2,
				'openlinker.runtime.v2', $7, $8, 1, 1, 'active', $9, $10, $10)`,
			sessionID,
			nodeID,
			agentID,
			credentialID,
			workerID,
			certificateSerial,
			contractDigest,
			features,
			coreInstanceID,
			databaseNow,
		); err != nil {
			return fmt.Errorf("insert event-store session: %w", err)
		}
		if _, err := tx.Exec(context.Background(), `
			INSERT INTO runtime_session_attachments (
				id, runtime_session_id, core_instance_id, attachment_kind, attached_at
			) VALUES ($1, $2, $3, 'connected', $4)`, attachmentID, sessionID, coreInstanceID, databaseNow); err != nil {
			return fmt.Errorf("insert event-store session attachment: %w", err)
		}

		if _, err := tx.Exec(context.Background(), `
			INSERT INTO runs (
				id, user_id, agent_id, input, status,
				cost_cents, platform_fee_cents, creator_revenue_cents, source,
				runtime_contract_id, idempotency_key_hash, idempotency_fingerprint,
				request_metadata, connection_mode_snapshot, endpoint_idempotency_snapshot,
				dispatch_state, max_offer_count, max_attempts,
				dispatch_deadline_at, run_deadline_at
			) VALUES (
				$1, $2, $3, '{}'::jsonb, 'running',
				0, 0, 0, 'api',
				'openlinker.runtime.v2', $4, $5,
				'{}'::jsonb, 'runtime', NULL,
				'pending', 20, 3, $6, $7
			)`,
			runID,
			userID,
			agentID,
			keyHash[:],
			fingerprint[:],
			dispatchDeadlineAt,
			runDeadlineAt,
		); err != nil {
			return fmt.Errorf("insert event-store run: %w", err)
		}
		if _, err := tx.Exec(context.Background(), `
			INSERT INTO run_attempts (
				id, run_id, agent_id, offer_no, attempt_no, executor_type,
				lease_id, fencing_token, runtime_token_id, runtime_worker_id,
				runtime_session_id, node_id, offered_by_core_instance_id,
				attached_core_instance_id, offered_at, offer_expires_at,
				accepted_at, lease_expires_at, attempt_deadline_at,
				slot_acquired_at, active_runtime_session_id
			) VALUES (
				$1, $2, $3, 1, 1, 'runtime',
				$4, 1, $5, $6, $7, $8, $9, $9, $10, $11, $10, $12, $13,
				$10, $7
			)`,
			attemptID,
			runID,
			agentID,
			leaseID,
			credentialID,
			workerID,
			sessionID,
			nodeID,
			coreInstanceID,
			offeredAt,
			offerExpiresAt,
			leaseExpiresAt,
			attemptDeadlineAt,
		); err != nil {
			return fmt.Errorf("insert event-store attempt: %w", err)
		}
		if _, err := tx.Exec(context.Background(), `
			UPDATE runs
			SET dispatch_state = 'executing',
				offer_count = 1,
				attempt_count = 1,
				latest_attempt_id = $2,
				active_attempt_id = $2,
				lease_id = $3,
				fencing_token = 1,
				executor_type = 'runtime',
				active_core_instance_id = $4,
				runtime_node_id = $5,
				runtime_worker_id = $6,
				runtime_session_id = $7,
				lease_token_id = $8,
				lease_offered_at = $9,
				lease_accepted_at = $9,
				lease_expires_at = $10,
				attempt_deadline_at = $11
			WHERE id = $1`,
			runID,
			attemptID,
			leaseID,
			coreInstanceID,
			nodeID,
			workerID,
			sessionID,
			credentialID,
			acceptedAt,
			leaseExpiresAt,
			attemptDeadlineAt,
		); err != nil {
			return fmt.Errorf("activate event-store attempt: %w", err)
		}
		return nil
	})
	require.NoError(t, err)

	identity := runtime.RuntimeAttemptIdentity{
		RunID:            runID,
		AttemptID:        attemptID,
		LeaseID:          leaseID,
		FencingToken:     1,
		NodeID:           &nodeID,
		AgentID:          agentID,
		WorkerID:         &workerID,
		RuntimeSessionID: &sessionID,
	}
	return eventStoreFixture{
		identity: identity,
		principal: runtime.RuntimeEventPrincipal{
			AgentID:                         agentID,
			RuntimeContractDigest:           contractDigest,
			CredentialID:                    &credentialID,
			NodeID:                          &nodeID,
			WorkerID:                        &workerID,
			RuntimeSessionID:                &sessionID,
			CoreInstanceID:                  &coreInstanceID,
			AttachmentID:                    &attachmentID,
			DeviceCertificateSerial:         &certificateSerial,
			DevicePublicKeyThumbprintSHA256: &publicKeyThumbprint,
		},
		leaseExpiresAt: leaseExpiresAt,
		credentialID:   credentialID,
		coreInstanceID: coreInstanceID,
	}
}

func rotateEventStoreAttempt(t *testing.T, pool *pgxpool.Pool, fixture eventStoreFixture) runtime.RuntimeAttemptIdentity {
	t.Helper()
	newAttemptID := uuid.New()
	newLeaseID := uuid.New()
	resultID := uuid.New()
	resultFingerprint := sha256.Sum256([]byte("retryable-result/" + resultID.String()))

	err := pgx.BeginTxFunc(context.Background(), pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		var databaseNow time.Time
		var runDeadlineAt time.Time
		if err := tx.QueryRow(context.Background(), `
			SELECT clock_timestamp(), run_deadline_at
			FROM runs
			WHERE id = $1
			FOR UPDATE`, fixture.identity.RunID).Scan(&databaseNow, &runDeadlineAt); err != nil {
			return err
		}

		if _, err := tx.Exec(context.Background(), `
			UPDATE run_attempts
			SET finished_at = $5,
				outcome = 'retryable_failure',
				result_id = $6,
				result_fingerprint = $7,
				result_classification = 'retryable_failure',
				final_client_event_seq = last_client_event_seq,
				slot_released_at = $5,
				active_runtime_session_id = NULL
			WHERE run_id = $1
			  AND id = $2
			  AND lease_id = $3
			  AND fencing_token = $4`,
			fixture.identity.RunID,
			fixture.identity.AttemptID,
			fixture.identity.LeaseID,
			fixture.identity.FencingToken,
			databaseNow,
			resultID,
			resultFingerprint[:],
		); err != nil {
			return fmt.Errorf("finish old event-store attempt: %w", err)
		}
		if _, err := tx.Exec(context.Background(), `
			UPDATE runs
			SET dispatch_state = 'retry_wait',
				next_attempt_at = $2,
				active_attempt_id = NULL,
				lease_id = NULL,
				executor_type = NULL,
				active_core_instance_id = NULL,
				runtime_node_id = NULL,
				runtime_worker_id = NULL,
				runtime_session_id = NULL,
				lease_token_id = NULL,
				lease_offered_at = NULL,
				lease_accepted_at = NULL,
				lease_expires_at = NULL,
				attempt_deadline_at = NULL
			WHERE id = $1`, fixture.identity.RunID, databaseNow); err != nil {
			return fmt.Errorf("move event-store run to retry_wait: %w", err)
		}

		offeredAt := databaseNow
		leaseExpiresAt := databaseNow.Add(5 * time.Minute)
		attemptDeadlineAt := databaseNow.Add(5 * time.Minute)
		if !attemptDeadlineAt.Before(runDeadlineAt) {
			attemptDeadlineAt = runDeadlineAt.Add(-time.Second)
		}
		if leaseExpiresAt.After(attemptDeadlineAt) {
			leaseExpiresAt = attemptDeadlineAt
		}
		if _, err := tx.Exec(context.Background(), `
			INSERT INTO run_attempts (
				id, run_id, agent_id, offer_no, attempt_no, executor_type,
				lease_id, fencing_token, runtime_token_id, runtime_worker_id,
				runtime_session_id, node_id, offered_by_core_instance_id,
				attached_core_instance_id, offered_at, offer_expires_at,
				accepted_at, lease_expires_at, attempt_deadline_at,
				slot_acquired_at, active_runtime_session_id
			) VALUES (
				$1, $2, $3, 2, 2, 'runtime',
				$4, 2, $5, $6, $7, $8, $9, $9, $10, $11, $10, $12, $13,
				$10, $7
			)`,
			newAttemptID,
			fixture.identity.RunID,
			fixture.identity.AgentID,
			newLeaseID,
			fixture.credentialID,
			*fixture.identity.WorkerID,
			*fixture.identity.RuntimeSessionID,
			*fixture.identity.NodeID,
			fixture.coreInstanceID,
			offeredAt,
			offeredAt.Add(30*time.Second),
			leaseExpiresAt,
			attemptDeadlineAt,
		); err != nil {
			return fmt.Errorf("insert rotated event-store attempt: %w", err)
		}
		if _, err := tx.Exec(context.Background(), `
			UPDATE runs
			SET dispatch_state = 'executing',
				next_attempt_at = NULL,
				offer_count = 2,
				attempt_count = 2,
				latest_attempt_id = $2,
				active_attempt_id = $2,
				lease_id = $3,
				fencing_token = 2,
				executor_type = 'runtime',
				active_core_instance_id = $4,
				runtime_node_id = $5,
				runtime_worker_id = $6,
				runtime_session_id = $7,
				lease_token_id = $8,
				lease_offered_at = $9,
				lease_accepted_at = $9,
				lease_expires_at = $10,
				attempt_deadline_at = $11
			WHERE id = $1`,
			fixture.identity.RunID,
			newAttemptID,
			newLeaseID,
			fixture.coreInstanceID,
			*fixture.identity.NodeID,
			*fixture.identity.WorkerID,
			*fixture.identity.RuntimeSessionID,
			fixture.credentialID,
			offeredAt,
			leaseExpiresAt,
			attemptDeadlineAt,
		); err != nil {
			return fmt.Errorf("activate rotated event-store attempt: %w", err)
		}
		return nil
	})
	require.NoError(t, err)

	identity := fixture.identity
	identity.AttemptID = newAttemptID
	identity.LeaseID = newLeaseID
	identity.FencingToken = 2
	return identity
}
