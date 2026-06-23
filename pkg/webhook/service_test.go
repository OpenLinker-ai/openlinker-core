package webhook

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

const truncateWebhookTables = "TRUNCATE run_webhook_deliveries, run_webhook_subscriptions, webhook_deliveries, api_keys, wallets, runs, charges, withdrawals, task_queries, agent_skills, agents, users RESTART IDENTITY CASCADE"

func setupWebhookTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置，跳过 webhook 集成测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))
	_, err = pool.Exec(ctx, truncateWebhookTables)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanCtx, truncateWebhookTables)
		pool.Close()
	})
	return pool
}

func insertWebhookUser(t *testing.T, pool *pgxpool.Pool, isCreator bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified)
		 VALUES ($1, $2, 'x', 'Webhook User', $3, $3)`,
		id, "webhook-"+id.String()[:8]+"@example.com", isCreator)
	require.NoError(t, err)
	return id
}

func insertWebhookAgent(t *testing.T, pool *pgxpool.Pool, creatorID uuid.UUID, slug string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agents (
			id, creator_id, slug, name, description, endpoint_url, price_per_call_cents,
			tags, lifecycle_status, visibility, certification_status
		) VALUES ($1, $2, $3, $4, 'Webhook test agent', $5, 100, '{ops}', 'active', 'public', 'unreviewed')`,
		id, creatorID, slug, "Webhook Agent "+slug, "https://example.com/agent/"+slug)
	require.NoError(t, err)
	return id
}

func insertWebhookRun(t *testing.T, pool *pgxpool.Pool, userID, agentID uuid.UUID) db.Run {
	t.Helper()
	runID := uuid.New()
	duration := int32(42)
	var run db.Run
	err := pool.QueryRow(context.Background(),
		`INSERT INTO runs (
			id, user_id, agent_id, input, output, status,
			cost_cents, platform_fee_cents, creator_revenue_cents,
			duration_ms, finished_at
		) VALUES (
			$1, $2, $3, '{"q":"hi"}', '{"text":"ok"}', 'success',
			100, 25, 75, $4, NOW()
		)
		RETURNING id, user_id, agent_id, input, output, status, error_code,
		          error_message, cost_cents, platform_fee_cents, creator_revenue_cents,
		          duration_ms, started_at, finished_at`,
		runID, userID, agentID, duration).
		Scan(
			&run.ID,
			&run.UserID,
			&run.AgentID,
			&run.Input,
			&run.Output,
			&run.Status,
			&run.ErrorCode,
			&run.ErrorMessage,
			&run.CostCents,
			&run.PlatformFeeCents,
			&run.CreatorRevenueCents,
			&run.DurationMs,
			&run.StartedAt,
			&run.FinishedAt,
		)
	require.NoError(t, err)
	return run
}

func TestSetRotateClearWebhook(t *testing.T) {
	pool := setupWebhookTestDB(t)
	svc := NewService(pool)
	creatorID := insertWebhookUser(t, pool, true)
	otherID := insertWebhookUser(t, pool, true)
	agentID := insertWebhookAgent(t, pool, creatorID, "webhook-config-"+uuid.NewString()[:8])

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	setResp, err := svc.SetWebhook(context.Background(), agentID, creatorID, server.URL)
	require.NoError(t, err)
	assert.Equal(t, server.URL, setResp.URL)
	assert.Len(t, setResp.Secret, 64)

	_, err = svc.RotateSecret(context.Background(), agentID, otherID)
	assert.Error(t, err)

	rotated, err := svc.RotateSecret(context.Background(), agentID, creatorID)
	require.NoError(t, err)
	assert.Equal(t, server.URL, rotated.URL)
	assert.Len(t, rotated.Secret, 64)
	assert.NotEqual(t, setResp.Secret, rotated.Secret)

	require.NoError(t, svc.ClearWebhook(context.Background(), agentID, creatorID))
	_, err = svc.RotateSecret(context.Background(), agentID, creatorID)
	assert.Error(t, err)
}

func TestEnqueueDeliveryPostsSignedPayload(t *testing.T) {
	pool := setupWebhookTestDB(t)
	svc := NewService(pool)
	userID := insertWebhookUser(t, pool, false)
	creatorID := insertWebhookUser(t, pool, true)
	agentID := insertWebhookAgent(t, pool, creatorID, "webhook-deliver-"+uuid.NewString()[:8])

	type receivedRequest struct {
		body      []byte
		signature string
		event     string
		delivery  string
	}
	gotCh := make(chan receivedRequest, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotCh <- receivedRequest{
			body:      body,
			signature: r.Header.Get("X-OpenLinker-Signature"),
			event:     r.Header.Get("X-OpenLinker-Event"),
			delivery:  r.Header.Get("X-OpenLinker-Delivery"),
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	svc.httpClient = server.Client()

	setResp, err := svc.SetWebhook(context.Background(), agentID, creatorID, server.URL)
	require.NoError(t, err)
	run := insertWebhookRun(t, pool, userID, agentID)

	require.NoError(t, svc.EnqueueDelivery(context.Background(), &run, "webhook-deliver", map[string]interface{}{"text": "ok"}))

	var got receivedRequest
	select {
	case got = <-gotCh:
	case <-time.After(2 * time.Second):
		t.Fatal("webhook endpoint was not called")
	}
	assert.Equal(t, eventRunCompleted, got.event)
	assert.NotEmpty(t, got.delivery)
	assert.Equal(t, "sha256="+signPayload(got.body, setResp.Secret), got.signature)

	var payload WebhookPayload
	require.NoError(t, json.Unmarshal(got.body, &payload))
	assert.Equal(t, run.ID.String(), payload.RunID)
	assert.Equal(t, "success", payload.Status)
	assert.Equal(t, int32(100), payload.CostCents)

	require.Eventually(t, func() bool {
		items, err := svc.ListDeliveries(context.Background(), agentID, creatorID, 20)
		return err == nil && len(items) == 1 && items[0].Status == "success" && items[0].AttemptCount == 1
	}, 2*time.Second, 20*time.Millisecond)
}

func TestRunWebhookSubscriptionDeliversSignedRunEvent(t *testing.T) {
	pool := setupWebhookTestDB(t)
	svc := NewService(pool)
	userID := insertWebhookUser(t, pool, false)
	creatorID := insertWebhookUser(t, pool, true)
	agentID := insertWebhookAgent(t, pool, creatorID, "run-webhook-"+uuid.NewString()[:8])
	run := insertWebhookRun(t, pool, userID, agentID)

	type receivedRequest struct {
		body      []byte
		signature string
		event     string
		delivery  string
	}
	gotCh := make(chan receivedRequest, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotCh <- receivedRequest{
			body:      body,
			signature: r.Header.Get("X-OpenLinker-Signature"),
			event:     r.Header.Get("X-OpenLinker-Event"),
			delivery:  r.Header.Get("X-OpenLinker-Delivery"),
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	svc.httpClient = server.Client()

	sub, err := svc.CreateRunWebhookSubscription(context.Background(), run.ID, userID, &CreateRunWebhookRequest{
		URL:        server.URL,
		EventTypes: []string{"run.completed"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, sub.Secret)

	event, err := db.New(pool).CreateRunEvent(context.Background(), db.CreateRunEventParams{
		RunID:     run.ID,
		EventType: "run.completed",
		Payload:   []byte(`{"status":"success"}`),
	})
	require.NoError(t, err)
	require.NoError(t, svc.EnqueueRunEvent(context.Background(), event))

	var got receivedRequest
	select {
	case got = <-gotCh:
	case <-time.After(2 * time.Second):
		t.Fatal("run webhook endpoint was not called")
	}
	assert.Equal(t, "run.completed", got.event)
	assert.NotEmpty(t, got.delivery)
	assert.Equal(t, "sha256="+signPayload(got.body, sub.Secret), got.signature)

	var payload RunWebhookPayload
	require.NoError(t, json.Unmarshal(got.body, &payload))
	assert.Equal(t, event.ID.String(), payload.EventID)
	assert.Equal(t, run.ID.String(), payload.RunID)
	assert.Equal(t, int32(1), payload.Sequence)
	assert.Equal(t, "success", payload.Payload["status"])

	require.Eventually(t, func() bool {
		var status string
		var attemptCount int32
		err := pool.QueryRow(context.Background(),
			`SELECT status, attempt_count FROM run_webhook_deliveries WHERE run_event_id=$1`,
			event.ID).Scan(&status, &attemptCount)
		return err == nil && status == "success" && attemptCount == 1
	}, 2*time.Second, 20*time.Millisecond)
}

func TestRunWebhookSubscriptionCanPauseAndResumeNonTerminalEvents(t *testing.T) {
	pool := setupWebhookTestDB(t)
	svc := NewService(pool)
	userID := insertWebhookUser(t, pool, false)
	creatorID := insertWebhookUser(t, pool, true)
	agentID := insertWebhookAgent(t, pool, creatorID, "run-webhook-pause-"+uuid.NewString()[:8])
	run := insertWebhookRun(t, pool, userID, agentID)

	gotCh := make(chan string, 2)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCh <- r.Header.Get("X-OpenLinker-Event")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	svc.httpClient = server.Client()

	sub, err := svc.CreateRunWebhookSubscription(context.Background(), run.ID, userID, &CreateRunWebhookRequest{
		URL:        server.URL,
		EventTypes: []string{"run.message.delta"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"run.message.delta"}, sub.EventTypes)

	paused, err := svc.UpdateRunWebhookSubscriptionStatus(context.Background(), run.ID, uuid.MustParse(sub.ID), userID, "paused")
	require.NoError(t, err)
	assert.Equal(t, "paused", paused.Status)

	messageEvent, err := db.New(pool).CreateRunEvent(context.Background(), db.CreateRunEventParams{
		RunID:     run.ID,
		EventType: "run.message.delta",
		Payload:   []byte(`{"text":"intermediate update"}`),
	})
	require.NoError(t, err)
	require.NoError(t, svc.EnqueueRunEvent(context.Background(), messageEvent))

	select {
	case event := <-gotCh:
		t.Fatalf("paused run webhook should not receive event, got %s", event)
	case <-time.After(150 * time.Millisecond):
	}

	resumed, err := svc.UpdateRunWebhookSubscriptionStatus(context.Background(), run.ID, uuid.MustParse(sub.ID), userID, "active")
	require.NoError(t, err)
	assert.Equal(t, "active", resumed.Status)

	messageEvent2, err := db.New(pool).CreateRunEvent(context.Background(), db.CreateRunEventParams{
		RunID:     run.ID,
		EventType: "run.message.delta",
		Payload:   []byte(`{"text":"after resume"}`),
	})
	require.NoError(t, err)
	require.NoError(t, svc.EnqueueRunEvent(context.Background(), messageEvent2))

	select {
	case event := <-gotCh:
		assert.Equal(t, "run.message.delta", event)
	case <-time.After(2 * time.Second):
		t.Fatal("resumed run webhook endpoint was not called")
	}
}

func TestRunWebhookBatchManagementAcrossRuns(t *testing.T) {
	pool := setupWebhookTestDB(t)
	svc := NewService(pool)
	userID := insertWebhookUser(t, pool, false)
	otherUserID := insertWebhookUser(t, pool, false)
	creatorID := insertWebhookUser(t, pool, true)
	agentID := insertWebhookAgent(t, pool, creatorID, "run-webhook-batch-"+uuid.NewString()[:8])
	runA := insertWebhookRun(t, pool, userID, agentID)
	runB := insertWebhookRun(t, pool, userID, agentID)
	otherRun := insertWebhookRun(t, pool, otherUserID, agentID)

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	subA, err := svc.CreateRunWebhookSubscription(context.Background(), runA.ID, userID, &CreateRunWebhookRequest{
		URL:        server.URL,
		EventTypes: []string{"run.completed"},
	})
	require.NoError(t, err)
	subB, err := svc.CreateRunWebhookSubscription(context.Background(), runB.ID, userID, &CreateRunWebhookRequest{
		URL:        server.URL,
		EventTypes: []string{"run.failed"},
	})
	require.NoError(t, err)
	otherSub, err := svc.CreateRunWebhookSubscription(context.Background(), otherRun.ID, otherUserID, &CreateRunWebhookRequest{
		URL:        server.URL,
		EventTypes: []string{"run.completed"},
	})
	require.NoError(t, err)

	active, err := svc.ListRunWebhookSubscriptionsForOwner(context.Background(), userID, "active", 20)
	require.NoError(t, err)
	require.Len(t, active, 2)

	paused, err := svc.BatchManageRunWebhookSubscriptions(context.Background(), userID, &BatchRunWebhookSubscriptionsRequest{
		Action:          "pause",
		SubscriptionIDs: []string{subA.ID, subB.ID, otherSub.ID},
	})
	require.NoError(t, err)
	assert.Equal(t, "pause", paused.Action)
	assert.Equal(t, 2, paused.UpdatedCount)
	assert.Len(t, paused.Items, 2)
	for _, item := range paused.Items {
		assert.Equal(t, "paused", item.Status)
	}

	pausedList, err := svc.ListRunWebhookSubscriptionsForOwner(context.Background(), userID, "paused", 20)
	require.NoError(t, err)
	require.Len(t, pausedList, 2)

	resumed, err := svc.BatchManageRunWebhookSubscriptions(context.Background(), userID, &BatchRunWebhookSubscriptionsRequest{
		Action:          "resume",
		SubscriptionIDs: []string{subA.ID},
	})
	require.NoError(t, err)
	require.Equal(t, 1, resumed.UpdatedCount)
	assert.Equal(t, "active", resumed.Items[0].Status)

	deleted, err := svc.BatchManageRunWebhookSubscriptions(context.Background(), userID, &BatchRunWebhookSubscriptionsRequest{
		Action:          "delete",
		SubscriptionIDs: []string{subB.ID},
	})
	require.NoError(t, err)
	require.Equal(t, 1, deleted.UpdatedCount)
	assert.Equal(t, "deleted", deleted.Items[0].Status)

	all, err := svc.ListRunWebhookSubscriptionsForOwner(context.Background(), userID, "", 20)
	require.NoError(t, err)
	require.Len(t, all, 1)
	assert.Equal(t, subA.ID, all[0].ID)
	assert.Equal(t, "active", all[0].Status)

	other, err := svc.ListRunWebhookSubscriptionsForOwner(context.Background(), otherUserID, "", 20)
	require.NoError(t, err)
	require.Len(t, other, 1)
	assert.Equal(t, otherSub.ID, other[0].ID)
	assert.Equal(t, "active", other[0].Status)
}

func TestAttemptDeliveryRetriesAndThenFailsFinal(t *testing.T) {
	pool := setupWebhookTestDB(t)
	svc := NewService(pool)
	userID := insertWebhookUser(t, pool, false)
	creatorID := insertWebhookUser(t, pool, true)
	agentID := insertWebhookAgent(t, pool, creatorID, "webhook-retry-"+uuid.NewString()[:8])

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer server.Close()
	svc.httpClient = server.Client()

	_, err := svc.SetWebhook(context.Background(), agentID, creatorID, server.URL)
	require.NoError(t, err)
	run := insertWebhookRun(t, pool, userID, agentID)
	delivery, err := db.New(pool).CreateWebhookDelivery(context.Background(), db.CreateWebhookDeliveryParams{
		AgentID: agentID,
		RunID:   run.ID,
		URL:     server.URL,
		Payload: []byte(`{"event":"run.completed"}`),
	})
	require.NoError(t, err)

	require.NoError(t, svc.AttemptDelivery(context.Background(), delivery.ID))
	var status string
	var attemptCount int32
	var nextRetryAt *time.Time
	err = pool.QueryRow(context.Background(),
		`SELECT status, attempt_count, next_retry_at FROM webhook_deliveries WHERE id=$1`,
		delivery.ID).Scan(&status, &attemptCount, &nextRetryAt)
	require.NoError(t, err)
	assert.Equal(t, "pending", status)
	assert.Equal(t, int32(1), attemptCount)
	assert.NotNil(t, nextRetryAt)

	_, err = pool.Exec(context.Background(),
		`UPDATE webhook_deliveries SET attempt_count=2, next_retry_at=NOW() WHERE id=$1`,
		delivery.ID)
	require.NoError(t, err)

	require.NoError(t, svc.AttemptDelivery(context.Background(), delivery.ID))
	var responseStatus *int32
	var errorMessage *string
	err = pool.QueryRow(context.Background(),
		`SELECT status, attempt_count, response_status, error_message, next_retry_at
		 FROM webhook_deliveries WHERE id=$1`,
		delivery.ID).Scan(&status, &attemptCount, &responseStatus, &errorMessage, &nextRetryAt)
	require.NoError(t, err)
	assert.Equal(t, "failed", status)
	assert.Equal(t, int32(3), attemptCount)
	require.NotNil(t, responseStatus)
	assert.Equal(t, int32(500), *responseStatus)
	require.NotNil(t, errorMessage)
	assert.Equal(t, "HTTP 500", *errorMessage)
	assert.Nil(t, nextRetryAt)
}
