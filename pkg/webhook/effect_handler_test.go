package webhook

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	runtimepkg "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestAgentWebhookEffectRetriesReuseOneStableDelivery(t *testing.T) {
	effectID := uuid.New()
	runID := uuid.New()
	agentID := uuid.New()
	var (
		mu          sync.Mutex
		deliveryIDs []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		deliveryIDs = append(deliveryIDs, r.Header.Get("X-OpenLinker-Delivery"))
		mu.Unlock()
		http.Error(w, "retry", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	q := &fakeWebhookEffectQueries{
		run: db.Run{
			ID: runID, AgentID: agentID, UserID: uuid.New(), Status: "failed",
			Input: []byte(`{"query":"hello"}`), StartedAt: time.Now(),
		},
		webhookConfig: db.GetAgentWebhookConfigRow{
			ID: agentID, Slug: "helper", WebhookURL: &server.URL,
			WebhookSecret: stringPointer("secret"),
		},
	}
	svc := &Service{effectQueries: q, httpClient: server.Client()}
	firstLease := uuid.New()
	effect := db.RunEffectOutbox{
		ID: effectID, RunID: runID, EffectType: runtimepkg.RunEffectTypeAgentWebhook,
		TargetKey: "agent:" + agentID.String(), Status: "processing",
		LeaseOwner: &firstLease, AttemptCount: 1, MaxAttempts: 3,
	}

	first := svc.AttemptAgentWebhookEffect(context.Background(), effect)
	require.False(t, first.Succeeded)
	require.True(t, first.Retryable)
	secondLease := uuid.New()
	effect.LeaseOwner = &secondLease
	effect.AttemptCount = 2
	second := svc.AttemptAgentWebhookEffect(context.Background(), effect)
	require.False(t, second.Succeeded)
	require.True(t, second.Retryable)

	require.Equal(t, 1, q.webhookCreateCount)
	require.NotNil(t, q.webhookDelivery.EffectOutboxID)
	require.Equal(t, effectID, q.webhookDelivery.ID)
	require.Equal(t, effectID, *q.webhookDelivery.EffectOutboxID)
	require.Equal(t, int32(2), q.webhookDelivery.AttemptCount)
	require.Equal(t, []string{effectID.String(), effectID.String()}, deliveryIDs)
}

func TestAgentWebhookEffectOldAttemptCannotOverwriteNewAttempt(t *testing.T) {
	effectID := uuid.New()
	runID := uuid.New()
	agentID := uuid.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	q := &fakeWebhookEffectQueries{
		webhookDelivery: db.WebhookDelivery{
			ID: effectID, RunID: runID, AgentID: agentID, URL: server.URL,
			Payload: []byte(`{"event":"run.completed"}`), Status: "pending",
			AttemptCount: 2, EffectOutboxID: &effectID,
		},
		webhookSecret: "secret",
	}
	svc := &Service{effectQueries: q, httpClient: server.Client()}
	oldLease := uuid.New()
	result := svc.AttemptAgentWebhookEffect(context.Background(), db.RunEffectOutbox{
		ID: effectID, RunID: runID, EffectType: runtimepkg.RunEffectTypeAgentWebhook,
		TargetKey: "agent:" + agentID.String(), Status: "processing",
		LeaseOwner: &oldLease, AttemptCount: 1, MaxAttempts: 3,
	})

	require.False(t, result.Succeeded)
	require.True(t, result.Retryable)
	require.Equal(t, "DELIVERY_ATTEMPT_SUPERSEDED", result.ErrorCode)
	require.Equal(t, "pending", q.webhookDelivery.Status)
	require.Equal(t, int32(2), q.webhookDelivery.AttemptCount)
}

func TestTaskCallbackEffectUsesEffectIDAsDeliveryID(t *testing.T) {
	effectID := uuid.New()
	runID := uuid.New()
	subscriptionID := uuid.New()
	var deliveredID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deliveredID = r.Header.Get("X-OpenLinker-Delivery")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	q := &fakeWebhookEffectQueries{
		subscription: db.TaskCallbackSubscription{
			ID: subscriptionID, RunID: runID, TargetURL: server.URL, Secret: "secret",
			Status: "active", EventTypes: []string{"run.completed"},
		},
		event: db.RunEvent{
			ID: effectID, RunID: runID, Sequence: 3, EventType: "run.completed",
			Payload: []byte(`{"status":"success","terminal":true}`), CreatedAt: time.Now(),
		},
	}
	svc := &Service{effectQueries: q, httpClient: server.Client()}
	leaseOwner := uuid.New()
	result := svc.AttemptTaskCallbackEffect(context.Background(), db.RunEffectOutbox{
		ID: effectID, RunID: runID, TerminalEventID: effectID,
		EffectType: runtimepkg.RunEffectTypeTaskCallback,
		TargetKey:  "subscription:" + subscriptionID.String(),
		Status:     "processing", LeaseOwner: &leaseOwner, AttemptCount: 1, MaxAttempts: 3,
	})

	require.True(t, result.Succeeded)
	require.Equal(t, effectID.String(), deliveredID)
	require.Equal(t, 1, q.taskCreateCount)
	require.Equal(t, effectID, q.taskDelivery.ID)
	require.NotNil(t, q.taskDelivery.EffectOutboxID)
	require.Equal(t, effectID, *q.taskDelivery.EffectOutboxID)
	require.Equal(t, "success", q.taskDelivery.Status)
}

// 真实运行里任务回调走这条路径，投递的和落库的都要是过滤后的内容。
func TestTaskCallbackEffectDeliversPayloadFilteredForOwner(t *testing.T) {
	const token = "seller-endpoint-token"
	endpointToken := token
	creatorID := uuid.New()
	deliver := func(owner uuid.UUID) (string, string) {
		t.Helper()
		effectID, runID, subscriptionID := uuid.New(), uuid.New(), uuid.New()
		var body string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, _ := io.ReadAll(r.Body)
			body = string(raw)
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()
		q := &fakeWebhookEffectQueries{
			run:   db.Run{ID: runID, AgentID: uuid.New()},
			agent: &db.Agent{CreatorID: creatorID, EndpointAuthHeader: &endpointToken},
			subscription: db.TaskCallbackSubscription{
				ID: subscriptionID, RunID: runID, OwnerUserID: owner, TargetURL: server.URL, Secret: "secret",
				Status: "active", EventTypes: []string{"run.completed"},
			},
			event: db.RunEvent{
				ID: effectID, RunID: runID, Sequence: 3, EventType: "run.completed", CreatedAt: time.Now(),
				Payload: []byte(`{"output":{"origin":"203.0.113.7","marker":"kept","note":"` + token + `"}}`),
			},
		}
		svc := &Service{effectQueries: q, httpClient: server.Client()}
		leaseOwner := uuid.New()
		result := svc.AttemptTaskCallbackEffect(context.Background(), db.RunEffectOutbox{
			ID: effectID, RunID: runID, TerminalEventID: effectID,
			EffectType: runtimepkg.RunEffectTypeTaskCallback,
			TargetKey:  "subscription:" + subscriptionID.String(),
			Status:     "processing", LeaseOwner: &leaseOwner, AttemptCount: 1, MaxAttempts: 3,
		})
		require.True(t, result.Succeeded)
		return body, string(q.taskDelivery.Payload)
	}

	delivered, stored := deliver(uuid.New())
	for _, payload := range []string{delivered, stored} {
		require.NotContains(t, payload, token)
		require.NotContains(t, payload, "203.0.113.7")
		require.Contains(t, payload, "kept")
		require.Contains(t, payload, `"output_redacted":true`)
	}

	delivered, _ = deliver(creatorID)
	require.Contains(t, delivered, token, "创作者自己订阅时保留原文")
}

func TestResetWebhookEffectDeliveryReconcilesCrashWindow(t *testing.T) {
	effectID := uuid.New()
	runID := uuid.New()
	zero := int64(0)
	q := &fakeWebhookEffectQueries{
		webhookDelivery: db.WebhookDelivery{
			ID: effectID, RunID: runID, Status: "pending", EffectOutboxID: &effectID,
		},
		webhookResetRows: &zero,
	}
	svc := &Service{effectQueries: q}
	err := svc.ResetWebhookEffectDelivery(context.Background(), db.RunEffectOutbox{
		ID: effectID, RunID: runID, EffectType: runtimepkg.RunEffectTypeAgentWebhook,
	})
	require.NoError(t, err)
}

func TestResetWebhookEffectDeliveryRejectsFailedZeroRowReset(t *testing.T) {
	effectID := uuid.New()
	runID := uuid.New()
	zero := int64(0)
	q := &fakeWebhookEffectQueries{
		webhookDelivery: db.WebhookDelivery{
			ID: effectID, RunID: runID, Status: "failed", EffectOutboxID: &effectID,
		},
		webhookResetRows: &zero,
	}
	svc := &Service{effectQueries: q}
	err := svc.ResetWebhookEffectDelivery(context.Background(), db.RunEffectOutbox{
		ID: effectID, RunID: runID, EffectType: runtimepkg.RunEffectTypeAgentWebhook,
	})
	require.ErrorContains(t, err, "remains failed")
}

func TestResetTaskCallbackEffectDeliveryRejectsIdentityConflict(t *testing.T) {
	effectID := uuid.New()
	zero := int64(0)
	q := &fakeWebhookEffectQueries{
		taskDelivery: db.TaskCallbackDelivery{
			ID: effectID, RunEventID: uuid.New(), Status: "pending", EffectOutboxID: &effectID,
		},
		taskResetRows: &zero,
	}
	svc := &Service{effectQueries: q}
	err := svc.ResetWebhookEffectDelivery(context.Background(), db.RunEffectOutbox{
		ID: effectID, RunID: uuid.New(), TerminalEventID: uuid.New(),
		EffectType: runtimepkg.RunEffectTypeTaskCallback,
	})
	require.ErrorContains(t, err, "identity conflicts")
}

type fakeWebhookEffectQueries struct {
	run                db.Run
	webhookConfig      db.GetAgentWebhookConfigRow
	webhookDelivery    db.WebhookDelivery
	webhookSecret      string
	webhookCreateCount int

	agent           *db.Agent
	subscription    db.TaskCallbackSubscription
	event           db.RunEvent
	taskDelivery    db.TaskCallbackDelivery
	taskCreateCount int

	webhookResetRows *int64
	taskResetRows    *int64
}

func (q *fakeWebhookEffectQueries) GetRunByID(context.Context, uuid.UUID) (db.Run, error) {
	if q.run.ID == uuid.Nil {
		return db.Run{}, pgx.ErrNoRows
	}
	return q.run, nil
}

func (q *fakeWebhookEffectQueries) GetAgentByID(context.Context, uuid.UUID) (db.Agent, error) {
	if q.agent == nil {
		return db.Agent{}, pgx.ErrNoRows
	}
	return *q.agent, nil
}

func (q *fakeWebhookEffectQueries) GetAgentWebhookConfig(context.Context, uuid.UUID) (db.GetAgentWebhookConfigRow, error) {
	if q.webhookConfig.ID == uuid.Nil {
		return db.GetAgentWebhookConfigRow{}, pgx.ErrNoRows
	}
	return q.webhookConfig, nil
}

func (q *fakeWebhookEffectQueries) CreateWebhookEffectDelivery(_ context.Context, arg db.CreateWebhookEffectDeliveryParams) (db.WebhookDelivery, error) {
	if q.webhookDelivery.ID == uuid.Nil {
		q.webhookCreateCount++
		q.webhookDelivery = db.WebhookDelivery{
			ID: arg.ID, AgentID: arg.AgentID, RunID: arg.RunID, URL: arg.URL,
			Payload: arg.Payload, Status: "pending", EffectOutboxID: &arg.ID,
		}
		q.webhookSecret = pointerString(q.webhookConfig.WebhookSecret)
	}
	return q.webhookDelivery, nil
}

func (q *fakeWebhookEffectQueries) GetWebhookDeliveryByID(context.Context, uuid.UUID) (db.GetWebhookDeliveryRow, error) {
	if q.webhookDelivery.ID == uuid.Nil {
		return db.GetWebhookDeliveryRow{}, pgx.ErrNoRows
	}
	secret := q.webhookSecret
	return db.GetWebhookDeliveryRow{
		WebhookDelivery: q.webhookDelivery,
		WebhookSecret:   &secret,
	}, nil
}

func (q *fakeWebhookEffectQueries) MarkWebhookEffectDeliverySuccess(_ context.Context, arg db.MarkWebhookEffectDeliverySuccessParams) (int64, error) {
	if q.webhookDelivery.Status != "pending" || q.webhookDelivery.AttemptCount >= arg.AttemptCount {
		return 0, nil
	}
	q.webhookDelivery.Status = "success"
	q.webhookDelivery.AttemptCount = arg.AttemptCount
	return 1, nil
}

func (q *fakeWebhookEffectQueries) MarkWebhookEffectDeliveryRetry(_ context.Context, arg db.MarkWebhookEffectDeliveryRetryParams) (int64, error) {
	if q.webhookDelivery.Status != "pending" || q.webhookDelivery.AttemptCount >= arg.AttemptCount {
		return 0, nil
	}
	q.webhookDelivery.AttemptCount = arg.AttemptCount
	return 1, nil
}

func (q *fakeWebhookEffectQueries) MarkWebhookEffectDeliveryFailed(_ context.Context, arg db.MarkWebhookEffectDeliveryFailedParams) (int64, error) {
	if q.webhookDelivery.Status != "pending" || q.webhookDelivery.AttemptCount >= arg.AttemptCount {
		return 0, nil
	}
	q.webhookDelivery.Status = "failed"
	q.webhookDelivery.AttemptCount = arg.AttemptCount
	return 1, nil
}

func (q *fakeWebhookEffectQueries) ResetWebhookEffectDelivery(context.Context, uuid.UUID) (int64, error) {
	if q.webhookResetRows != nil {
		return *q.webhookResetRows, nil
	}
	q.webhookDelivery.Status = "pending"
	q.webhookDelivery.AttemptCount = 0
	return 1, nil
}

func (q *fakeWebhookEffectQueries) GetTaskCallbackSubscriptionByID(context.Context, uuid.UUID) (db.TaskCallbackSubscription, error) {
	if q.subscription.ID == uuid.Nil {
		return db.TaskCallbackSubscription{}, pgx.ErrNoRows
	}
	return q.subscription, nil
}

func (q *fakeWebhookEffectQueries) GetTaskCallbackEffectEvent(context.Context, db.GetTaskCallbackEffectEventParams) (db.RunEvent, error) {
	if q.event.ID == uuid.Nil {
		return db.RunEvent{}, pgx.ErrNoRows
	}
	return q.event, nil
}

func (q *fakeWebhookEffectQueries) CreateTaskCallbackEffectDelivery(_ context.Context, arg db.CreateTaskCallbackEffectDeliveryParams) (db.TaskCallbackDelivery, error) {
	if q.taskDelivery.ID == uuid.Nil {
		q.taskCreateCount++
		q.taskDelivery = db.TaskCallbackDelivery{
			ID: arg.ID, SubscriptionID: arg.SubscriptionID, RunEventID: arg.RunEventID,
			Payload: arg.Payload, Status: "pending", EffectOutboxID: &arg.ID,
		}
	}
	return q.taskDelivery, nil
}

func (q *fakeWebhookEffectQueries) GetTaskCallbackDeliveryByID(context.Context, uuid.UUID) (db.GetTaskCallbackDeliveryByIDRow, error) {
	if q.taskDelivery.ID == uuid.Nil {
		return db.GetTaskCallbackDeliveryByIDRow{}, pgx.ErrNoRows
	}
	return db.GetTaskCallbackDeliveryByIDRow{
		TaskCallbackDelivery: q.taskDelivery,
		TargetURL:            q.subscription.TargetURL,
		Secret:               q.subscription.Secret,
		AuthScheme:           q.subscription.AuthScheme,
		AuthCredentials:      q.subscription.AuthCredentials,
		EventType:            q.event.EventType,
	}, nil
}

func (q *fakeWebhookEffectQueries) MarkTaskCallbackEffectDeliverySuccess(_ context.Context, arg db.MarkTaskCallbackEffectDeliverySuccessParams) (int64, error) {
	if q.taskDelivery.Status != "pending" || q.taskDelivery.AttemptCount >= arg.AttemptCount {
		return 0, nil
	}
	q.taskDelivery.Status = "success"
	q.taskDelivery.AttemptCount = arg.AttemptCount
	return 1, nil
}

func (q *fakeWebhookEffectQueries) MarkTaskCallbackEffectDeliveryRetry(_ context.Context, arg db.MarkTaskCallbackEffectDeliveryRetryParams) (int64, error) {
	if q.taskDelivery.Status != "pending" || q.taskDelivery.AttemptCount >= arg.AttemptCount {
		return 0, nil
	}
	q.taskDelivery.AttemptCount = arg.AttemptCount
	return 1, nil
}

func (q *fakeWebhookEffectQueries) MarkTaskCallbackEffectDeliveryFailed(_ context.Context, arg db.MarkTaskCallbackEffectDeliveryFailedParams) (int64, error) {
	if q.taskDelivery.Status != "pending" || q.taskDelivery.AttemptCount >= arg.AttemptCount {
		return 0, nil
	}
	q.taskDelivery.Status = "failed"
	q.taskDelivery.AttemptCount = arg.AttemptCount
	return 1, nil
}

func (q *fakeWebhookEffectQueries) ResetTaskCallbackEffectDelivery(context.Context, uuid.UUID) (int64, error) {
	if q.taskResetRows != nil {
		return *q.taskResetRows, nil
	}
	q.taskDelivery.Status = "pending"
	q.taskDelivery.AttemptCount = 0
	return 1, nil
}

func stringPointer(value string) *string { return &value }

func pointerString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
