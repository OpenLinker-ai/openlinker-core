package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestWebhookServiceAgentConfigAndHistory(t *testing.T) {
	creatorID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	url := "https://client.example/hook"
	secret := "old-secret"
	q := &fakeWebhookQueries{
		agentCfg: db.GetAgentWebhookConfigRow{
			ID:            agentID,
			CreatorID:     creatorID,
			Slug:          "writer",
			WebhookURL:    &url,
			WebhookSecret: &secret,
		},
		setRows:   1,
		clearRows: 1,
		deliveries: []db.WebhookDelivery{
			{
				ID:           uuid.New(),
				AgentID:      agentID,
				RunID:        runID,
				URL:          url,
				Status:       "success",
				AttemptCount: 1,
				CreatedAt:    now,
				UpdatedAt:    now,
			},
		},
	}
	svc := &Service{queries: q}

	setResp, err := svc.SetWebhook(context.Background(), agentID, creatorID, url)
	if err != nil {
		t.Fatalf("SetWebhook error = %v", err)
	}
	if setResp.URL != url || setResp.Secret == "" {
		t.Fatalf("SetWebhook response = %#v", setResp)
	}
	if q.setWebhookArg.ID != agentID || q.setWebhookArg.CreatorID != creatorID || q.setWebhookArg.WebhookURL == nil || *q.setWebhookArg.WebhookURL != url {
		t.Fatalf("SetWebhook arg = %#v", q.setWebhookArg)
	}

	rotated, err := svc.RotateSecret(context.Background(), agentID, creatorID)
	if err != nil {
		t.Fatalf("RotateSecret error = %v", err)
	}
	if rotated.URL != url || rotated.Secret == "" || rotated.Secret == secret {
		t.Fatalf("RotateSecret response = %#v", rotated)
	}

	items, err := svc.ListDeliveries(context.Background(), agentID, creatorID, maxListLimit+1)
	if err != nil {
		t.Fatalf("ListDeliveries error = %v", err)
	}
	if q.listDeliveriesArg.AgentID != agentID || q.listDeliveriesArg.Limit != defaultListLimit {
		t.Fatalf("ListDeliveries arg = %#v", q.listDeliveriesArg)
	}
	if len(items) != 1 || items[0].RunID != runID.String() || items[0].Status != "success" {
		t.Fatalf("delivery items = %#v", items)
	}

	if err := svc.ClearWebhook(context.Background(), agentID, creatorID); err != nil {
		t.Fatalf("ClearWebhook error = %v", err)
	}
	if q.clearWebhookArg.ID != agentID || q.clearWebhookArg.CreatorID != creatorID {
		t.Fatalf("ClearWebhook arg = %#v", q.clearWebhookArg)
	}
}

func TestWebhookServiceAttemptDeliveryStateMachine(t *testing.T) {
	deliveryID := uuid.New()
	subscriptionID := uuid.New()
	payload := []byte(`{"event":"run.completed"}`)
	secret := "webhook-secret"

	successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-OpenLinker-Signature") != "sha256="+signPayload(payload, secret) {
			t.Fatalf("signature = %q", r.Header.Get("X-OpenLinker-Signature"))
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("accepted"))
	}))
	defer successServer.Close()

	q := &fakeWebhookQueries{
		deliveryRow: db.GetWebhookDeliveryRow{
			WebhookDelivery: db.WebhookDelivery{
				ID:      deliveryID,
				URL:     successServer.URL,
				Payload: payload,
				Status:  "pending",
			},
			WebhookSecret: &secret,
		},
	}
	svc := &Service{queries: q, httpClient: successServer.Client()}
	if err := svc.AttemptDelivery(context.Background(), deliveryID); err != nil {
		t.Fatalf("AttemptDelivery success error = %v", err)
	}
	if q.successArg.ID != deliveryID || q.successArg.ResponseStatus == nil || *q.successArg.ResponseStatus != http.StatusAccepted {
		t.Fatalf("success arg = %#v", q.successArg)
	}
	if q.successArg.ResponseBody == nil || *q.successArg.ResponseBody != "accepted" {
		t.Fatalf("success body = %#v", q.successArg.ResponseBody)
	}

	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("temporary"))
	}))
	defer failServer.Close()
	retryQ := &fakeWebhookQueries{
		deliveryRow: db.GetWebhookDeliveryRow{
			WebhookDelivery: db.WebhookDelivery{
				ID:           deliveryID,
				URL:          failServer.URL,
				Payload:      payload,
				Status:       "pending",
				AttemptCount: 0,
			},
			WebhookSecret: &secret,
		},
	}
	svc = &Service{queries: retryQ, httpClient: failServer.Client()}
	if err := svc.AttemptDelivery(context.Background(), deliveryID); err != nil {
		t.Fatalf("AttemptDelivery retry error = %v", err)
	}
	if retryQ.retryArg.ID != deliveryID || retryQ.retryArg.ResponseStatus == nil || *retryQ.retryArg.ResponseStatus != http.StatusInternalServerError {
		t.Fatalf("retry arg = %#v", retryQ.retryArg)
	}
	if retryQ.retryArg.ErrorMessage == nil || *retryQ.retryArg.ErrorMessage != "HTTP 500" || retryQ.retryArg.NextRetryAt.IsZero() {
		t.Fatalf("retry arg = %#v", retryQ.retryArg)
	}

	deletedSecretQ := &fakeWebhookQueries{
		deliveryRow: db.GetWebhookDeliveryRow{
			WebhookDelivery: db.WebhookDelivery{ID: deliveryID, Status: "pending"},
		},
	}
	svc = &Service{queries: deletedSecretQ, httpClient: http.DefaultClient}
	if err := svc.AttemptDelivery(context.Background(), deliveryID); err != nil {
		t.Fatalf("AttemptDelivery deleted secret error = %v", err)
	}
	if deletedSecretQ.finalArg.ErrorMessage == nil || *deletedSecretQ.finalArg.ErrorMessage != "webhook secret 已被清除" {
		t.Fatalf("deleted secret final arg = %#v", deletedSecretQ.finalArg)
	}

	taskCallbackQ := &fakeWebhookQueries{
		runDeliveryRow: db.GetTaskCallbackDeliveryByIDRow{
			TaskCallbackDelivery: db.TaskCallbackDelivery{
				ID:             deliveryID,
				SubscriptionID: subscriptionID,
				Payload:        payload,
				Status:         "pending",
			},
			TargetURL:       successServer.URL,
			Secret:          secret,
			EventType:       "run.completed",
			AuthScheme:      stringPtr("Bearer"),
			AuthCredentials: stringPtr("token"),
		},
	}
	svc = &Service{queries: taskCallbackQ, httpClient: successServer.Client()}
	if err := svc.AttemptTaskCallbackDelivery(context.Background(), deliveryID); err != nil {
		t.Fatalf("AttemptTaskCallbackDelivery success error = %v", err)
	}
	if taskCallbackQ.runSuccessArg.ID != deliveryID || taskCallbackQ.resetSubscriptionID != subscriptionID {
		t.Fatalf("task callback success/reset = %#v/%s", taskCallbackQ.runSuccessArg, taskCallbackQ.resetSubscriptionID)
	}

	runFinalQ := &fakeWebhookQueries{
		runDeliveryRow: db.GetTaskCallbackDeliveryByIDRow{
			TaskCallbackDelivery: db.TaskCallbackDelivery{
				ID:             deliveryID,
				SubscriptionID: subscriptionID,
				Payload:        payload,
				Status:         "pending",
				AttemptCount:   2,
			},
			TargetURL: failServer.URL,
			Secret:    secret,
			EventType: "run.completed",
		},
	}
	svc = &Service{queries: runFinalQ, httpClient: failServer.Client()}
	if err := svc.AttemptTaskCallbackDelivery(context.Background(), deliveryID); err != nil {
		t.Fatalf("AttemptTaskCallbackDelivery final error = %v", err)
	}
	if runFinalQ.runFinalArg.ID != deliveryID || runFinalQ.incrementSubscriptionID != subscriptionID {
		t.Fatalf("task callback final/increment = %#v/%s", runFinalQ.runFinalArg, runFinalQ.incrementSubscriptionID)
	}
}

func TestWebhookServiceProcessPendingDeliversTaskCallbackQueue(t *testing.T) {
	runDeliveryID := uuid.New()
	subscriptionID := uuid.New()
	payload := []byte(`{"event":"run.completed"}`)
	secret := "webhook-secret"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-OpenLinker-Signature") != "sha256="+signPayload(payload, secret) {
			t.Fatalf("signature = %q", r.Header.Get("X-OpenLinker-Signature"))
		}
		if r.Header.Get("X-OpenLinker-Delivery") == "" {
			t.Fatal("delivery header was empty")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	q := &fakeWebhookQueries{
		pendingRunDeliveries: []db.TaskCallbackDelivery{{ID: runDeliveryID}},
		runDeliveryRow: db.GetTaskCallbackDeliveryByIDRow{
			TaskCallbackDelivery: db.TaskCallbackDelivery{
				ID:             runDeliveryID,
				SubscriptionID: subscriptionID,
				Payload:        payload,
				Status:         "pending",
			},
			TargetURL: server.URL,
			Secret:    secret,
			EventType: "run.completed",
		},
	}
	svc := &Service{queries: q, httpClient: server.Client()}

	svc.processPending(context.Background())

	if q.runSuccessArg.ID != runDeliveryID || q.runSuccessArg.ResponseStatus == nil || *q.runSuccessArg.ResponseStatus != http.StatusNoContent {
		t.Fatalf("run delivery success arg = %#v", q.runSuccessArg)
	}
	if q.resetSubscriptionID != subscriptionID {
		t.Fatalf("reset subscription id = %s, want %s", q.resetSubscriptionID, subscriptionID)
	}
}

func TestWebhookServiceQueueAndDeliveryErrorEdges(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	deliveryID := uuid.New()
	subscriptionID := uuid.New()
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	url := "https://client.example/hook"
	secret := "webhook-secret"
	sentinel := errors.New("database stopped")
	run := &db.Run{
		ID:         runID,
		UserID:     userID,
		AgentID:    agentID,
		Input:      []byte(`{"prompt":"hi"}`),
		Status:     "success",
		CostCents:  42,
		StartedAt:  now,
		FinishedAt: &now,
	}

	if err := (&Service{queries: &fakeWebhookQueries{agentCfgErr: pgx.ErrNoRows}}).
		EnqueueDelivery(context.Background(), run, "agent-one", map[string]interface{}{"ok": true}); err != nil {
		t.Fatalf("EnqueueDelivery missing agent = %v", err)
	}
	if err := (&Service{queries: &fakeWebhookQueries{agentCfgErr: sentinel}}).
		EnqueueDelivery(context.Background(), run, "agent-one", nil); err == nil {
		t.Fatalf("EnqueueDelivery config error should propagate")
	}
	if err := (&Service{queries: &fakeWebhookQueries{agentCfg: db.GetAgentWebhookConfigRow{ID: agentID, CreatorID: userID}}}).
		EnqueueDelivery(context.Background(), run, "agent-one", nil); err != nil {
		t.Fatalf("EnqueueDelivery without url = %v", err)
	}

	createErrQ := &fakeWebhookQueries{
		agentCfg:          db.GetAgentWebhookConfigRow{ID: agentID, CreatorID: userID, WebhookURL: &url, WebhookSecret: &secret},
		createDeliveryErr: sentinel,
	}
	if err := (&Service{queries: createErrQ}).
		EnqueueDelivery(context.Background(), run, "agent-one", map[string]interface{}{"ok": true}); err == nil {
		t.Fatalf("EnqueueDelivery create error should propagate")
	}
	if createErrQ.createDeliveryArg.AgentID != agentID || createErrQ.createDeliveryArg.URL != url {
		t.Fatalf("create delivery arg = %#v", createErrQ.createDeliveryArg)
	}

	createQ := &fakeWebhookQueries{
		agentCfg: db.GetAgentWebhookConfigRow{ID: agentID, CreatorID: userID, WebhookURL: &url, WebhookSecret: &secret},
		createDelivery: db.WebhookDelivery{
			ID:        deliveryID,
			AgentID:   agentID,
			RunID:     runID,
			URL:       url,
			Status:    "pending",
			CreatedAt: now,
			UpdatedAt: now,
		},
	}
	if err := (&Service{queries: createQ, httpClient: webhookHTTPClient(http.StatusNoContent, "", nil)}).
		EnqueueDelivery(context.Background(), run, "agent-one", map[string]interface{}{"ok": true}); err != nil {
		t.Fatalf("EnqueueDelivery success = %v", err)
	}
	var payload WebhookPayload
	if err := json.Unmarshal(createQ.createDeliveryArg.Payload, &payload); err != nil || payload.AgentSlug != "agent-one" || payload.RunID != runID.String() {
		t.Fatalf("delivery payload = %#v %v", payload, err)
	}

	if err := (&Service{queries: &fakeWebhookQueries{deliveryErr: pgx.ErrNoRows}}).
		AttemptDelivery(context.Background(), deliveryID); err != nil {
		t.Fatalf("AttemptDelivery missing = %v", err)
	}
	if err := (&Service{queries: &fakeWebhookQueries{deliveryErr: sentinel}}).
		AttemptDelivery(context.Background(), deliveryID); err == nil {
		t.Fatalf("AttemptDelivery query error should propagate")
	}
	if err := (&Service{queries: &fakeWebhookQueries{deliveryRow: db.GetWebhookDeliveryRow{WebhookDelivery: db.WebhookDelivery{ID: deliveryID, Status: "success"}}}}).
		AttemptDelivery(context.Background(), deliveryID); err != nil {
		t.Fatalf("AttemptDelivery terminal = %v", err)
	}

	successErrQ := &fakeWebhookQueries{
		deliveryRow: db.GetWebhookDeliveryRow{
			WebhookDelivery: db.WebhookDelivery{ID: deliveryID, URL: url, Payload: []byte(`{}`), Status: "pending"},
			WebhookSecret:   &secret,
		},
		successErr: sentinel,
	}
	if err := (&Service{queries: successErrQ, httpClient: webhookHTTPClient(http.StatusAccepted, "ok", nil)}).
		AttemptDelivery(context.Background(), deliveryID); err == nil {
		t.Fatalf("AttemptDelivery success mark error should propagate")
	}

	retryErrQ := &fakeWebhookQueries{
		deliveryRow: db.GetWebhookDeliveryRow{
			WebhookDelivery: db.WebhookDelivery{ID: deliveryID, URL: url, Payload: []byte(`{}`), Status: "pending"},
			WebhookSecret:   &secret,
		},
		retryErr: sentinel,
	}
	if err := (&Service{queries: retryErrQ, httpClient: webhookHTTPClient(http.StatusInternalServerError, "temporary", nil)}).
		AttemptDelivery(context.Background(), deliveryID); err == nil {
		t.Fatalf("AttemptDelivery retry mark error should propagate")
	}

	finalErrQ := &fakeWebhookQueries{
		deliveryRow: db.GetWebhookDeliveryRow{
			WebhookDelivery: db.WebhookDelivery{ID: deliveryID, URL: url, Payload: []byte(`{}`), Status: "pending", AttemptCount: 2},
			WebhookSecret:   &secret,
		},
		finalErr: sentinel,
	}
	if err := (&Service{queries: finalErrQ, httpClient: webhookHTTPClient(0, "", errors.New("dial failed"))}).
		AttemptDelivery(context.Background(), deliveryID); err == nil {
		t.Fatalf("AttemptDelivery final mark error should propagate")
	}

	if err := (&Service{queries: &fakeWebhookQueries{runDeliveryErr: pgx.ErrNoRows}}).
		AttemptTaskCallbackDelivery(context.Background(), deliveryID); err != nil {
		t.Fatalf("AttemptTaskCallbackDelivery missing = %v", err)
	}
	if err := (&Service{queries: &fakeWebhookQueries{runDeliveryErr: sentinel}}).
		AttemptTaskCallbackDelivery(context.Background(), deliveryID); err == nil {
		t.Fatalf("AttemptTaskCallbackDelivery query error should propagate")
	}
	if err := (&Service{queries: &fakeWebhookQueries{runDeliveryRow: db.GetTaskCallbackDeliveryByIDRow{TaskCallbackDelivery: db.TaskCallbackDelivery{ID: deliveryID, Status: "failed"}}}}).
		AttemptTaskCallbackDelivery(context.Background(), deliveryID); err != nil {
		t.Fatalf("AttemptTaskCallbackDelivery terminal = %v", err)
	}

	runRetryQ := &fakeWebhookQueries{
		runDeliveryRow: db.GetTaskCallbackDeliveryByIDRow{
			TaskCallbackDelivery: db.TaskCallbackDelivery{ID: deliveryID, SubscriptionID: subscriptionID, Payload: []byte(`{}`), Status: "pending"},
			TargetURL:            url,
			Secret:               secret,
			EventType:            "run.completed",
		},
	}
	if err := (&Service{queries: runRetryQ, httpClient: webhookHTTPClient(http.StatusTooManyRequests, "slow down", nil)}).
		AttemptTaskCallbackDelivery(context.Background(), deliveryID); err != nil {
		t.Fatalf("AttemptTaskCallbackDelivery retry = %v", err)
	}
	if runRetryQ.runRetryArg.ID != deliveryID || runRetryQ.runRetryArg.NextRetryAt.IsZero() {
		t.Fatalf("run retry arg = %#v", runRetryQ.runRetryArg)
	}

	runSuccessErrQ := &fakeWebhookQueries{
		runDeliveryRow: db.GetTaskCallbackDeliveryByIDRow{
			TaskCallbackDelivery: db.TaskCallbackDelivery{ID: deliveryID, SubscriptionID: subscriptionID, Payload: []byte(`{}`), Status: "pending"},
			TargetURL:            url,
			Secret:               secret,
			EventType:            "run.completed",
		},
		runSuccessErr: sentinel,
	}
	if err := (&Service{queries: runSuccessErrQ, httpClient: webhookHTTPClient(http.StatusOK, "ok", nil)}).
		AttemptTaskCallbackDelivery(context.Background(), deliveryID); err == nil {
		t.Fatalf("AttemptTaskCallbackDelivery success mark error should propagate")
	}

	resetErrQ := &fakeWebhookQueries{
		runDeliveryRow: db.GetTaskCallbackDeliveryByIDRow{
			TaskCallbackDelivery: db.TaskCallbackDelivery{ID: deliveryID, SubscriptionID: subscriptionID, Payload: []byte(`{}`), Status: "pending"},
			TargetURL:            url,
			Secret:               secret,
			EventType:            "run.completed",
		},
		resetErr: sentinel,
	}
	if err := (&Service{queries: resetErrQ, httpClient: webhookHTTPClient(http.StatusOK, "ok", nil)}).
		AttemptTaskCallbackDelivery(context.Background(), deliveryID); err == nil {
		t.Fatalf("AttemptTaskCallbackDelivery reset error should propagate")
	}

	runFinalErrQ := &fakeWebhookQueries{
		runDeliveryRow: db.GetTaskCallbackDeliveryByIDRow{
			TaskCallbackDelivery: db.TaskCallbackDelivery{ID: deliveryID, SubscriptionID: subscriptionID, Payload: []byte(`{}`), Status: "pending", AttemptCount: 2},
			TargetURL:            url,
			Secret:               secret,
			EventType:            "run.completed",
		},
		runFinalErr: sentinel,
	}
	if err := (&Service{queries: runFinalErrQ, httpClient: webhookHTTPClient(http.StatusBadGateway, "bad", nil)}).
		AttemptTaskCallbackDelivery(context.Background(), deliveryID); err == nil {
		t.Fatalf("AttemptTaskCallbackDelivery final mark error should propagate")
	}

	incrementErrQ := &fakeWebhookQueries{
		runDeliveryRow: db.GetTaskCallbackDeliveryByIDRow{
			TaskCallbackDelivery: db.TaskCallbackDelivery{ID: deliveryID, SubscriptionID: subscriptionID, Payload: []byte(`{}`), Status: "pending", AttemptCount: 2},
			TargetURL:            url,
			Secret:               secret,
			EventType:            "run.completed",
		},
		incrementErr: sentinel,
	}
	if err := (&Service{queries: incrementErrQ, httpClient: webhookHTTPClient(http.StatusBadGateway, "bad", nil)}).
		AttemptTaskCallbackDelivery(context.Background(), deliveryID); err == nil {
		t.Fatalf("AttemptTaskCallbackDelivery increment error should propagate")
	}
}

func TestTaskCallbackSubscriptionManagement(t *testing.T) {
	userID := uuid.New()
	runID := uuid.New()
	subID := uuid.New()
	eventID := uuid.New()
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	sub := db.TaskCallbackSubscription{
		ID:          subID,
		RunID:       runID,
		OwnerUserID: userID,
		TargetURL:   "https://client.example/push",
		EventTypes:  []string{"run.completed"},
		Secret:      "secret",
		Status:      "active",
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	q := &fakeWebhookQueries{
		run:            db.Run{ID: runID, UserID: userID},
		createSub:      sub,
		latestEventErr: pgx.ErrNoRows,
		listSubs:       []db.TaskCallbackSubscription{sub},
		listRunDeliveries: []db.ListTaskCallbackDeliveriesByRunRow{{
			TaskCallbackDelivery: db.TaskCallbackDelivery{
				ID:             uuid.New(),
				SubscriptionID: subID,
				RunEventID:     eventID,
				Status:         "success",
				AttemptCount:   1,
				DeliveredAt:    &now,
				CreatedAt:      now,
				UpdatedAt:      now,
			},
			TargetURL: "https://client.example/push",
			EventType: "run.completed",
		}},
		listOwnerSubs: []db.TaskCallbackSubscription{sub},
		batchSubs:     []db.TaskCallbackSubscription{sub},
		updateSub:     sub,
		deleteRows:    1,
		activeSubs:    []db.TaskCallbackSubscription{sub},
		createRunDelivery: db.TaskCallbackDelivery{
			ID:             uuid.New(),
			SubscriptionID: subID,
			RunEventID:     eventID,
			Status:         "pending",
			CreatedAt:      now,
			UpdatedAt:      now,
		},
	}
	svc := &Service{queries: q, allowLocalHTTP: true}

	created, err := svc.CreateTaskCallbackSubscription(context.Background(), runID, userID, &CreateTaskCallbackRequest{
		URL:             "http://127.0.0.1/push",
		EventTypes:      []string{"run.completed", "unknown"},
		AuthScheme:      "Bearer",
		AuthCredentials: "token",
		Metadata:        map[string]interface{}{"client": "local"},
	})
	if err != nil {
		t.Fatalf("CreateTaskCallbackSubscription error = %v", err)
	}
	if created.ID != subID.String() || created.Secret == "" {
		t.Fatalf("created subscription = %#v", created)
	}
	if q.createSubArg.RunID != runID || q.createSubArg.OwnerUserID != userID || q.createSubArg.TargetURL != "http://127.0.0.1/push" {
		t.Fatalf("create sub arg = %#v", q.createSubArg)
	}
	if len(q.createSubArg.EventTypes) != 1 || q.createSubArg.EventTypes[0] != "run.completed" {
		t.Fatalf("create sub event types = %#v", q.createSubArg.EventTypes)
	}
	var metadata map[string]interface{}
	if err := json.Unmarshal(q.createSubArg.Metadata, &metadata); err != nil || metadata["client"] != "local" {
		t.Fatalf("task callback metadata = %#v %v", metadata, err)
	}

	listed, err := svc.ListTaskCallbackSubscriptions(context.Background(), runID, userID)
	if err != nil {
		t.Fatalf("ListTaskCallbackSubscriptions error = %v", err)
	}
	if len(listed) != 1 || q.listRunSubsArg.RunID != runID || q.listRunSubsArg.OwnerUserID != userID {
		t.Fatalf("listed subscriptions/arg = %#v/%#v", listed, q.listRunSubsArg)
	}

	deliveryHistory, err := svc.ListTaskCallbackDeliveries(context.Background(), runID, userID, maxListLimit+1)
	if err != nil {
		t.Fatalf("ListTaskCallbackDeliveries error = %v", err)
	}
	if len(deliveryHistory) != 1 || deliveryHistory[0].EventType != "run.completed" || deliveryHistory[0].DeliveredAt == nil {
		t.Fatalf("delivery history = %#v", deliveryHistory)
	}
	if q.listRunDeliveriesArg.RunID != runID || q.listRunDeliveriesArg.OwnerUserID != userID || q.listRunDeliveriesArg.Limit != defaultListLimit {
		t.Fatalf("delivery history arg = %#v", q.listRunDeliveriesArg)
	}

	ownerList, err := svc.ListTaskCallbackSubscriptionsForOwner(context.Background(), userID, "paused", maxListLimit+1)
	if err != nil {
		t.Fatalf("ListTaskCallbackSubscriptionsForOwner error = %v", err)
	}
	if len(ownerList) != 1 || q.listOwnerArg.Status != "paused" || q.listOwnerArg.Limit != defaultListLimit {
		t.Fatalf("owner list/arg = %#v/%#v", ownerList, q.listOwnerArg)
	}

	batch, err := svc.BatchManageTaskCallbackSubscriptions(context.Background(), userID, &BatchTaskCallbackSubscriptionsRequest{
		SubscriptionIDs: []string{subID.String()},
		Action:          "pause",
	})
	if err != nil {
		t.Fatalf("BatchManageTaskCallbackSubscriptions error = %v", err)
	}
	if batch.UpdatedCount != 1 || q.batchArg.Status != "paused" {
		t.Fatalf("batch/arg = %#v/%#v", batch, q.batchArg)
	}

	updated, err := svc.UpdateTaskCallbackSubscriptionStatus(context.Background(), runID, subID, userID, "active")
	if err != nil {
		t.Fatalf("UpdateTaskCallbackSubscriptionStatus error = %v", err)
	}
	if updated.ID != subID.String() || q.updateArg.Status != "active" {
		t.Fatalf("updated/arg = %#v/%#v", updated, q.updateArg)
	}

	if err := svc.DeleteTaskCallbackSubscription(context.Background(), runID, subID, userID); err != nil {
		t.Fatalf("DeleteTaskCallbackSubscription error = %v", err)
	}
	if q.deleteArg.ID != subID || q.deleteArg.RunID != runID || q.deleteArg.OwnerUserID != userID {
		t.Fatalf("delete arg = %#v", q.deleteArg)
	}

	event := db.RunEvent{ID: eventID, RunID: runID, EventType: "run.completed", Sequence: 2, Payload: []byte(`{"status":"success"}`), CreatedAt: now}
	if err := svc.EnqueueRunEvent(context.Background(), event); err != nil {
		t.Fatalf("EnqueueRunEvent error = %v", err)
	}
	if q.activeArg.RunID != runID || q.activeArg.EventType != "run.completed" || q.createRunDeliveryArg.SubscriptionID != subID || q.createRunDeliveryArg.RunEventID != eventID {
		t.Fatalf("enqueue run event args = %#v/%#v", q.activeArg, q.createRunDeliveryArg)
	}
}

func TestWebhookServiceEnqueueRunEventCachesNoTaskCallbackSubscription(t *testing.T) {
	runID := uuid.New()
	eventID := uuid.New()
	q := &fakeWebhookQueries{}
	svc := &Service{queries: q}

	event := db.RunEvent{ID: eventID, RunID: runID, EventType: "run.started", Sequence: 1, Payload: []byte(`{}`), CreatedAt: time.Now()}
	if err := svc.EnqueueRunEvent(context.Background(), event); err != nil {
		t.Fatalf("first EnqueueRunEvent error = %v", err)
	}
	if q.activeCalls != 1 {
		t.Fatalf("active subscription queries = %d, want 1", q.activeCalls)
	}

	event.ID = uuid.New()
	event.EventType = "run.completed"
	if err := svc.EnqueueRunEvent(context.Background(), event); err != nil {
		t.Fatalf("second EnqueueRunEvent error = %v", err)
	}
	if q.activeCalls != 1 {
		t.Fatalf("cached no-subscription run should not query again, got %d", q.activeCalls)
	}

	sub := db.TaskCallbackSubscription{
		ID:         uuid.New(),
		RunID:      runID,
		TargetURL:  "https://client.example/hook",
		Secret:     "secret",
		EventTypes: []string{"run.completed"},
		Status:     "active",
	}
	svc.rememberTaskCallbackRun(runID, true)
	q.activeSubs = []db.TaskCallbackSubscription{sub}
	event.ID = uuid.New()
	if err := svc.EnqueueRunEvent(context.Background(), event); err != nil {
		t.Fatalf("active EnqueueRunEvent error = %v", err)
	}
	if q.activeCalls != 2 {
		t.Fatalf("active subscription query count = %d, want 2", q.activeCalls)
	}
	if q.createRunDeliveryArg.SubscriptionID != sub.ID || q.createRunDeliveryArg.RunEventID != event.ID {
		t.Fatalf("delivery arg = %#v", q.createRunDeliveryArg)
	}
}

func TestWebhookServiceErrors(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	subID := uuid.New()
	otherUser := uuid.New()
	url := "https://client.example/hook"
	sentinel := errors.New("database stopped")
	tooManyIDs := make([]string, 51)
	for i := range tooManyIDs {
		tooManyIDs[i] = uuid.NewString()
	}

	for _, tc := range []struct {
		name string
		call func(*Service) error
		q    *fakeWebhookQueries
		want int
	}{
		{
			name: "set invalid url",
			call: func(s *Service) error {
				_, err := s.SetWebhook(context.Background(), agentID, userID, "http://example.com/hook")
				return err
			},
			q:    &fakeWebhookQueries{},
			want: http.StatusBadRequest,
		},
		{
			name: "set missing agent",
			call: func(s *Service) error {
				_, err := s.SetWebhook(context.Background(), agentID, userID, url)
				return err
			},
			q:    &fakeWebhookQueries{agentCfgErr: pgx.ErrNoRows},
			want: http.StatusNotFound,
		},
		{
			name: "set owner mismatch",
			call: func(s *Service) error {
				_, err := s.SetWebhook(context.Background(), agentID, userID, url)
				return err
			},
			q:    &fakeWebhookQueries{agentCfg: db.GetAgentWebhookConfigRow{ID: agentID, CreatorID: otherUser}},
			want: http.StatusNotFound,
		},
		{
			name: "set save error",
			call: func(s *Service) error {
				_, err := s.SetWebhook(context.Background(), agentID, userID, url)
				return err
			},
			q:    &fakeWebhookQueries{agentCfg: db.GetAgentWebhookConfigRow{ID: agentID, CreatorID: userID}, setErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "clear db error",
			call: func(s *Service) error {
				return s.ClearWebhook(context.Background(), agentID, userID)
			},
			q:    &fakeWebhookQueries{clearErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "rotate missing agent",
			call: func(s *Service) error {
				_, err := s.RotateSecret(context.Background(), agentID, userID)
				return err
			},
			q:    &fakeWebhookQueries{agentCfgErr: pgx.ErrNoRows},
			want: http.StatusNotFound,
		},
		{
			name: "rotate query error",
			call: func(s *Service) error {
				_, err := s.RotateSecret(context.Background(), agentID, userID)
				return err
			},
			q:    &fakeWebhookQueries{agentCfgErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "rotate owner mismatch",
			call: func(s *Service) error {
				_, err := s.RotateSecret(context.Background(), agentID, userID)
				return err
			},
			q:    &fakeWebhookQueries{agentCfg: db.GetAgentWebhookConfigRow{ID: agentID, CreatorID: otherUser}},
			want: http.StatusNotFound,
		},
		{
			name: "rotate without url",
			call: func(s *Service) error {
				_, err := s.RotateSecret(context.Background(), agentID, userID)
				return err
			},
			q:    &fakeWebhookQueries{agentCfg: db.GetAgentWebhookConfigRow{ID: agentID, CreatorID: userID}},
			want: http.StatusBadRequest,
		},
		{
			name: "rotate save error",
			call: func(s *Service) error {
				_, err := s.RotateSecret(context.Background(), agentID, userID)
				return err
			},
			q:    &fakeWebhookQueries{agentCfg: db.GetAgentWebhookConfigRow{ID: agentID, CreatorID: userID, WebhookURL: &url}, setErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "rotate no rows",
			call: func(s *Service) error {
				_, err := s.RotateSecret(context.Background(), agentID, userID)
				return err
			},
			q:    &fakeWebhookQueries{agentCfg: db.GetAgentWebhookConfigRow{ID: agentID, CreatorID: userID, WebhookURL: &url}},
			want: http.StatusNotFound,
		},
		{
			name: "clear missing",
			call: func(s *Service) error {
				return s.ClearWebhook(context.Background(), agentID, userID)
			},
			q:    &fakeWebhookQueries{},
			want: http.StatusNotFound,
		},
		{
			name: "list deliveries missing agent",
			call: func(s *Service) error {
				_, err := s.ListDeliveries(context.Background(), agentID, userID, 20)
				return err
			},
			q:    &fakeWebhookQueries{agentCfgErr: pgx.ErrNoRows},
			want: http.StatusNotFound,
		},
		{
			name: "list deliveries query owner error",
			call: func(s *Service) error {
				_, err := s.ListDeliveries(context.Background(), agentID, userID, 20)
				return err
			},
			q:    &fakeWebhookQueries{agentCfgErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "list deliveries owner mismatch",
			call: func(s *Service) error {
				_, err := s.ListDeliveries(context.Background(), agentID, userID, 20)
				return err
			},
			q:    &fakeWebhookQueries{agentCfg: db.GetAgentWebhookConfigRow{ID: agentID, CreatorID: otherUser}},
			want: http.StatusNotFound,
		},
		{
			name: "list deliveries query error",
			call: func(s *Service) error {
				_, err := s.ListDeliveries(context.Background(), agentID, userID, 20)
				return err
			},
			q:    &fakeWebhookQueries{agentCfg: db.GetAgentWebhookConfigRow{ID: agentID, CreatorID: userID}, listDeliveriesErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "create task callback nil request",
			call: func(s *Service) error {
				_, err := s.CreateTaskCallbackSubscription(context.Background(), runID, userID, nil)
				return err
			},
			q:    &fakeWebhookQueries{},
			want: http.StatusBadRequest,
		},
		{
			name: "create task callback invalid url",
			call: func(s *Service) error {
				_, err := s.CreateTaskCallbackSubscription(context.Background(), runID, userID, &CreateTaskCallbackRequest{URL: "http://127.0.0.1/hook"})
				return err
			},
			q:    &fakeWebhookQueries{},
			want: http.StatusBadRequest,
		},
		{
			name: "create task callback invalid metadata",
			call: func(s *Service) error {
				_, err := s.CreateTaskCallbackSubscription(context.Background(), runID, userID, &CreateTaskCallbackRequest{URL: url, Metadata: map[string]interface{}{"bad": func() {}}})
				return err
			},
			q:    &fakeWebhookQueries{},
			want: http.StatusBadRequest,
		},
		{
			name: "create task callback missing run",
			call: func(s *Service) error {
				_, err := s.CreateTaskCallbackSubscription(context.Background(), runID, userID, &CreateTaskCallbackRequest{URL: url})
				return err
			},
			q:    &fakeWebhookQueries{runErr: pgx.ErrNoRows},
			want: http.StatusNotFound,
		},
		{
			name: "create task callback wrong owner",
			call: func(s *Service) error {
				_, err := s.CreateTaskCallbackSubscription(context.Background(), runID, userID, &CreateTaskCallbackRequest{URL: url})
				return err
			},
			q:    &fakeWebhookQueries{run: db.Run{ID: runID, UserID: otherUser}},
			want: http.StatusNotFound,
		},
		{
			name: "create task callback query error",
			call: func(s *Service) error {
				_, err := s.CreateTaskCallbackSubscription(context.Background(), runID, userID, &CreateTaskCallbackRequest{URL: url})
				return err
			},
			q:    &fakeWebhookQueries{runErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "create task callback insert error",
			call: func(s *Service) error {
				_, err := s.CreateTaskCallbackSubscription(context.Background(), runID, userID, &CreateTaskCallbackRequest{URL: url})
				return err
			},
			q:    &fakeWebhookQueries{run: db.Run{ID: runID, UserID: userID}, createSubErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "list task callbacks query error",
			call: func(s *Service) error {
				_, err := s.ListTaskCallbackSubscriptions(context.Background(), runID, userID)
				return err
			},
			q:    &fakeWebhookQueries{run: db.Run{ID: runID, UserID: userID}, listSubsErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "managed list invalid status",
			call: func(s *Service) error {
				_, err := s.ListTaskCallbackSubscriptionsForOwner(context.Background(), userID, "deleted", 20)
				return err
			},
			q:    &fakeWebhookQueries{},
			want: http.StatusBadRequest,
		},
		{
			name: "managed list query error",
			call: func(s *Service) error {
				_, err := s.ListTaskCallbackSubscriptionsForOwner(context.Background(), userID, "active", 20)
				return err
			},
			q:    &fakeWebhookQueries{listOwnerErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "batch nil request",
			call: func(s *Service) error {
				_, err := s.BatchManageTaskCallbackSubscriptions(context.Background(), userID, nil)
				return err
			},
			q:    &fakeWebhookQueries{},
			want: http.StatusBadRequest,
		},
		{
			name: "batch invalid action",
			call: func(s *Service) error {
				_, err := s.BatchManageTaskCallbackSubscriptions(context.Background(), userID, &BatchTaskCallbackSubscriptionsRequest{SubscriptionIDs: []string{subID.String()}, Action: "archive"})
				return err
			},
			q:    &fakeWebhookQueries{},
			want: http.StatusBadRequest,
		},
		{
			name: "batch empty ids",
			call: func(s *Service) error {
				_, err := s.BatchManageTaskCallbackSubscriptions(context.Background(), userID, &BatchTaskCallbackSubscriptionsRequest{Action: "pause"})
				return err
			},
			q:    &fakeWebhookQueries{},
			want: http.StatusBadRequest,
		},
		{
			name: "batch invalid id",
			call: func(s *Service) error {
				_, err := s.BatchManageTaskCallbackSubscriptions(context.Background(), userID, &BatchTaskCallbackSubscriptionsRequest{SubscriptionIDs: []string{"bad"}, Action: "pause"})
				return err
			},
			q:    &fakeWebhookQueries{},
			want: http.StatusBadRequest,
		},
		{
			name: "batch too many ids",
			call: func(s *Service) error {
				_, err := s.BatchManageTaskCallbackSubscriptions(context.Background(), userID, &BatchTaskCallbackSubscriptionsRequest{SubscriptionIDs: tooManyIDs, Action: "pause"})
				return err
			},
			q:    &fakeWebhookQueries{},
			want: http.StatusBadRequest,
		},
		{
			name: "batch update error",
			call: func(s *Service) error {
				_, err := s.BatchManageTaskCallbackSubscriptions(context.Background(), userID, &BatchTaskCallbackSubscriptionsRequest{SubscriptionIDs: []string{subID.String()}, Action: "resume"})
				return err
			},
			q:    &fakeWebhookQueries{batchErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "update invalid status",
			call: func(s *Service) error {
				_, err := s.UpdateTaskCallbackSubscriptionStatus(context.Background(), runID, subID, userID, "failed")
				return err
			},
			q:    &fakeWebhookQueries{},
			want: http.StatusBadRequest,
		},
		{
			name: "update missing subscription",
			call: func(s *Service) error {
				_, err := s.UpdateTaskCallbackSubscriptionStatus(context.Background(), runID, subID, userID, "paused")
				return err
			},
			q:    &fakeWebhookQueries{run: db.Run{ID: runID, UserID: userID}, updateErr: pgx.ErrNoRows},
			want: http.StatusNotFound,
		},
		{
			name: "update query error",
			call: func(s *Service) error {
				_, err := s.UpdateTaskCallbackSubscriptionStatus(context.Background(), runID, subID, userID, "paused")
				return err
			},
			q:    &fakeWebhookQueries{run: db.Run{ID: runID, UserID: userID}, updateErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "delete missing subscription",
			call: func(s *Service) error {
				return s.DeleteTaskCallbackSubscription(context.Background(), runID, subID, userID)
			},
			q:    &fakeWebhookQueries{run: db.Run{ID: runID, UserID: userID}},
			want: http.StatusNotFound,
		},
		{
			name: "delete query error",
			call: func(s *Service) error {
				return s.DeleteTaskCallbackSubscription(context.Background(), runID, subID, userID)
			},
			q:    &fakeWebhookQueries{run: db.Run{ID: runID, UserID: userID}, deleteErr: sentinel},
			want: http.StatusInternalServerError,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireWebhookHTTPStatus(t, tc.call(&Service{queries: tc.q, httpClient: http.DefaultClient}), tc.want)
		})
	}
}

type webhookRoundTripper func(*http.Request) (*http.Response, error)

func (f webhookRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func webhookHTTPClient(status int, body string, err error) *http.Client {
	return &http.Client{Transport: webhookRoundTripper(func(*http.Request) (*http.Response, error) {
		if err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode:    status,
			Header:        make(http.Header),
			Body:          io.NopCloser(strings.NewReader(body)),
			ContentLength: int64(len(body)),
		}, nil
	})}
}

type fakeWebhookQueries struct {
	setWebhookArg db.SetAgentWebhookParams
	setRows       int64
	setErr        error

	clearWebhookArg db.ClearAgentWebhookParams
	clearRows       int64
	clearErr        error

	agentCfg    db.GetAgentWebhookConfigRow
	agentCfgErr error

	createDeliveryArg db.CreateWebhookDeliveryParams
	createDelivery    db.WebhookDelivery
	createDeliveryErr error

	deliveryRow db.GetWebhookDeliveryRow
	deliveryErr error

	successArg db.MarkDeliverySuccessParams
	successErr error
	retryArg   db.MarkDeliveryFailedRetryParams
	retryErr   error
	finalArg   db.MarkDeliveryFailedFinalParams
	finalErr   error

	listDeliveriesArg db.ListDeliveriesByAgentParams
	deliveries        []db.WebhookDelivery
	listDeliveriesErr error
	pendingDeliveries []db.WebhookDelivery
	pendingErr        error

	run    db.Run
	runErr error

	createSubArg db.CreateTaskCallbackSubscriptionParams
	createSub    db.TaskCallbackSubscription
	createSubErr error

	latestEventArg db.GetLatestRunEventForTypesParams
	latestEvent    db.RunEvent
	latestEventErr error

	listRunSubsArg db.ListTaskCallbackSubscriptionsByRunParams
	listSubs       []db.TaskCallbackSubscription
	listSubsErr    error

	listRunDeliveriesArg db.ListTaskCallbackDeliveriesByRunParams
	listRunDeliveries    []db.ListTaskCallbackDeliveriesByRunRow
	listRunDeliveriesErr error

	listOwnerArg  db.ListTaskCallbackSubscriptionsByOwnerParams
	listOwnerSubs []db.TaskCallbackSubscription
	listOwnerErr  error

	batchArg  db.BatchUpdateTaskCallbackSubscriptionsForOwnerParams
	batchSubs []db.TaskCallbackSubscription
	batchErr  error

	updateArg db.UpdateTaskCallbackSubscriptionStatusForOwnerParams
	updateSub db.TaskCallbackSubscription
	updateErr error

	deleteArg  db.DeleteTaskCallbackSubscriptionForOwnerParams
	deleteRows int64
	deleteErr  error

	activeArg   db.ListActiveTaskCallbackSubscriptionsForEventParams
	activeSubs  []db.TaskCallbackSubscription
	activeErr   error
	activeCalls int

	createRunDeliveryArg db.CreateTaskCallbackDeliveryParams
	createRunDelivery    db.TaskCallbackDelivery
	createRunDeliveryErr error

	runDeliveryRow db.GetTaskCallbackDeliveryByIDRow
	runDeliveryErr error

	runSuccessArg db.MarkTaskCallbackDeliverySuccessParams
	runSuccessErr error
	runRetryArg   db.MarkTaskCallbackDeliveryFailedRetryParams
	runRetryErr   error
	runFinalArg   db.MarkTaskCallbackDeliveryFailedFinalParams
	runFinalErr   error

	incrementSubscriptionID uuid.UUID
	incrementErr            error
	resetSubscriptionID     uuid.UUID
	resetErr                error

	pendingRunDeliveries []db.TaskCallbackDelivery
	pendingRunErr        error
}

func (q *fakeWebhookQueries) SetAgentWebhook(_ context.Context, arg db.SetAgentWebhookParams) (int64, error) {
	q.setWebhookArg = arg
	return q.setRows, q.setErr
}

func (q *fakeWebhookQueries) ClearAgentWebhook(_ context.Context, arg db.ClearAgentWebhookParams) (int64, error) {
	q.clearWebhookArg = arg
	return q.clearRows, q.clearErr
}

func (q *fakeWebhookQueries) GetAgentWebhookConfig(context.Context, uuid.UUID) (db.GetAgentWebhookConfigRow, error) {
	return q.agentCfg, q.agentCfgErr
}

func (q *fakeWebhookQueries) CreateWebhookDelivery(_ context.Context, arg db.CreateWebhookDeliveryParams) (db.WebhookDelivery, error) {
	q.createDeliveryArg = arg
	if q.createDelivery.ID == uuid.Nil {
		q.createDelivery = db.WebhookDelivery{
			ID:        uuid.New(),
			AgentID:   arg.AgentID,
			RunID:     arg.RunID,
			URL:       arg.URL,
			Payload:   arg.Payload,
			Status:    "pending",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
	}
	return q.createDelivery, q.createDeliveryErr
}

func (q *fakeWebhookQueries) GetWebhookDeliveryByID(context.Context, uuid.UUID) (db.GetWebhookDeliveryRow, error) {
	return q.deliveryRow, q.deliveryErr
}

func (q *fakeWebhookQueries) MarkDeliverySuccess(_ context.Context, arg db.MarkDeliverySuccessParams) error {
	q.successArg = arg
	return q.successErr
}

func (q *fakeWebhookQueries) MarkDeliveryFailedRetry(_ context.Context, arg db.MarkDeliveryFailedRetryParams) error {
	q.retryArg = arg
	return q.retryErr
}

func (q *fakeWebhookQueries) MarkDeliveryFailedFinal(_ context.Context, arg db.MarkDeliveryFailedFinalParams) error {
	q.finalArg = arg
	return q.finalErr
}

func (q *fakeWebhookQueries) ListDeliveriesByAgent(_ context.Context, arg db.ListDeliveriesByAgentParams) ([]db.WebhookDelivery, error) {
	q.listDeliveriesArg = arg
	return q.deliveries, q.listDeliveriesErr
}

func (q *fakeWebhookQueries) ListPendingDeliveries(context.Context) ([]db.WebhookDelivery, error) {
	return q.pendingDeliveries, q.pendingErr
}

func (q *fakeWebhookQueries) GetRunByID(context.Context, uuid.UUID) (db.Run, error) {
	return q.run, q.runErr
}

func (q *fakeWebhookQueries) CreateTaskCallbackSubscription(_ context.Context, arg db.CreateTaskCallbackSubscriptionParams) (db.TaskCallbackSubscription, error) {
	q.createSubArg = arg
	if q.createSub.ID == uuid.Nil {
		q.createSub = db.TaskCallbackSubscription{
			ID:              uuid.New(),
			RunID:           arg.RunID,
			OwnerUserID:     arg.OwnerUserID,
			TargetURL:       arg.TargetURL,
			Secret:          arg.Secret,
			EventTypes:      arg.EventTypes,
			AuthScheme:      arg.AuthScheme,
			AuthCredentials: arg.AuthCredentials,
			Metadata:        arg.Metadata,
			Status:          "active",
			CreatedAt:       time.Now(),
			UpdatedAt:       time.Now(),
		}
	}
	return q.createSub, q.createSubErr
}

func (q *fakeWebhookQueries) GetLatestRunEventForTypes(_ context.Context, arg db.GetLatestRunEventForTypesParams) (db.RunEvent, error) {
	q.latestEventArg = arg
	return q.latestEvent, q.latestEventErr
}

func (q *fakeWebhookQueries) ListTaskCallbackSubscriptionsByRun(_ context.Context, arg db.ListTaskCallbackSubscriptionsByRunParams) ([]db.TaskCallbackSubscription, error) {
	q.listRunSubsArg = arg
	return q.listSubs, q.listSubsErr
}

func (q *fakeWebhookQueries) ListTaskCallbackDeliveriesByRun(_ context.Context, arg db.ListTaskCallbackDeliveriesByRunParams) ([]db.ListTaskCallbackDeliveriesByRunRow, error) {
	q.listRunDeliveriesArg = arg
	return q.listRunDeliveries, q.listRunDeliveriesErr
}

func (q *fakeWebhookQueries) ListTaskCallbackSubscriptionsByOwner(_ context.Context, arg db.ListTaskCallbackSubscriptionsByOwnerParams) ([]db.TaskCallbackSubscription, error) {
	q.listOwnerArg = arg
	return q.listOwnerSubs, q.listOwnerErr
}

func (q *fakeWebhookQueries) BatchUpdateTaskCallbackSubscriptionsForOwner(_ context.Context, arg db.BatchUpdateTaskCallbackSubscriptionsForOwnerParams) ([]db.TaskCallbackSubscription, error) {
	q.batchArg = arg
	return q.batchSubs, q.batchErr
}

func (q *fakeWebhookQueries) UpdateTaskCallbackSubscriptionStatusForOwner(_ context.Context, arg db.UpdateTaskCallbackSubscriptionStatusForOwnerParams) (db.TaskCallbackSubscription, error) {
	q.updateArg = arg
	return q.updateSub, q.updateErr
}

func (q *fakeWebhookQueries) DeleteTaskCallbackSubscriptionForOwner(_ context.Context, arg db.DeleteTaskCallbackSubscriptionForOwnerParams) (int64, error) {
	q.deleteArg = arg
	return q.deleteRows, q.deleteErr
}

func (q *fakeWebhookQueries) ListActiveTaskCallbackSubscriptionsForEvent(_ context.Context, arg db.ListActiveTaskCallbackSubscriptionsForEventParams) ([]db.TaskCallbackSubscription, error) {
	q.activeArg = arg
	q.activeCalls++
	return q.activeSubs, q.activeErr
}

func (q *fakeWebhookQueries) CreateTaskCallbackDelivery(_ context.Context, arg db.CreateTaskCallbackDeliveryParams) (db.TaskCallbackDelivery, error) {
	q.createRunDeliveryArg = arg
	if q.createRunDelivery.ID == uuid.Nil {
		q.createRunDelivery = db.TaskCallbackDelivery{
			ID:             uuid.New(),
			SubscriptionID: arg.SubscriptionID,
			RunEventID:     arg.RunEventID,
			Payload:        arg.Payload,
			Status:         "pending",
			CreatedAt:      time.Now(),
			UpdatedAt:      time.Now(),
		}
	}
	return q.createRunDelivery, q.createRunDeliveryErr
}

func (q *fakeWebhookQueries) GetTaskCallbackDeliveryByID(context.Context, uuid.UUID) (db.GetTaskCallbackDeliveryByIDRow, error) {
	return q.runDeliveryRow, q.runDeliveryErr
}

func (q *fakeWebhookQueries) MarkTaskCallbackDeliverySuccess(_ context.Context, arg db.MarkTaskCallbackDeliverySuccessParams) error {
	q.runSuccessArg = arg
	return q.runSuccessErr
}

func (q *fakeWebhookQueries) MarkTaskCallbackDeliveryFailedRetry(_ context.Context, arg db.MarkTaskCallbackDeliveryFailedRetryParams) error {
	q.runRetryArg = arg
	return q.runRetryErr
}

func (q *fakeWebhookQueries) MarkTaskCallbackDeliveryFailedFinal(_ context.Context, arg db.MarkTaskCallbackDeliveryFailedFinalParams) error {
	q.runFinalArg = arg
	return q.runFinalErr
}

func (q *fakeWebhookQueries) IncrementTaskCallbackSubscriptionFailure(_ context.Context, id uuid.UUID) error {
	q.incrementSubscriptionID = id
	return q.incrementErr
}

func (q *fakeWebhookQueries) ResetTaskCallbackSubscriptionFailures(_ context.Context, id uuid.UUID) error {
	q.resetSubscriptionID = id
	return q.resetErr
}

func (q *fakeWebhookQueries) ListPendingTaskCallbackDeliveries(context.Context) ([]db.TaskCallbackDelivery, error) {
	return q.pendingRunDeliveries, q.pendingRunErr
}
