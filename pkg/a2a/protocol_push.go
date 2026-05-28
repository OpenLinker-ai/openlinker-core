package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
	"github.com/kinzhi/openlinker-core/pkg/webhook"
)

type runPushManager interface {
	CreateRunWebhookSubscription(ctx context.Context, runID, userID uuid.UUID, req *webhook.CreateRunWebhookRequest) (*webhook.RunWebhookSubscriptionResponse, error)
	DeleteRunWebhookSubscription(ctx context.Context, runID, subscriptionID, userID uuid.UUID) error
}

func (s *Service) SetRunPushManager(manager runPushManager) {
	s.runPush = manager
}

func (s *Service) SetPushNotificationConfig(ctx context.Context, userID uuid.UUID, slug string, params *A2ATaskPushConfigParams) (*A2ATaskPushNotificationConfig, error) {
	if s.runPush == nil {
		return nil, httpx.ServiceUnavailable("A2A Push Notification Config 未启用")
	}
	if params == nil {
		return nil, httpx.BadRequest("params 不能为空")
	}
	taskID := taskIDFromPushParams(params)
	runID, err := s.ensureProtocolRun(ctx, userID, slug, taskID)
	if err != nil {
		return nil, err
	}
	cfg := params.PushNotificationConfig
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, httpx.BadRequest("pushNotificationConfig.url 不能为空")
	}
	scheme, credentials := pushAuthFromConfig(cfg)
	resp, err := s.runPush.CreateRunWebhookSubscription(ctx, runID, userID, &webhook.CreateRunWebhookRequest{
		URL:                 cfg.URL,
		EventTypes:          pushEventTypes(cfg.EventTypes),
		PushAuthScheme:      scheme,
		PushAuthCredentials: credentials,
		PushMetadata:        cfg.Metadata,
	})
	if err != nil {
		return nil, err
	}
	out := A2ATaskPushNotificationConfig{
		TaskID: taskID,
		PushNotificationConfig: A2APushNotificationConfig{
			ID:         resp.ID,
			URL:        resp.TargetURL,
			EventTypes: resp.EventTypes,
			Metadata:   cfg.Metadata,
		},
	}
	if scheme != "" {
		out.PushNotificationConfig.Authentication = &A2APushAuthenticationInfo{Scheme: scheme}
	}
	return &out, nil
}

func (s *Service) GetPushNotificationConfig(ctx context.Context, userID uuid.UUID, slug string, params *A2ATaskPushConfigParams) (*A2ATaskPushNotificationConfig, error) {
	if params == nil {
		return nil, httpx.BadRequest("params 不能为空")
	}
	taskID := taskIDFromPushParams(params)
	runID, err := s.ensureProtocolRun(ctx, userID, slug, taskID)
	if err != nil {
		return nil, err
	}
	items, err := s.listPushSubscriptions(ctx, runID, userID)
	if err != nil {
		return nil, err
	}
	configID := strings.TrimSpace(params.PushNotificationConfigID)
	if configID == "" {
		configID = strings.TrimSpace(params.PushNotificationConfig.ID)
	}
	if configID == "" && len(items) == 1 {
		item := pushConfigFromSubscription(taskID, items[0], false)
		return &item, nil
	}
	if configID == "" {
		return nil, httpx.BadRequest("pushNotificationConfigId 不能为空")
	}
	for _, item := range items {
		if item.ID.String() == configID {
			out := pushConfigFromSubscription(taskID, item, false)
			return &out, nil
		}
	}
	return nil, httpx.NotFound("Push Notification Config 不存在")
}

func (s *Service) ListPushNotificationConfigs(ctx context.Context, userID uuid.UUID, slug string, params *A2ATaskPushConfigParams) (*A2ATaskPushConfigList, error) {
	if params == nil {
		return nil, httpx.BadRequest("params 不能为空")
	}
	taskID := taskIDFromPushParams(params)
	runID, err := s.ensureProtocolRun(ctx, userID, slug, taskID)
	if err != nil {
		return nil, err
	}
	items, err := s.listPushSubscriptions(ctx, runID, userID)
	if err != nil {
		return nil, err
	}
	out := make([]A2ATaskPushNotificationConfig, 0, len(items))
	for _, item := range items {
		out = append(out, pushConfigFromSubscription(taskID, item, false))
	}
	return &A2ATaskPushConfigList{Items: out}, nil
}

func (s *Service) DeletePushNotificationConfig(ctx context.Context, userID uuid.UUID, slug string, params *A2ATaskPushConfigParams) error {
	if s.runPush == nil {
		return httpx.ServiceUnavailable("A2A Push Notification Config 未启用")
	}
	if params == nil {
		return httpx.BadRequest("params 不能为空")
	}
	taskID := taskIDFromPushParams(params)
	runID, err := s.ensureProtocolRun(ctx, userID, slug, taskID)
	if err != nil {
		return err
	}
	configID := strings.TrimSpace(params.PushNotificationConfigID)
	if configID == "" {
		configID = strings.TrimSpace(params.PushNotificationConfig.ID)
	}
	if configID == "" {
		return httpx.BadRequest("pushNotificationConfigId 不能为空")
	}
	subID, err := uuid.Parse(configID)
	if err != nil {
		return httpx.BadRequest("pushNotificationConfigId 不是合法 uuid")
	}
	return s.runPush.DeleteRunWebhookSubscription(ctx, runID, subID, userID)
}

func (s *Service) listPushSubscriptions(ctx context.Context, runID, userID uuid.UUID) ([]db.RunWebhookSubscription, error) {
	items, err := s.queries.ListRunWebhookSubscriptionsByRun(ctx, db.ListRunWebhookSubscriptionsByRunParams{
		RunID:       runID,
		OwnerUserID: userID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return []db.RunWebhookSubscription{}, nil
		}
		return nil, httpx.Internal("查询 Push Notification Config 失败")
	}
	return items, nil
}

func taskIDFromPushParams(params *A2ATaskPushConfigParams) string {
	if params == nil {
		return ""
	}
	if strings.TrimSpace(params.TaskID) != "" {
		return strings.TrimSpace(params.TaskID)
	}
	return strings.TrimSpace(params.ID)
}

func pushAuthFromConfig(cfg A2APushNotificationConfig) (string, string) {
	if cfg.Authentication != nil {
		scheme := strings.TrimSpace(cfg.Authentication.Scheme)
		credentials := strings.TrimSpace(cfg.Authentication.Credentials)
		if scheme != "" && credentials != "" {
			return scheme, credentials
		}
	}
	if strings.TrimSpace(cfg.Token) != "" {
		return "Bearer", strings.TrimSpace(cfg.Token)
	}
	return "", ""
}

func pushEventTypes(raw []string) []string {
	if len(raw) > 0 {
		return raw
	}
	return []string{
		"run.created",
		"run.started",
		"run.dispatch.pending",
		"run.dispatch.claimed",
		"run.message.delta",
		"run.artifact.delta",
		"run.completed",
		"run.failed",
		"run.canceled",
	}
}

func pushConfigFromSubscription(taskID string, sub db.RunWebhookSubscription, includeCredentials bool) A2ATaskPushNotificationConfig {
	cfg := A2APushNotificationConfig{
		ID:         sub.ID.String(),
		URL:        sub.TargetURL,
		EventTypes: append([]string{}, sub.EventTypes...),
		Metadata:   pushMetadataFromSubscription(sub),
	}
	if sub.PushAuthScheme != nil {
		auth := &A2APushAuthenticationInfo{Scheme: *sub.PushAuthScheme}
		if includeCredentials && sub.PushAuthCredentials != nil {
			auth.Credentials = *sub.PushAuthCredentials
		}
		cfg.Authentication = auth
	}
	return A2ATaskPushNotificationConfig{TaskID: taskID, PushNotificationConfig: cfg}
}

func pushMetadataFromSubscription(sub db.RunWebhookSubscription) map[string]interface{} {
	out := map[string]interface{}{
		"openlinker_subscription_status": sub.Status,
	}
	if len(sub.PushMetadata) > 0 {
		_ = json.Unmarshal(sub.PushMetadata, &out)
	}
	out["openlinker_subscription_status"] = sub.Status
	out["openlinker_consecutive_failures"] = sub.ConsecutiveFailures
	return out
}
