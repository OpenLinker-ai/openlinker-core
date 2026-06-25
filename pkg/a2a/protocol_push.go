package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/webhook"
)

type taskCallbackManager interface {
	CreateTaskCallbackSubscription(ctx context.Context, runID, userID uuid.UUID, req *webhook.CreateTaskCallbackRequest) (*webhook.TaskCallbackSubscriptionResponse, error)
	DeleteTaskCallbackSubscription(ctx context.Context, runID, subscriptionID, userID uuid.UUID) error
}

func (s *Service) SetTaskCallbackManager(manager taskCallbackManager) {
	s.taskCallbackManager = manager
}

func taskCallbackConfigFromCallRequest(req *CallAgentRequest) *A2APushNotificationConfig {
	if req == nil {
		return nil
	}
	if req.TaskCallback != nil {
		return req.TaskCallback
	}
	if req.PushNotification != nil {
		return req.PushNotification
	}
	if req.PushNotificationAlias != nil {
		return req.PushNotificationAlias
	}
	return req.PushNotificationConfig
}

func (s *Service) createCallerTaskCallback(
	ctx context.Context,
	userID uuid.UUID,
	runID string,
	cfg *A2APushNotificationConfig,
) (*runtime.RunTaskCallbackResponse, error) {
	if cfg == nil {
		return nil, nil
	}
	if err := s.validateCallerTaskCallbackConfig(cfg); err != nil {
		return nil, err
	}
	parsedRunID, err := uuid.Parse(strings.TrimSpace(runID))
	if err != nil {
		return nil, httpx.Internal("子运行 ID 格式错误")
	}
	scheme, credentials := callbackAuthFromA2AConfig(*cfg)
	resp, err := s.taskCallbackManager.CreateTaskCallbackSubscription(ctx, parsedRunID, userID, &webhook.CreateTaskCallbackRequest{
		URL:             cfg.URL,
		Secret:          cfg.Secret,
		EventTypes:      defaultTaskCallbackEventTypes(taskCallbackEventTypesFromA2A(*cfg)),
		AuthScheme:      scheme,
		AuthCredentials: credentials,
		Metadata:        cfg.Metadata,
	})
	if err != nil {
		return nil, err
	}
	return &runtime.RunTaskCallbackResponse{
		ID:                  resp.ID,
		RunID:               resp.RunID,
		TargetURL:           resp.TargetURL,
		EventTypes:          resp.EventTypes,
		AuthScheme:          resp.AuthScheme,
		Status:              resp.Status,
		ConsecutiveFailures: resp.ConsecutiveFailures,
		Secret:              resp.Secret,
		CreatedAt:           resp.CreatedAt,
		UpdatedAt:           resp.UpdatedAt,
	}, nil
}

func (s *Service) validateCallerTaskCallbackConfig(cfg *A2APushNotificationConfig) error {
	if cfg == nil {
		return nil
	}
	if s.taskCallbackManager == nil {
		return httpx.ServiceUnavailable("任务回调未启用")
	}
	if strings.TrimSpace(cfg.URL) == "" {
		return httpx.BadRequest("task_callback.url 不能为空")
	}
	return nil
}

func (s *Service) SetPushNotificationConfig(ctx context.Context, userID uuid.UUID, slug string, params *A2ATaskPushConfigParams) (*A2ATaskPushNotificationConfig, error) {
	if s.taskCallbackManager == nil {
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
	scheme, credentials := callbackAuthFromA2AConfig(cfg)
	resp, err := s.taskCallbackManager.CreateTaskCallbackSubscription(ctx, runID, userID, &webhook.CreateTaskCallbackRequest{
		URL:             cfg.URL,
		Secret:          cfg.Secret,
		EventTypes:      defaultTaskCallbackEventTypes(taskCallbackEventTypesFromA2A(cfg)),
		AuthScheme:      scheme,
		AuthCredentials: credentials,
		Metadata:        cfg.Metadata,
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
	items, err := s.listTaskCallbackSubscriptionsForA2A(ctx, runID, userID)
	if err != nil {
		return nil, err
	}
	configID := strings.TrimSpace(params.PushNotificationConfigID)
	if configID == "" {
		configID = strings.TrimSpace(params.PushNotificationConfig.ID)
	}
	if configID == "" && len(items) == 1 {
		item := a2aPushConfigFromTaskCallback(taskID, items[0], false)
		return &item, nil
	}
	if configID == "" {
		return nil, httpx.BadRequest("pushNotificationConfigId 不能为空")
	}
	for _, item := range items {
		if item.ID.String() == configID {
			out := a2aPushConfigFromTaskCallback(taskID, item, false)
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
	items, err := s.listTaskCallbackSubscriptionsForA2A(ctx, runID, userID)
	if err != nil {
		return nil, err
	}
	out := make([]A2ATaskPushNotificationConfig, 0, len(items))
	for _, item := range items {
		out = append(out, a2aPushConfigFromTaskCallback(taskID, item, false))
	}
	return &A2ATaskPushConfigList{Items: out}, nil
}

func (s *Service) DeletePushNotificationConfig(ctx context.Context, userID uuid.UUID, slug string, params *A2ATaskPushConfigParams) error {
	if s.taskCallbackManager == nil {
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
	return s.taskCallbackManager.DeleteTaskCallbackSubscription(ctx, runID, subID, userID)
}

func (s *Service) listTaskCallbackSubscriptionsForA2A(ctx context.Context, runID, userID uuid.UUID) ([]db.TaskCallbackSubscription, error) {
	items, err := s.queries.ListTaskCallbackSubscriptionsByRun(ctx, db.ListTaskCallbackSubscriptionsByRunParams{
		RunID:       runID,
		OwnerUserID: userID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return []db.TaskCallbackSubscription{}, nil
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

func callbackAuthFromA2AConfig(cfg A2APushNotificationConfig) (string, string) {
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

func taskCallbackEventTypesFromA2A(cfg A2APushNotificationConfig) []string {
	if len(cfg.EventTypes) > 0 {
		return cfg.EventTypes
	}
	return cfg.EventTypesAlias
}

func defaultTaskCallbackEventTypes(raw []string) []string {
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

func a2aPushConfigFromTaskCallback(taskID string, sub db.TaskCallbackSubscription, includeCredentials bool) A2ATaskPushNotificationConfig {
	cfg := A2APushNotificationConfig{
		ID:         sub.ID.String(),
		URL:        sub.TargetURL,
		EventTypes: append([]string{}, sub.EventTypes...),
		Metadata:   a2aMetadataFromTaskCallback(sub),
	}
	if sub.AuthScheme != nil {
		auth := &A2APushAuthenticationInfo{Scheme: *sub.AuthScheme}
		if includeCredentials && sub.AuthCredentials != nil {
			auth.Credentials = *sub.AuthCredentials
		}
		cfg.Authentication = auth
	}
	return A2ATaskPushNotificationConfig{TaskID: taskID, PushNotificationConfig: cfg}
}

func a2aMetadataFromTaskCallback(sub db.TaskCallbackSubscription) map[string]interface{} {
	out := map[string]interface{}{
		"openlinker_subscription_status": sub.Status,
	}
	if len(sub.Metadata) > 0 {
		_ = json.Unmarshal(sub.Metadata, &out)
	}
	out["openlinker_subscription_status"] = sub.Status
	out["openlinker_consecutive_failures"] = sub.ConsecutiveFailures
	return out
}
