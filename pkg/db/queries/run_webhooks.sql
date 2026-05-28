-- run_webhooks.sql
-- A2A-style run push subscriptions. Payloads are derived from run_events.

-- name: CreateRunWebhookSubscription :one
INSERT INTO run_webhook_subscriptions (
    run_id, owner_user_id, caller_agent_id, target_url, secret, event_types,
    push_auth_scheme, push_auth_credentials, push_metadata
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, COALESCE($9, '{}'::jsonb)
)
RETURNING id, run_id, owner_user_id, caller_agent_id, target_url, secret,
          event_types, push_auth_scheme, push_auth_credentials, push_metadata,
          status, consecutive_failures, created_at, updated_at, deleted_at;

-- name: ListRunWebhookSubscriptionsByRun :many
SELECT id, run_id, owner_user_id, caller_agent_id, target_url, secret,
       event_types, push_auth_scheme, push_auth_credentials, push_metadata,
       status, consecutive_failures, created_at, updated_at, deleted_at
FROM run_webhook_subscriptions
WHERE run_id = $1
  AND owner_user_id = $2
  AND status <> 'deleted'
ORDER BY created_at DESC;

-- name: ListRunWebhookSubscriptionsByOwner :many
SELECT id, run_id, owner_user_id, caller_agent_id, target_url, secret,
       event_types, push_auth_scheme, push_auth_credentials, push_metadata,
       status, consecutive_failures, created_at, updated_at, deleted_at
FROM run_webhook_subscriptions
WHERE owner_user_id = $1
  AND status <> 'deleted'
  AND ($2::text = '' OR status = $2)
ORDER BY updated_at DESC, created_at DESC
LIMIT $3;

-- name: DeleteRunWebhookSubscriptionForOwner :execrows
UPDATE run_webhook_subscriptions
SET status = 'deleted',
    deleted_at = NOW()
WHERE id = $1
  AND run_id = $2
  AND owner_user_id = $3
  AND status <> 'deleted';

-- name: UpdateRunWebhookSubscriptionStatusForOwner :one
UPDATE run_webhook_subscriptions
SET status = $4,
    consecutive_failures = CASE WHEN $4 = 'active' THEN 0 ELSE consecutive_failures END,
    updated_at = NOW()
WHERE id = $1
  AND run_id = $2
  AND owner_user_id = $3
  AND status <> 'deleted'
RETURNING id, run_id, owner_user_id, caller_agent_id, target_url, secret,
          event_types, push_auth_scheme, push_auth_credentials, push_metadata,
          status, consecutive_failures, created_at, updated_at, deleted_at;

-- name: BatchUpdateRunWebhookSubscriptionsForOwner :many
UPDATE run_webhook_subscriptions
SET status = $3,
    consecutive_failures = CASE WHEN $3 = 'active' THEN 0 ELSE consecutive_failures END,
    deleted_at = CASE WHEN $3 = 'deleted' THEN NOW() ELSE deleted_at END,
    updated_at = NOW()
WHERE owner_user_id = $1
  AND id = ANY($2::uuid[])
  AND status <> 'deleted'
RETURNING id, run_id, owner_user_id, caller_agent_id, target_url, secret,
          event_types, push_auth_scheme, push_auth_credentials, push_metadata,
          status, consecutive_failures, created_at, updated_at, deleted_at;

-- name: ListActiveRunWebhookSubscriptionsForEvent :many
SELECT id, run_id, owner_user_id, caller_agent_id, target_url, secret,
       event_types, push_auth_scheme, push_auth_credentials, push_metadata,
       status, consecutive_failures, created_at, updated_at, deleted_at
FROM run_webhook_subscriptions
WHERE run_id = $1
  AND status = 'active'
  AND $2 = ANY(event_types)
ORDER BY created_at ASC;

-- name: CreateRunWebhookDelivery :one
INSERT INTO run_webhook_deliveries (
    subscription_id, run_event_id, payload, status, attempt_count, next_retry_at
) VALUES (
    $1, $2, $3, 'pending', 0, NOW()
)
ON CONFLICT (subscription_id, run_event_id) DO UPDATE
SET payload = EXCLUDED.payload
RETURNING id, subscription_id, run_event_id, payload, status,
          response_status, response_body, error_message,
          attempt_count, next_retry_at, delivered_at, created_at, updated_at;

-- name: GetRunWebhookDeliveryByID :one
SELECT d.id, d.subscription_id, d.run_event_id, d.payload, d.status,
       d.response_status, d.response_body, d.error_message,
       d.attempt_count, d.next_retry_at, d.delivered_at, d.created_at, d.updated_at,
       s.target_url, s.secret, s.push_auth_scheme, s.push_auth_credentials, e.event_type
FROM run_webhook_deliveries d
JOIN run_webhook_subscriptions s ON s.id = d.subscription_id
JOIN run_events e ON e.id = d.run_event_id
WHERE d.id = $1;

-- name: MarkRunWebhookDeliverySuccess :exec
UPDATE run_webhook_deliveries
SET status = 'success',
    response_status = $2,
    response_body = $3,
    error_message = NULL,
    attempt_count = attempt_count + 1,
    next_retry_at = NULL,
    delivered_at = NOW(),
    updated_at = NOW()
WHERE id = $1;

-- name: MarkRunWebhookDeliveryFailedRetry :exec
UPDATE run_webhook_deliveries
SET status = 'pending',
    response_status = $2,
    response_body = $3,
    error_message = $4,
    attempt_count = attempt_count + 1,
    next_retry_at = $5,
    updated_at = NOW()
WHERE id = $1;

-- name: MarkRunWebhookDeliveryFailedFinal :exec
UPDATE run_webhook_deliveries
SET status = 'failed',
    response_status = $2,
    response_body = $3,
    error_message = $4,
    attempt_count = attempt_count + 1,
    next_retry_at = NULL,
    updated_at = NOW()
WHERE id = $1;

-- name: IncrementRunWebhookSubscriptionFailure :exec
UPDATE run_webhook_subscriptions
SET consecutive_failures = consecutive_failures + 1,
    status = CASE
        WHEN consecutive_failures + 1 >= 3 THEN 'failed'
        ELSE status
    END,
    updated_at = NOW()
WHERE id = $1;

-- name: ResetRunWebhookSubscriptionFailures :exec
UPDATE run_webhook_subscriptions
SET consecutive_failures = 0,
    updated_at = NOW()
WHERE id = $1;

-- name: ListPendingRunWebhookDeliveries :many
SELECT id, subscription_id, run_event_id, payload, status,
       response_status, response_body, error_message,
       attempt_count, next_retry_at, delivered_at, created_at, updated_at
FROM run_webhook_deliveries
WHERE status = 'pending'
  AND next_retry_at IS NOT NULL
  AND next_retry_at <= NOW()
ORDER BY next_retry_at ASC
LIMIT 50;

-- name: GetLatestRunEventForTypes :one
SELECT id, run_id, parent_run_id, sequence, event_type, payload, created_at
FROM run_events
WHERE run_id = $1
  AND event_type = ANY($2::text[])
ORDER BY sequence DESC
LIMIT 1;
