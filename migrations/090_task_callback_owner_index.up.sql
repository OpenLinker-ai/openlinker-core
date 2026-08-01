CREATE INDEX CONCURRENTLY idx_task_callback_subscriptions_owner
    ON public.task_callback_subscriptions
    (owner_user_id, updated_at DESC, created_at DESC)
    WHERE status <> 'deleted';
