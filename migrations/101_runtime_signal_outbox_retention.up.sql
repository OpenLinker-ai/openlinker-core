CREATE INDEX CONCURRENTLY idx_runtime_signal_outbox_published_retention
    ON public.runtime_signal_outbox
    (published_at, id)
    WHERE status = 'published';
