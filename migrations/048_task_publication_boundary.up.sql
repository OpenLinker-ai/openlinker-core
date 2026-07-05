ALTER TABLE task_queries
    ADD COLUMN visibility TEXT NOT NULL DEFAULT 'private',
    ADD COLUMN public_summary TEXT,
    ADD COLUMN published_at TIMESTAMPTZ;

ALTER TABLE task_queries
    ADD CONSTRAINT task_queries_visibility_valid CHECK (
        visibility IN ('private', 'public')
    ),
    ADD CONSTRAINT task_queries_public_summary_len CHECK (
        public_summary IS NULL OR char_length(public_summary) BETWEEN 4 AND 240
    ),
    ADD CONSTRAINT task_queries_publication_consistency CHECK (
        (
            visibility = 'private'
            AND published_at IS NULL
        )
        OR (
            visibility = 'public'
            AND published_at IS NOT NULL
            AND public_summary IS NOT NULL
        )
    );

CREATE INDEX idx_task_queries_public_board
    ON task_queries (published_at DESC, created_at DESC)
    WHERE visibility = 'public';
