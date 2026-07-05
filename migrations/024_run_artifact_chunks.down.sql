BEGIN;

DROP INDEX IF EXISTS idx_run_artifact_chunks_artifact;
DROP INDEX IF EXISTS idx_run_artifact_chunks_run_source_index;
DROP TABLE IF EXISTS run_artifact_chunks;

DROP INDEX IF EXISTS idx_run_artifacts_run_source;
ALTER TABLE run_artifacts
    DROP COLUMN IF EXISTS source_artifact_id;

COMMIT;
