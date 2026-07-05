-- 006_skill_benchmark.down.sql

BEGIN;

DROP TABLE IF EXISTS agent_skill_scores;
DROP TABLE IF EXISTS agent_skill_benchmark_runs;
DROP TABLE IF EXISTS skill_test_cases;

COMMIT;
