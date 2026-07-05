-- 056_skill_proposals.down.sql

BEGIN;

DROP TRIGGER IF EXISTS skill_proposals_set_updated_at ON skill_proposals;
DROP TABLE IF EXISTS skill_proposals;

COMMIT;
