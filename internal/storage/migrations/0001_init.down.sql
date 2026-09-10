-- 0001_init.down.sql
-- Revert core Nebula schema.

DROP TABLE IF EXISTS instances CASCADE;
DROP TABLE IF EXISTS deployments CASCADE;
DROP TABLE IF EXISTS workers CASCADE;
DROP TABLE IF EXISTS projects CASCADE;
