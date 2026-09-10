-- 0007_autoscaling.down.sql
DROP TABLE IF EXISTS scaling_policies;

ALTER TABLE deployments
    DROP COLUMN IF EXISTS desired_replicas;

ALTER TABLE projects
    DROP COLUMN IF EXISTS max_replicas,
    DROP COLUMN IF EXISTS min_replicas,
    DROP COLUMN IF EXISTS desired_replicas;
