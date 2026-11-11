ALTER TABLE feature_sets ADD COLUMN IF NOT EXISTS owner_subject VARCHAR(200);
CREATE INDEX IF NOT EXISTS idx_feature_sets_owner ON feature_sets(owner_subject,updated_at DESC);

ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS created_by VARCHAR(200);

ALTER TABLE bi_report_schedules
    ADD COLUMN IF NOT EXISTS lease_owner VARCHAR(80),
    ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_bi_report_schedules_lease_due
    ON bi_report_schedules(enabled,next_run_at,lease_until);

INSERT INTO sbase_role_permissions(role_id,permission)
SELECT r.id,p.permission FROM sbase_roles r CROSS JOIN (VALUES
    ('pipelines:read'),('pipelines:write'),('bi:write'),('features:read'),('features:write')
) p(permission)
WHERE r.name='ADMIN' ON CONFLICT DO NOTHING;
