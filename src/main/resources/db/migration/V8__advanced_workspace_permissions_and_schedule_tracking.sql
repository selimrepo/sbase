ALTER TABLE schedule_runs ADD COLUMN IF NOT EXISTS outcome_recorded_at TIMESTAMPTZ;
INSERT INTO sbase_role_permissions(role_id,permission)
SELECT r.id,p.permission FROM sbase_roles r CROSS JOIN (VALUES ('pipelines:read'),('pipelines:write'),('bi:write')) p(permission)
WHERE r.name='ADMIN' ON CONFLICT DO NOTHING;
INSERT INTO sbase_role_permissions(role_id,permission)
SELECT r.id,p.permission FROM sbase_roles r CROSS JOIN (VALUES ('pipelines:read'),('pipelines:write')) p(permission)
WHERE r.name='DATA_ENGINEER' ON CONFLICT DO NOTHING;
