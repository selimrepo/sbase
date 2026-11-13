-- Explicit fail-closed registry for every Doris table exposed to tenant data paths.
CREATE TABLE IF NOT EXISTS doris_tenant_table_policies (
    schema_name VARCHAR(128) NOT NULL,
    table_name VARCHAR(128) NOT NULL,
    tenant_column VARCHAR(64) NOT NULL DEFAULT 'tenant_id',
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    classification VARCHAR(24) NOT NULL DEFAULT 'TENANT_DATA'
        CHECK (classification IN ('TENANT_DATA','PLATFORM_INTERNAL','PUBLIC_REFERENCE')),
    approved_by VARCHAR(200),
    approved_at TIMESTAMPTZ,
    verification_status VARCHAR(16) NOT NULL DEFAULT 'PENDING'
        CHECK (verification_status IN ('PENDING','VERIFIED','REJECTED')),
    verified_row_count BIGINT,
    invalid_tenant_rows BIGINT,
    verification_hash VARCHAR(128),
    verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(schema_name,table_name),
    CHECK (tenant_column ~ '^[A-Za-z_][A-Za-z0-9_]*$'),
    CHECK ((enabled = FALSE) OR (
        approved_by IS NOT NULL AND approved_at IS NOT NULL
        AND verification_status='VERIFIED' AND verified_at IS NOT NULL
        AND invalid_tenant_rows IS NOT NULL AND invalid_tenant_rows=0 AND verified_row_count IS NOT NULL AND verified_row_count>=0
    ))
);
CREATE INDEX IF NOT EXISTS idx_doris_tenant_table_policy_enabled
    ON doris_tenant_table_policies(enabled,schema_name,table_name);

CREATE TABLE IF NOT EXISTS doris_tenant_policy_audit (
    id BIGSERIAL PRIMARY KEY,
    schema_name VARCHAR(128) NOT NULL,
    table_name VARCHAR(128) NOT NULL,
    tenant_id UUID,
    subject VARCHAR(200) NOT NULL,
    action VARCHAR(40) NOT NULL,
    outcome VARCHAR(20) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    details JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_doris_tenant_policy_audit_tenant_time
    ON doris_tenant_policy_audit(tenant_id,occurred_at DESC);

-- Intentionally do not auto-enable existing tables: an operator must verify
-- physical tenant_id coverage and approve each table before tenant queries/writes.

CREATE OR REPLACE FUNCTION sbase_audit_doris_tenant_policy_change() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    target_schema VARCHAR(128);
    target_table VARCHAR(128);
    target_enabled BOOLEAN;
    target_tenant_column VARCHAR(64);
    target_classification VARCHAR(24);
    target_verification_status VARCHAR(16);
    target_verified_row_count BIGINT;
    target_invalid_tenant_rows BIGINT;
BEGIN
    IF TG_OP='DELETE' THEN
        target_schema:=OLD.schema_name;
        target_table:=OLD.table_name;
        target_enabled:=FALSE;
        target_tenant_column:=OLD.tenant_column;
        target_classification:=OLD.classification;
        target_verification_status:=OLD.verification_status;
        target_verified_row_count:=OLD.verified_row_count;
        target_invalid_tenant_rows:=OLD.invalid_tenant_rows;
    ELSE
        target_schema:=NEW.schema_name;
        target_table:=NEW.table_name;
        target_enabled:=NEW.enabled;
        target_tenant_column:=NEW.tenant_column;
        target_classification:=NEW.classification;
        target_verification_status:=NEW.verification_status;
        target_verified_row_count:=NEW.verified_row_count;
        target_invalid_tenant_rows:=NEW.invalid_tenant_rows;
    END IF;
    INSERT INTO doris_tenant_policy_audit(schema_name,table_name,subject,action,outcome,details)
    VALUES(target_schema,target_table,current_user,TG_OP,
        CASE WHEN target_enabled THEN 'ENABLED' ELSE 'DISABLED' END,
        jsonb_build_object('tenant_column',target_tenant_column,'classification',target_classification,
                           'verification_status',target_verification_status,
                           'verified_row_count',target_verified_row_count,
                           'invalid_tenant_rows',target_invalid_tenant_rows));
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_doris_tenant_policy_audit ON doris_tenant_table_policies;
CREATE TRIGGER trg_doris_tenant_policy_audit
AFTER INSERT OR UPDATE OR DELETE ON doris_tenant_table_policies
FOR EACH ROW EXECUTE FUNCTION sbase_audit_doris_tenant_policy_change();
