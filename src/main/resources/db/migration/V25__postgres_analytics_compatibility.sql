-- PostgreSQL is now the sole analytical and operational database.
-- Keep the original registry tables as the physical source of truth for existing
-- installations; simple updatable views provide the new neutral names.
CREATE OR REPLACE VIEW postgres_tenant_table_policies AS
    SELECT * FROM doris_tenant_table_policies;

CREATE OR REPLACE VIEW postgres_tenant_policy_audit AS
    SELECT * FROM doris_tenant_policy_audit;

-- Existing catalog entries that pointed at Doris now refer to the same physical
-- PostgreSQL catalog namespace. This is metadata-only; it does not drop tables.
UPDATE datasets
   SET source_type='POSTGRESQL'
 WHERE UPPER(source_type)='DORIS';
