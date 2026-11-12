# Doris tenant table governance

Doris is a shared analytical engine. Every table containing tenant data must be
registered in PostgreSQL `doris_tenant_table_policies` before governed BI, pipeline
reads/writes, the Doris ingestion connector, or the shared Spark dataset writer can
access it. Runtime access is fail-closed: the table must be allow-listed, approved as
`TENANT_DATA`, marked `VERIFIED`, and physically expose `tenant_id`.

## Approving a table

1. Inspect the full Doris table definition. Confirm `tenant_id` is populated from a
   trusted source and is not user-editable. Backfill legacy rows before approval.
2. Confirm ingestion, CDC, backfill, compaction and repair jobs preserve ownership.
   A nullable or default-to-one-tenant discriminator is not acceptable.
3. Run a complete Doris data-quality scan and record the returned values:

```sql
SELECT COUNT(*) AS verified_row_count,
       SUM(CASE
             WHEN tenant_id IS NULL
               OR tenant_id NOT REGEXP '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
             THEN 1 ELSE 0
           END) AS invalid_tenant_rows
FROM analytics.orders;
```

The scan must report zero invalid rows. Re-run it after every backfill or ingestion
schema change. UUID syntax validation does not prove business ownership: reconcile
tenant IDs with the authoritative tenant inventory and source-system ownership rules.
4. Add the table to `sbase.doris.allowed-tables`.
5. Register it only after review. Replace the sample row count with the measured value:

```sql
INSERT INTO doris_tenant_table_policies
    (schema_name, table_name, tenant_column, enabled, classification,
     approved_by, approved_at, verification_status, verified_row_count,
     invalid_tenant_rows, verified_at)
VALUES
    ('analytics', 'orders', 'tenant_id', TRUE, 'TENANT_DATA',
     'platform-security-review', now(), 'VERIFIED', 125000, 0, now())
ON CONFLICT (schema_name, table_name) DO UPDATE
SET tenant_column=EXCLUDED.tenant_column,
    enabled=TRUE,
    classification='TENANT_DATA',
    approved_by=EXCLUDED.approved_by,
    approved_at=now(),
    verification_status='VERIFIED',
    verified_row_count=EXCLUDED.verified_row_count,
    invalid_tenant_rows=0,
    verified_at=now(),
    updated_at=now();
```

Disable immediately if ownership is uncertain:

```sql
UPDATE doris_tenant_table_policies
   SET enabled=FALSE, verification_status='REJECTED', updated_at=now()
 WHERE schema_name='analytics' AND table_name='orders';
```

Policy changes are written to `doris_tenant_policy_audit`. Re-approval requires a new
data-quality scan and a new verification timestamp. Existing tables are intentionally
not auto-approved by Flyway.

## Doris privileges

The registry is a runtime application guard, not a replacement for Doris account
permissions. Production deployments must use a restricted Doris service account:
no DDL, no user-controlled SQL, no direct BI access to raw schemas, and only the
specific SELECT/INSERT privileges required by SBase. Review grants, views, materialized
views, routine-load jobs and external catalogs separately. Spark JDBC writers and
the Doris ingestion connector use the configured service credentials.

Platform-internal metadata and query-audit tables must never be registered as
`TENANT_DATA`. Their endpoints remain platform-admin-only and expose only cluster
operational metadata, not other tenants' row contents.

UUID format checks and operator attestation cannot independently prove that every
source record was assigned the correct business tenant. External source ownership,
Kafka ACLs, external JDBC permissions and object-store IAM must be verified separately.
