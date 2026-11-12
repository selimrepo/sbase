# Doris tenant table governance

Doris is a shared analytical engine. Every table that can contain tenant data must be
explicitly registered in PostgreSQL table `doris_tenant_table_policies` before the
application will query or write it through governed BI or the shared Spark dataset
writer. Registration alone is insufficient: the runtime also checks that the actual
Doris table exposes a `tenant_id` column and injects/validates the authenticated
tenant discriminator.

## Approving a table

1. Inspect the full Doris table definition and confirm every row has a correct,
   immutable `tenant_id` value. Backfill legacy rows before enabling access.
2. Confirm all ingestion, CDC, backfill, compaction and repair jobs preserve that
   column. A nullable or default-to-one-tenant column is not an acceptable control.
3. Add the table to `sbase.doris.allowed-tables`.
4. Register it only after review:

```sql
INSERT INTO doris_tenant_table_policies
    (schema_name, table_name, tenant_column, enabled, classification, approved_by, approved_at)
VALUES
    ('analytics', 'orders', 'tenant_id', TRUE, 'TENANT_DATA', 'platform-security-review', now())
ON CONFLICT (schema_name, table_name) DO UPDATE
SET tenant_column=EXCLUDED.tenant_column, enabled=TRUE,
    classification=EXCLUDED.classification, approved_by=EXCLUDED.approved_by,
    approved_at=now(), updated_at=now();
```

Disable immediately if ownership is uncertain:

```sql
UPDATE doris_tenant_table_policies SET enabled=FALSE, updated_at=now()
WHERE schema_name='analytics' AND table_name='orders';
```

Platform-internal metadata and query-audit tables must never be registered as tenant
data tables. Their endpoints remain platform-admin-only and must return aggregates
without exposing another tenant's row contents.

The registry is a runtime fail-closed guard, not a replacement for Doris account
permissions. Production deployments must use a restricted Doris service account:
no DDL, no user-controlled SQL, no direct BI access to raw schemas, and only the
specific SELECT/INSERT privileges required by SBase. Review grants, views, materialized
views, routine-load jobs and external catalogs separately. Spark JDBC writers must use
the same restricted credentials.

This migration intentionally does not auto-approve existing tables. Existing tables
must be reviewed and registered individually; this prevents a legacy table without
complete tenant ownership from silently becoming shared.
