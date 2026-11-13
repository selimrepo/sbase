# Tenant boundary enforcement and connector contracts

## Doris
All tenant-data tables are fail-closed. A table must be in `SBASE_DORIS_ALLOWED_TABLES`,
registered and approved in `doris_tenant_table_policies`, and physically expose a
`tenant_id` column. Legacy SQL and governed BI both call the same policy service;
the authenticated tenant is injected by the query boundary, not accepted from SQL.
The shared Spark writer rejects foreign/null tenant rows. Pipeline reads filter by the
persisted pipeline-run tenant, and pipeline reads/writes also require an active table
policy and physical tenant column.

Do not register platform metadata, information-schema, audit, or public reference tables
as tenant data. Existing tables are intentionally not auto-approved by migration.
Follow `docs/doris-tenant-governance.md` to inspect, backfill and approve each table.
Use a restricted Doris account with only required SELECT/INSERT grants; the policy
registry is not a substitute for database credentials and grants.

## Connector data contracts
Every durable ingestion record must carry a top-level `tenant_id` equal to the owning
connector's persisted tenant. `DurableRecordSink` derives the owner from PostgreSQL
and rejects missing or foreign tenant IDs before inserting records or Kafka batches.
The V23 composite foreign keys also make it impossible to associate a run, record,
checkpoint, dead letter, Kafka partition lease or batch receipt with a connector from
another tenant.

- **File:** files are read only below
  `SBASE_INGESTION_FILE_ROOT/tenants/<tenant-uuid>/`. Caller-supplied `allowedRoot`
  is ignored. CSV and JSONL records must include a matching `tenant_id`.
- **REST:** every returned record must include a matching `tenant_id`; otherwise the
  durable sink rejects the page. The upstream API credential must itself be tenant-scoped.
- **JDBC:** the source query must return `tenant_id`; each row is checked against the
  persisted run tenant before it is written. Target tables must accept the tenant column.
  External database grants and row-level policies remain the operator's responsibility.
- **Doris connector:** in addition to JDBC row checks, the destination table must pass
  the Doris tenant table registry and physical schema checks.
- **Kafka and Debezium:** every message must carry the trusted `sbase-tenant-id` header
  (or the configured `tenantHeader`) matching the connector tenant. JSON payloads are
  recursively checked for conflicting `tenant_id` values. Binary/Avro payloads rely on
  the trusted header; therefore Kafka ACLs must restrict who can publish to these topics
  and who can set that header. Do not trust arbitrary producers to self-assert it.

The dispatcher claims only runs whose connector tenant matches the run tenant and
revalidates the relationship immediately before connector execution. Dead-letter replay
remains tenant-scoped and creates a new tenant-owned run.

## Database integrity
V23 performs a preflight check and aborts if existing ingestion or pipeline records
already contain cross-tenant references. It then adds composite foreign keys that bind
child records to the same tenant as their parent. It also makes online feature keys
tenant-local by including `tenant_id` in the primary key and upsert conflict target.

## Operations and assurance
1. Backfill and review every Doris table before enabling it in the registry.
2. Provision tenant-specific file directories and do not allow untrusted users to write
   symlinks into the ingestion root.
3. Configure Kafka ACLs for trusted producers and topic-level tenant routing.
4. Use separate least-privilege credentials for each external JDBC source/target where
   supported; avoid shared superuser accounts.
5. Run the cross-tenant negative integration suite after every connector or schema change.
6. Treat any missing tenant discriminator, ownership mismatch, unregistered Doris table,
   or failed policy lookup as a hard denial.

These controls harden the application paths and PostgreSQL relationships. They do not
prove isolation inside arbitrary external JDBC systems, Kafka brokers, object-store
administrative APIs, or Doris users with unrestricted direct SQL access. Those boundaries
must be secured with provider-side IAM/ACLs and verified in the deployment environment.
