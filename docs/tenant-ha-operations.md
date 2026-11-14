# Tenant isolation, PostgreSQL HA and recovery operations

SBase uses one PostgreSQL database for application metadata, tenant data, BI/OLAP,
pipeline staging, ingestion checkpoints and Spark input/output. Apache Doris is
not part of the runtime or Compose topology.

## Tenant isolation

- Resolve tenant identity from the authenticated principal and durable job/run row.
- Require every analytical source and destination to be explicitly allow-listed.
- Require a verified `tenant_id` column on every tenant-owned table.
- Push tenant predicates into PostgreSQL SQL before Spark loads data.
- Require idempotency keys to include `tenant_id` and match a physical UNIQUE
  constraint or index.
- Spark writes use run-scoped UNLOGGED staging tables followed by
  `INSERT .. ON CONFLICT DO NOTHING` in PostgreSQL.
- Keep the PostgreSQL row-level security policy as an additional boundary for
  tables that have RLS enabled; application filters are not a substitute for
  database grants and RLS.

See [tenant-boundary-enforcement.md](tenant-boundary-enforcement.md) and
[postgres-tenant-governance.md](postgres-tenant-governance.md).

## Local development topology

The default Compose stack runs one PostgreSQL instance and Spark master/worker.
The `app-ha` profile adds a second SBase application node only; it does not make
the PostgreSQL database highly available.

```bash
docker compose config --quiet
docker compose --profile app-ha config --quiet
docker compose up --build -d
```

Do not delete the existing PostgreSQL volume when upgrading. PostgreSQL is the
only persistent database in the new topology.

## Production PostgreSQL availability

For production, run PostgreSQL on durable storage with a tested HA design:
streaming replication or a managed PostgreSQL service, automatic failover,
connection routing, WAL archiving and point-in-time recovery. The Compose
profile is a development/single-node deployment and must not be treated as a
database HA design.

Recommended controls:

1. Use a dedicated least-privilege application role; keep owner/migration
   credentials separate from runtime credentials.
2. Enable TLS for database connections across hosts.
3. Configure WAL archiving and continuous backups (for example pgBackRest or
   an equivalent managed-service facility).
4. Test point-in-time recovery into an isolated environment at least quarterly.
5. Monitor replication lag, connection saturation, deadlocks, vacuum age,
   bloat, disk headroom and long-running transactions.
6. Use connection pool limits that account for every SBase application node,
   Spark executor and operational client.
7. Exercise failover and connection re-establishment without dropping the
   PostgreSQL data volume.

## Backup and restore

Use `ops/backup-postgres.sh` and `ops/restore-postgres.sh` for the repository's
supported logical backup/restore workflow. A logical backup is not a
point-in-time recovery strategy; production deployments should also archive WAL
and periodically prove a full restore.

## Migrating from an older Doris deployment

The new Compose files no longer start Doris. Existing Docker volumes are not
deleted by this change; preserve them until any required export has been
completed. Do not run `docker compose down --volumes` during the transition.

The migration does not silently import external analytical data. Export each
required table from the old Doris deployment, convert Doris-specific types and
DDL to PostgreSQL types/constraints, load it into a staging schema, verify
row counts and tenant ownership, create UNIQUE constraints, and only then
register the PostgreSQL table in the tenant governance registry. Retain the
original backup until the PostgreSQL copy has passed reconciliation.
