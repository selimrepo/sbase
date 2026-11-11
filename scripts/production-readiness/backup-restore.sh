#!/usr/bin/env bash
set -Eeuo pipefail
: "\${PGHOST:?Set PGHOST}"; : "\${PGUSER:?Set PGUSER}"; : "\${PGDATABASE:?Set PGDATABASE}"; : "\${PGPASSWORD:?Set PGPASSWORD}"
command -v pg_dump >/dev/null; command -v pg_restore >/dev/null; command -v psql >/dev/null
scratch="sbase_pr_restore_$(date +%s)_$$"; dump_file="$(mktemp -t sbase-pr-restore.XXXXXX.dump)"
cleanup() { psql -X -v ON_ERROR_STOP=1 -d postgres -c "DROP DATABASE IF EXISTS \\"$scratch\\"" >/dev/null 2>&1 || true; rm -f "$dump_file"; }
trap cleanup EXIT

echo "Dumping $PGDATABASE to custom-format archive..."
pg_dump --format=custom --no-owner --no-privileges --file "$dump_file" "$PGDATABASE"
pg_restore --list "$dump_file" >/dev/null
psql -X -v ON_ERROR_STOP=1 -d postgres -c "CREATE DATABASE \\"$scratch\\""
pg_restore --exit-on-error --no-owner --no-privileges --dbname "$scratch" "$dump_file"
source_tables="$(psql -X -At -v ON_ERROR_STOP=1 -d "$PGDATABASE" -c "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema') AND table_type='BASE TABLE'")"
restored_tables="$(psql -X -At -v ON_ERROR_STOP=1 -d "$scratch" -c "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema') AND table_type='BASE TABLE'")"
[[ "$source_tables" -eq "$restored_tables" ]] || { echo "FAIL table count mismatch: source=$source_tables restored=$restored_tables"; exit 1; }
psql -X -v ON_ERROR_STOP=1 -d "$scratch" -c "SELECT 1" >/dev/null
echo "PASS archive readable; restore completed; base-table count matches ($source_tables)"
echo "NOTE Also test PITR, WAL archiving, encryption, off-host retention, checksums and RPO/RTO."
