#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

# Run from the repository root. The output directory should be on durable,
# encrypted storage and copied off-host by the deployment's backup agent.
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD must be set}"
BACKUP_DIR="${SBASE_BACKUP_DIR:-./backups/postgres}"
RETENTION_DAYS="${SBASE_BACKUP_RETENTION_DAYS:-14}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$BACKUP_DIR"
chmod 700 "$BACKUP_DIR"
FILE="$BACKUP_DIR/sbase-$STAMP.dump"
TMP="$FILE.partial"

cleanup() { rm -f "$TMP"; }
trap cleanup EXIT

docker compose exec -T postgres sh -ec '
  export PGPASSWORD="$POSTGRES_PASSWORD"
  exec pg_dump --format=custom --no-owner --username=sbase --dbname=sbase
' > "$TMP"
test -s "$TMP"
mv "$TMP" "$FILE"
sha256sum "$FILE" > "$FILE.sha256"
find "$BACKUP_DIR" -type f -name 'sbase-*.dump' -mtime +"$RETENTION_DAYS" -delete
find "$BACKUP_DIR" -type f -name 'sbase-*.dump.sha256' -mtime +"$RETENTION_DAYS" -delete
echo "PostgreSQL backup created: $FILE"
echo "Copy this backup and its checksum to off-host encrypted storage."
