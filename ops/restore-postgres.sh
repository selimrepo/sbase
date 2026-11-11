#!/usr/bin/env bash
set -Eeuo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 path/to/sbase-YYYYMMDDTHHMMSSZ.dump" >&2
  exit 2
fi
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD must be set}"
BACKUP="$1"
[[ -s "$BACKUP" ]] || { echo "Backup file not found or empty: $BACKUP" >&2; exit 1; }
if [[ -f "$BACKUP.sha256" ]]; then
  (cd "$(dirname "$BACKUP")" && sha256sum --check "$(basename "$BACKUP").sha256")
fi
if [[ "${CONFIRM_DESTRUCTIVE_RESTORE:-}" != "YES" ]]; then
  echo "This replaces objects in the sbase database. Set CONFIRM_DESTRUCTIVE_RESTORE=YES to continue." >&2
  exit 1
fi

docker compose exec -T postgres sh -ec '
  export PGPASSWORD="$POSTGRES_PASSWORD"
  dropdb --if-exists --username=sbase --force sbase
  createdb --username=sbase --owner=sbase sbase
'
docker compose exec -T postgres sh -ec '
  export PGPASSWORD="$POSTGRES_PASSWORD"
  exec pg_restore --exit-on-error --no-owner --username=sbase --dbname=sbase
' < "$BACKUP"
echo "Restore completed. Validate application migrations and perform a smoke test."
