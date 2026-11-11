#!/usr/bin/env bash
set -Eeuo pipefail
: "${DORIS_HOST:=doris-fe}"
: "${DORIS_PORT:=9030}"
: "${DORIS_USER:?Set DORIS_USER}"
: "${DORIS_PASSWORD:?Set DORIS_PASSWORD}"
: "${DORIS_S3_ENDPOINT:?Set DORIS_S3_ENDPOINT}"
: "${DORIS_S3_BUCKET:?Set DORIS_S3_BUCKET}"
: "${DORIS_S3_ACCESS_KEY:?Set DORIS_S3_ACCESS_KEY}"
: "${DORIS_S3_SECRET_KEY:?Set DORIS_S3_SECRET_KEY}"
: "${DORIS_S3_REGION:=us-east-1}"
: "${DORIS_DATABASE:=analytics}"
: "${SNAPSHOT_LABEL:?Set SNAPSHOT_LABEL to a completed backup}"
: "${RESTORE_TABLES:?Set comma-separated RESTORE_TABLES}"
[[ "${SNAPSHOT_LABEL}" =~ ^[A-Za-z0-9_]+$ && "${DORIS_DATABASE}" =~ ^[A-Za-z0-9_]+$ ]] || exit 2
[[ "${CONFIRM_DORIS_RESTORE:-}" == "YES" ]] || { echo "Destructive restore; set CONFIRM_DORIS_RESTORE=YES" >&2; exit 1; }
TABLE_SQL=""
IFS=',' read -ra TABLES <<< "$RESTORE_TABLES"
for table in "${TABLES[@]}"; do
  [[ "$table" =~ ^[A-Za-z0-9_]+$ ]] || { echo "Invalid table name" >&2; exit 2; }
  TABLE_SQL+="$table,"
done
TABLE_SQL="${TABLE_SQL%,}"
mysql --protocol=TCP -h "$DORIS_HOST" -P "$DORIS_PORT" -u"$DORIS_USER" -p"$DORIS_PASSWORD" <<SQL
CREATE REPOSITORY IF NOT EXISTS sbase_s3_repo
WITH S3 ON LOCATION "s3://$DORIS_S3_BUCKET/doris-backups"
PROPERTIES (
  "s3.endpoint"="$DORIS_S3_ENDPOINT",
  "s3.region"="$DORIS_S3_REGION",
  "s3.access_key"="$DORIS_S3_ACCESS_KEY",
  "s3.secret_key"="$DORIS_S3_SECRET_KEY",
  "use_path_style"="true"
);
RESTORE SNAPSHOT $DORIS_DATABASE.$SNAPSHOT_LABEL
FROM sbase_s3_repo ON ($TABLE_SQL)
PROPERTIES ("backup_timestamp"="","replication_num"="3","timeout"="3600");
SQL
echo "Restore submitted. Verify SHOW RESTORE FROM $DORIS_DATABASE and validate row counts/checksums."
