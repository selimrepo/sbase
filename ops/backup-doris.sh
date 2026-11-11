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
: "${DORIS_TABLES:?Set comma-separated DORIS_TABLES}"
: "${SNAPSHOT_LABEL:=sbase_$(date -u +%Y%m%dT%H%M%SZ)}"
[[ "$SNAPSHOT_LABEL" =~ ^[A-Za-z0-9_]+$ ]] || { echo "Invalid snapshot label" >&2; exit 2; }
[[ "$DORIS_DATABASE" =~ ^[A-Za-z0-9_]+$ ]] || { echo "Invalid database" >&2; exit 2; }
TABLE_SQL=""
IFS=',' read -ra TABLES <<< "$DORIS_TABLES"
for table in "${TABLES[@]}"; do
  [[ "$table" =~ ^[A-Za-z0-9_]+$ ]] || { echo "Invalid table name: $table" >&2; exit 2; }
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
BACKUP SNAPSHOT $DORIS_DATABASE.$SNAPSHOT_LABEL
TO sbase_s3_repo ON ($TABLE_SQL)
PROPERTIES ("type"="full","timeout"="3600");
SQL
echo "Backup submitted: $DORIS_DATABASE.$SNAPSHOT_LABEL"
echo "Check SHOW BACKUP FROM $DORIS_DATABASE and verify the snapshot status before declaring recovery point valid."
