#!/bin/sh
# init-buckets.sh — Upload CSV data to MinIO raw bucket after stack start

MC="mc"
ALIAS="local"
ENDPOINT="http://minio:9000"
ACCESS_KEY="minioadmin"
SECRET_KEY="minioadmin123"
DATA_DIR="/opt/spark/work-dir/data/raw/csv"
echo "Configuring mc alias..."
$MC alias set $ALIAS $ENDPOINT $ACCESS_KEY $SECRET_KEY

echo "Uploading CSV files to raw bucket..."
for f in $DATA_DIR/*.csv; do
  filename=$(basename "$f")
  $MC cp "$f" "$ALIAS/lakehouse-raw/csv/$filename"
  echo "  Uploaded: $filename"
done

echo "Done. Files in raw bucket:"
$MC ls $ALIAS/lakehouse-raw/csv/
