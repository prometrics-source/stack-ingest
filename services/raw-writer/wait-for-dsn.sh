#!/usr/bin/env sh
set -e

DSN_FILE="${PG_DSN_FILE:-/secrets/pg_dsn}"
TRIES=30
SLEEP_SECS=2

echo "[wait-for-dsn] waiting for DSN file at ${DSN_FILE}"

i=0
while [ $i -lt $TRIES ]; do
  if [ -s "$DSN_FILE" ]; then
    echo "[wait-for-dsn] DSN file is present and non-empty"
    exec python /app/ingestor.py
  fi
  i=$((i+1))
  echo "[wait-for-dsn] DSN not ready yet (attempt $i/${TRIES}), retrying in ${SLEEP_SECS}s"
  sleep $SLEEP_SECS
done

echo "[wait-for-dsn] DSN file not found or empty after ${TRIES} attempts; exiting"
exit 1
