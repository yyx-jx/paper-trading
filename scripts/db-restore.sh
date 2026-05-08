#!/usr/bin/env bash
set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL is required}"
: "${BACKUP_FILE:?BACKUP_FILE is required}"

pg_restore --clean --if-exists --dbname "$DATABASE_URL" "$BACKUP_FILE"
echo "Restore completed from: $BACKUP_FILE"
