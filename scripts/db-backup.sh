#!/usr/bin/env bash
set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL is required}"

mkdir -p backups
stamp="$(date +%Y%m%d_%H%M%S)"
out="backups/btc_paper_trading_${stamp}.dump"

pg_dump -Fc "$DATABASE_URL" > "$out"
sha256sum "$out" > "$out.sha256"
echo "Backup created: $out"
