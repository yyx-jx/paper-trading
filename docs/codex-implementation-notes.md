# Codex Implementation Notes

Updated: 2026-05-07

## Completed

- Fixed the current TypeScript blockers in `apps/server/src/index.ts`: duplicate shutdown state, missing WS connection metrics, and unsafe P95 latency calculation.
- Updated config checks to the current expected migration id `000004`.
- Added CSV-only bulk user endpoints:
  - `GET /api/users/bulk/template.csv`
  - `POST /api/users/bulk/csv/preview`
  - `POST /api/users/bulk/csv`
- Added CSV parser support for the documented fields, including `managerUsername`, `permissionLevel`, `mustChangePassword`, template description-row skipping, and a 100-row limit.
- Added customer dataset export endpoints:
  - `POST /api/datasets/export/preview`
  - `POST /api/datasets/export`
- Customer dataset exports produce a ZIP with `manifest.json`, `schema.json`, `customer_dataset.csv`, `customer_dataset.jsonl`, and `export_audit.json`; Parquet requests return 501.
- Added stable anonymization for customer dataset user/order/trace identifiers and DB export audit recording.
- Added WS ticket support through `POST /api/ws/tickets`; the client now prefers one-time short-lived tickets and falls back to the legacy token URL for compatibility.
- Added WS heartbeat cleanup and metrics connection counts.
- Added an initial schema migration for fresh production databases before the existing expand migrations.
- Hardened Docker deployment with Caddy, internal-only app/matching/PG/Redis services, persistent volumes, and no `COPY data ./data` in the image.
- Added `docs/deployment-production.md`.
- Converted matching-service JSONL writes away from synchronous `appendFileSync`.

## Added Tests

- `npm run test:csv-bulk-users`
- `npm run test:dataset-export`
- `npm run test:deployment`

## Verification

Passed:

```text
npm run typecheck -- --pretty false
npm run test:config
npm run test:migrations
npm run test:permissions
npm run test:bulk-users
npm run test:csv-bulk-users
npm run test:logs
npm run test:export
npm run test:dataset-export
npm run test:deployment
npm run test:trading
npm run test:regression
npm run build
```

Not run against the current database to avoid mutating live data from this coding pass:

```text
npm run db:backup
npm run db:migrate
npm run db:status
```

Run those during the production maintenance window in the documented order.
