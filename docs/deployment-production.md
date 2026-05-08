# Production Deployment

This deployment target is a single-machine Docker Compose stack behind Caddy.
Only Caddy publishes public ports. PostgreSQL, Redis, matching-service, and
app-server stay on the Docker network.

## Required Environment

Create a production env file and pass it with `APP_ENV_FILE=.env.production`.

Required values:

```text
PUBLIC_DOMAIN=trade.example.com
CORS_ORIGINS=https://trade.example.com
JWT_SECRET=<strong non-default secret>
EXPORT_ANONYMIZATION_SECRET=<dedicated export anonymization secret>
POSTGRES_PASSWORD=<strong database password>
SERVER_REQUIRE_MIGRATIONS=true
SERVER_ALLOW_DEV_SCHEMA_BOOTSTRAP=false
EXPECTED_SCHEMA_MIGRATION_ID=000004
NODE_ENV=production
```

## Upgrade Flow

1. Stop new trading writes or enter a maintenance window.
2. Run `npm run db:backup`.
3. Run `npm run db:migrate`.
4. Run `npm run db:status` and confirm `000004` is applied.
5. Build and start: `APP_ENV_FILE=.env.production docker compose -f docker-compose.deploy.yml up -d --build`.
6. Check readiness: `curl -fsS https://$PUBLIC_DOMAIN/api/health/ready`.
7. Verify login, market data, order placement, logs, and export.

## Migration Smoke Test

Before touching production, run the migration smoke test against a disposable
PostgreSQL database. The script resets the target schema, so never point it at
production or at `DATABASE_URL`. `pg_dump` and `pg_restore` must be available
on `PATH` because the smoke test verifies backup recovery.

```bash
MIGRATION_SMOKE_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/btc_paper_smoke npm run test:migrations
```

For a separate restore target, provide:

```bash
MIGRATION_SMOKE_RESTORE_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/btc_paper_smoke_restore
```

The smoke test covers fresh migration, old schema upgrade to `000004`,
schema-version fail-fast behavior, and `pg_dump`/`pg_restore` recovery.

## Readiness And Fault Gates

Run these checks before a production rollout:

```bash
npm run test:deployment
npm run test:deployment-readiness
npm run test:fault-tolerance
npm run test:metrics
```

They verify production fail-fast config, Docker/Caddy exposure boundaries,
health/ready fields, metrics observability, schema guard behavior, and strict
PG write blocking. These checks do not connect to the production database.

## JSONL Logs

App and matching JSONL audit backups are written under the Docker `logs` volume
at `/app/data/logs`. Files rotate by date and size, for example:

```text
audit-events-2026-05-07.jsonl
audit-events-2026-05-07-0001.jsonl
behavior-action-logs-2026-05-07.jsonl
matching-events-2026-05-07.jsonl
matching-snapshots-2026-05-07.jsonl
```

PostgreSQL remains the authoritative store. JSONL is an append-only backup and
audit trail. Monitor `jsonl_queue_depth`, `jsonl_backlog_state`,
`jsonl_dropped_records_total`, and `jsonl_write_failures_total`; any dropped
record means the service protected the event loop under pressure and needs
operator review.

## Production Electron Client

Build the production C/S client only after the HTTPS/WSS server is ready:

```bash
VITE_API_BASE_URL=https://$PUBLIC_DOMAIN npm run package:win:prod
```

The production client is a UI terminal only. It does not start a local memory
backend by default. The Windows test installer remains separate and can still
start the embedded memory backend:

```bash
npm run package:win:test
```

Use `ELECTRON_EMBED_BACKEND=true` only for local diagnostics or test packages.
Do not enable it for production users.

## Rollback And Restore

The rollback source of truth is PostgreSQL backup restore:

```bash
BACKUP_FILE=backups/<backup>.dump npm run db:restore
```

After restore, restart the stack and verify `/api/health/ready`. Redis can be
recreated from PostgreSQL-backed state and does not need to be restored for a
normal rollback.

## Public Ports

Expected public exposure:

```text
80/tcp  -> Caddy HTTP challenge/redirect
443/tcp -> Caddy HTTPS/WSS
```

These must not be public:

```text
8787 app-server
8788 matching-service
5432 PostgreSQL
6379 Redis
```
