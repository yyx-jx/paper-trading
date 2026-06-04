# Production Deployment

This target is a temporary HTTP rollout for external desktop clients. Users receive only the Electron client; they do not need a browser URL. The client talks to the server through:

```text
API: http://<PRODUCTION_HOST>:10001
WS:  ws://<PRODUCTION_HOST>:10001
```

Only Caddy publishes the public business port. PostgreSQL, Redis, matching-service, and app-server stay on the Docker network.

## Required Environment

Create `.env.production` from `.env.production.example` and pass it with `APP_ENV_FILE=.env.production`.

For a fully isolated test stack on the same host, create `.env.green` from `.env.green.example`, use a separate compose project such as `app-green`, and expose it on `PUBLIC_PORT=10002`.

Required values:

```text
PUBLIC_DOMAIN=<PRODUCTION_HOST>
PUBLIC_BIND_HOST=0.0.0.0
PUBLIC_PORT=10001
PUBLIC_BASE_URL=http://<PRODUCTION_HOST>:10001
CORS_ORIGINS=http://<PRODUCTION_HOST>:10001
JWT_SECRET=<strong non-default secret>
EXPORT_ANONYMIZATION_SECRET=<dedicated export anonymization secret>
POSTGRES_PASSWORD=<strong database password>
SERVER_STRICT_PERSISTENCE=true
SERVER_REQUIRE_MIGRATIONS=true
SERVER_ALLOW_DEV_SCHEMA_BOOTSTRAP=false
EXPECTED_SCHEMA_MIGRATION_ID=000008
SEED_DEFAULT_USERS=false
NODE_ENV=production
DEPLOY_ENV=production
APP_SERVER_IMAGE=p-t-app-server:latest
MATCHING_SERVICE_IMAGE=p-t-matching-service:latest
```

## Public Ports

Expected public exposure:

```text
22/tcp    -> SSH management
10001/tcp -> Caddy HTTP API and WebSocket reverse proxy
```

These must not be public:

```text
8787 app-server
8788 matching-service
5432 PostgreSQL
6379 Redis
9090 Prometheus
3000 Grafana
```

## First Deploy Flow

```bash
cd /srv/p-t/app
cp .env.production.example .env.production
# Fill POSTGRES_PASSWORD, JWT_SECRET, EXPORT_ANONYMIZATION_SECRET before continuing.

APP_ENV_FILE=.env.production docker compose -f docker-compose.deploy.yml --env-file .env.production config
APP_ENV_FILE=.env.production docker compose -f docker-compose.deploy.yml --env-file .env.production up -d postgres redis
APP_ENV_FILE=.env.production docker compose -f docker-compose.deploy.yml --env-file .env.production run --rm app-server npm run db:migrate
APP_ENV_FILE=.env.production docker compose -f docker-compose.deploy.yml --env-file .env.production run --rm app-server npm run db:status
APP_ENV_FILE=.env.production docker compose -f docker-compose.deploy.yml --env-file .env.production run --rm app-server npx tsx scripts/create-admin.ts
APP_ENV_FILE=.env.production docker compose -f docker-compose.deploy.yml --env-file .env.production up -d --build
```

## Green Test Stack

Start a fully isolated green stack with a separate compose project, env file, database, Redis, and public port:

```bash
cd /srv/p-t/green
cp .env.green.example .env.green
APP_ENV_FILE=.env.green docker compose -p app-green -f docker-compose.deploy.yml --env-file .env.green build app-server matching-service
APP_ENV_FILE=.env.green docker compose -p app-green -f docker-compose.deploy.yml --env-file .env.green up -d
APP_ENV_FILE=.env.green docker compose -p app-green -f docker-compose.deploy.yml --env-file .env.green run --rm app-server npm run db:migrate
```

The green stack should use `PUBLIC_PORT=10002`, its own PostgreSQL credentials, its own Redis data, and `HYPER_BRIDGE_ENABLED=false`.

## HTTP Production Client

Build the temporary HTTP production client only after accepting the plaintext transport risk:

```bash
ALLOW_INSECURE_PROD_HTTP=true VITE_API_BASE_URL=http://103.147.13.98:10001 npm run package:win:prod
```

The production client does not start a local backend. WebSocket URLs are derived from the API URL and use `ws://` for this temporary HTTP origin.

## Migration Smoke Test

Before touching production, run the migration smoke test against a disposable PostgreSQL database. The script resets the target schema, so never point it at production or at `DATABASE_URL`.

```bash
npm run test:migration-safety
MIGRATION_SMOKE_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/btc_paper_smoke npm run test:migrations
```

## Readiness And Fault Gates

Run these checks before rollout:

```bash
npm run test:deployment
npm run test:deployment-readiness
npm run test:fault-tolerance
npm run test:metrics
```

They verify production fail-fast config, Docker/Caddy exposure boundaries, health/ready fields, metrics observability, schema guard behavior, and strict PostgreSQL write blocking. These checks do not connect to the production database.

## Backup, Rollback, And Restore

Run a database backup before every upgrade:

```bash
APP_ENV_FILE=.env.production docker compose -f docker-compose.deploy.yml --env-file .env.production run --rm app-server npm run db:backup
```

Keep rollback copies under `/srv/p-t/rollback/<timestamp>`. Prefer code rollback first. If a schema change is incompatible with old code, restore the pre-upgrade database backup:

```bash
BACKUP_FILE=/app/backups/<backup>.dump npm run db:restore
```

Never run `docker compose down -v` on production unless intentionally deleting all persisted data.
