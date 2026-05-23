# Database Migrations

This directory contains versioned PostgreSQL migrations for the BTC paper trading server.

Current stage:

- `npm run db:migrate` applies pending SQL files in `db/migrations`.
- `npm run db:status` prints applied/pending migrations.
- `npm run test:migrations` validates migration filenames and duplicate ids without needing a database.

Production upgrade order:

1. Stop new trading writes or enter maintenance mode.
2. Run `npm run db:backup`.
3. Run `npm run db:migrate`.
4. Optionally set `SERVER_REQUIRE_MIGRATIONS=true` and `EXPECTED_SCHEMA_MIGRATION_ID=000007`.
5. Start the server and verify login, trading, logs, export, and user management.

`store.ts` still contains development bootstrap schema SQL. Do not remove it until a full initial schema migration and old-database upgrade smoke test exist. The migration guard is opt-in during this transitional stage so existing local databases keep starting normally.
