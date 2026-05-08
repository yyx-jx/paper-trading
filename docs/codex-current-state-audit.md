# Codex Current State Audit

Updated: 2026-05-07

## Summary

This audit records the implementation state after adding the personal home page and first migration tooling slices. The project is a BTC 5-minute UP/DOWN paper trading C/S system with an Electron/React client, Fastify TypeScript server, PostgreSQL/Redis persistence/cache, JSONL audit backups, and in-memory hot state. The main trading path remains `SimulationEngine.placeOrder()` plus CLOB book estimation, not local user-to-user matching.

## Implemented In This Slice

| Area | Current State | Files |
|---|---|---|
| Personal home page | Added a dedicated `home` page with identity, account editing, password change, asset summary, recent activity, and scoped user management. The existing `profile` analytics page remains separate. | `apps/client/src/App.tsx`, `apps/client/src/styles.css` |
| Self profile update | Added `PATCH /api/me` for display name and language updates, with audit logging. | `apps/server/src/index.ts`, `apps/server/src/services/store.ts`, `apps/client/src/utils/api.ts` |
| Managed user update | Added `PATCH /api/users/:id` scoped by backend permission and management scope. Supports display name, role, language, manager, permission level, balance, and active state where allowed. | `apps/server/src/index.ts`, `apps/server/src/services/store.ts`, `apps/client/src/utils/api.ts` |
| Manager compatibility | Kept `seniorTesterId` and added `managerUserId` as the forward-compatible field. Existing data is backfilled from `senior_tester_id` when loaded. | `apps/server/src/domain/types.ts`, `apps/server/src/services/store.ts` |
| Permission level | Added `permissionLevel` / `permission_level` with default `Standard`; supports `Initial` and `Standard`. | `apps/server/src/domain/types.ts`, `apps/server/src/services/store.ts`, `apps/client/src/utils/api.ts` |
| Backend authorization | Admin can manage all users; Senior Tester can manage assigned Tester accounts; Test Engineer can list scoped users but does not receive update/reset permissions by default. | `apps/server/src/index.ts`, `apps/server/src/services/store.ts` |
| User visualization | Added visible/active/manageable/role summary cards, search, role filter, profile edit dialog, and status/permission chips. | `apps/client/src/App.tsx`, `apps/client/src/styles.css` |
| Standalone user page cleanup | Removed the old independent `users` route key after embedding scoped user management into the personal home page, and kept the old `profile` route for analytics. | `apps/client/src/App.tsx`, `apps/client/src/store/useAppStore.ts` |
| Migration runner | Added SQL migration directory, idempotent user-management compatibility migration, migration status/check/apply scripts, and package commands. | `db/migrations`, `scripts/db-migrate.ts`, `package.json` |
| Backup/restore scripts | Added PostgreSQL dump/restore helper scripts with backup checksum output. | `scripts/db-backup.sh`, `scripts/db-restore.sh`, `db/README.md` |
| Schema guard | Added opt-in `SERVER_REQUIRE_MIGRATIONS=true` startup guard for `schema_migrations` and `EXPECTED_SCHEMA_MIGRATION_ID`. | `.env.example`, `apps/server/src/config.ts`, `apps/server/src/services/store.ts` |

## Existing Capabilities Confirmed

| Area | Current State |
|---|---|
| Password storage | New and reset passwords are bcrypt-hashed. Legacy plaintext passwords are upgraded on successful login. |
| Self password change | `POST /api/me/password` already exists and verifies the current password. |
| Managed password reset | `POST /api/users/:id/reset-password` exists and requires the operator password. |
| User list scope | `/api/users` returns all users for Admin and scoped users for Senior Tester/Test Engineer. |
| Audit logs | User create, bulk create, disable, enable, password reset/change, balance update, and profile update write audit events. |
| PnL/fees | The current order path already calculates estimated/actual CLOB fees and includes buy fees in cost and sell fees in proceeds. Further explicit PnL fields remain pending. |
| Export | Existing `/api/logs/export` is preserved and continues to produce ZIP exports with anonymized IDs. |

## Still Pending From Master Plan

| Area | Status | Recommended Next Action |
|---|---|---|
| Formal migration system | Lightweight runner implemented; full initial schema migration is still pending. `store.ts` still contains `CREATE TABLE IF NOT EXISTS` and `ALTER TABLE ADD COLUMN IF NOT EXISTS`. | Move the full schema out of `store.ts`, add old-database upgrade smoke tests, then make the schema guard mandatory for production. |
| Backup/restore scripts | Basic PostgreSQL dump/restore helpers added; automated restore smoke test is pending. | Add a disposable database restore check in CI/deploy scripts. |
| Full auth module extraction | Not implemented. Authorization helpers still live in `index.ts` and role permissions in `store.ts`. | Extract `auth/permissions.ts`, `auth/scope.ts`, `auth/authz.ts`, and `auth/password.ts`. |
| WS ticket/heartbeat/delta | Not implemented in this slice. WS still uses JWT in query params and user channel sends full payloads. | Add ticket exchange, heartbeat cleanup, seq gap fallback, and user delta/throttle. |
| Transaction layer | Not implemented in this slice. Critical write paths still update memory and persist through existing store methods. | Add repository/transaction helpers for order, cancel, pending fail, redeem, manual settlement, and user updates. |
| Async JSONL writer | Not implemented. `appendFileSync` / `writeFileSync` remain in `store.ts`. | Add log writer queue, rotation, flush on shutdown, and queue metrics. |
| Metrics/readiness | Existing `/health` and latency/system endpoints exist; Prometheus-style metrics and `/api/health/live|ready` are pending. | Add health split and metrics endpoint. |
| Customer dataset export | Not implemented in this slice. Internal log export remains intact. | Add separate customer dataset export API with manifest/schema and D-grade filtering. |
| Docker reverse proxy hardening | Not implemented in this slice. | Update deploy compose and add Caddy/Nginx docs after migration and health checks. |

## Verification Run

```text
npm run typecheck -- --pretty false
npm run test:migrations
npm run test:config
npm run test:permissions
npm run test:bulk-users
npm run test:logs
npm run test:export
npm run test:trading
npm run test:regression
npm run build
```

All listed commands passed on 2026-05-07.

## Notes For Next Codex Pass

- Do not remove `seniorTesterId`; treat `managerUserId` as the forward-compatible alias.
- Do not expose password hashes or any password material in `PublicUser`.
- Keep `/api/logs/export` intact while adding any future customer data export.
- The new personal home page intentionally reuses existing user management functionality; analytics stays on the separate `profile` page.
