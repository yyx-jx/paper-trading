# Frontend Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the current Electron/React frontend by page and feature while preserving every current behavior, websocket pacing path, fallback flow, and user-visible latency.

**Architecture:** Keep `apps/client/src/App.tsx` as the app shell and orchestration boundary. Move page-sized UI and page-local helpers into `features/*`, keep cross-page primitives in `components/*` and `utils/*`, and keep the existing Zustand store/API contracts stable during this refactor. Do not change backend APIs, CSS class names, i18n keys, realtime throttling constants, requestAnimationFrame usage, or data-fetch fallback sequences.

**Tech Stack:** React 19, Vite 7, Zustand, TypeScript, Electron renderer, existing CSS and i18n.

---

## Guardrails

Work only in `D:\P_T-worktrees\frontend-refactor-deploy` on branch `codex/frontend-refactor-deploy`. Do not edit `D:\P_T`.

Baseline verification:

```powershell
npm run typecheck
npm run build:renderer
```

Baseline result:

```text
typecheck: pass
renderer build: pass
renderer JS: 404.92 kB, gzip 122.79 kB
renderer CSS: 65.49 kB, gzip 12.95 kB
```

Do not change backend APIs, DTO shapes, websocket message types, store shape, CSS class names, or latency-sensitive constants. Keep existing regression source-contract anchors in `App.tsx` until scripts are updated.

## Tasks

- [ ] Extract `LoginScreen` to `apps/client/src/features/auth/LoginScreen.tsx` and `TerminalSection` to `apps/client/src/components/TerminalSection.tsx`; run `npm run typecheck` and `npm run build:renderer`.
- [ ] Extract realtime status pure helpers to `apps/client/src/features/realtime/realtimeStatus.ts`, leaving the websocket effect in `App.tsx`; run `npm run typecheck` and `npm run build:renderer`.
- [ ] Extract analytics model and page to `apps/client/src/features/analytics/analyticsModel.ts` and `apps/client/src/features/analytics/AnalyticsPage.tsx`; run `npm run typecheck`, `npm run build:renderer`, and `npm run test:trading`.
- [ ] Extract log search/export to `apps/client/src/features/logs/*`; run `npm run typecheck`, `npm run build:renderer`, and `npm run test:trading`.
- [ ] Extract bulk user and user management to `apps/client/src/features/users/*`; run `npm run typecheck`, `npm run build:renderer`, `npm run test:bulk-users`, and `npm run test:permissions`.
- [ ] Extract trade page in slices under `apps/client/src/features/trade/*`; run `npm run typecheck`, `npm run build:renderer`, `npm run test:trading`, and `npm run test:regression`.
- [ ] Only after all previous tasks pass, optionally move the websocket effect into `apps/client/src/features/realtime/useRealtimeStreams.ts`; run `npm run typecheck`, `npm run build:renderer`, `npm run test:trading`, and `npm run test:regression`.
- [ ] Final review: run `git status --short`, `git diff --stat`, compare bundle size to baseline, and confirm `D:\P_T` is unchanged from its original dirty state.
