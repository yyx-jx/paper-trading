# Realtime Display Latency Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Each completed task must be marked `[x]` and must add evidence under Execution Log.

**Goal:** Reduce controllable frontend latency for displayed prices, K-line rendering, order book display, and post-order user-state updates, while keeping later deploy features intact.

**Architecture:** Do not roll back the whole branch to `d0f9934`. Preserve the current split `market:tick` plus low-frequency `market` full snapshot architecture, but borrow the key `d0f9934` behavior: every user-visible realtime market field must update from high-frequency tick data and must not wait for full snapshots. Full snapshots are only for low-frequency reconciliation.

**Tech Stack:** Fastify WebSocket, Zustand store, React, TypeScript, `tsx` diagnostics.

---

## Execution Log

- [x] **Phase 0: Create Plan Document**
  - Status: Completed.
  - Evidence: Created `D:\P_T\docs\superpowers\plans\2026-05-19-realtime-display-latency-fix.md`.

- [x] **Phase 1: Capture Production Baseline**
  - Status: Completed.
  - Command:
    ```powershell
    $env:LOAD_TEST_BASE_URL='http://103.147.13.98:10001'
    $env:LOAD_TEST_USERNAME='admin'
    $env:LOAD_TEST_PASSWORD='qwe123'
    $env:LOAD_TEST_SAMPLE_MS='120000'
    npm run test:market-ws-latency
    ```
  - Evidence: The npm script did not exit cleanly within 180 seconds, so an equivalent read-only Node WebSocket sampler was used for a 60 second baseline.
  - Baseline at `2026-05-19T06:20:50.722Z`: `firstTickMs=227`, `firstFullMs=564`, `tickIntervalP95=623`, `tickServerToClientP95=1275`, `fullServerToClientP95=3217`, `payloadBytesP95=6780`, `fullPayloadBytesP95=168581`, `binanceSourceAgeP95=3186`, `clobSourceAgeP95=1464`.

- [x] **Phase 2: Ensure Tick Covers Realtime Display Fields**
  - Status: Completed.
  - Files:
    - `D:\P_T\apps\server\src\domain\types.ts`
    - `D:\P_T\apps\client\src\utils\api.ts`
    - `D:\P_T\apps\server\src\index.ts`
    - `D:\P_T\apps\client\src\store\useAppStore.ts`
  - Evidence: `npm run test:market-tick-payload` passed. Tick carries candle updates and top levels and still does not carry full `orderBooks`.

- [x] **Phase 3: Make Tick Always Higher Priority Than Full Snapshot**
  - Status: Completed.
  - File: `D:\P_T\apps\server\src\index.ts`
  - Evidence: `registerMarketBroadcastClient()` now requests a tick before scheduling the first full snapshot. `sendFullSnapshotForClient()` retries when a tick was recently sent, a tick retry is pending, the client is sending a tick, or the socket has backpressure.

- [x] **Phase 4: Reduce Full and Bootstrap Payload Size**
  - Status: Completed.
  - Files:
    - `D:\P_T\apps\server\src\index.ts`
    - `D:\P_T\apps\client\src\utils\api.ts`
  - Evidence: Added `compactSnapshotForTransport()` for current/full/bootstrap transport snapshots. Bootstrap history was reduced to 30 rows and operated history to 200 rows. Exact byte targets require a deployment or local production-like server sample.

- [x] **Phase 5: Send Minimal User Trade Updates After Orders**
  - Status: Completed.
  - Files:
    - `D:\P_T\apps\server\src\index.ts`
    - `D:\P_T\apps\server\src\services\simulation.ts`
    - `D:\P_T\apps\client\src\store\useAppStore.ts`
  - Evidence: `/api/orders` now returns `tradePatch` with profile, positions, recent orders, and bounded lifecycle data. The client applies `tradePatch` immediately after successful order placement.

- [x] **Phase 6: Reduce Frontend Fallback Interference**
  - Status: Completed.
  - File: `D:\P_T\apps\client\src\App.tsx`
  - Evidence: Market stale/reconnect/fallback thresholds were raised to avoid normal jitter triggering fallback. Market fallback now only fetches the lightweight current-round payload and reuses existing history instead of pulling history in parallel.

- [ ] **Phase 7: Verification and Regression**
  - Status: Local verification completed; post-deploy production verification pending.
  - Commands:
    ```powershell
    npm run typecheck
    npm run test:market-tick-payload
    npm run test:frontend-display
    npm run test:regression
    npm run build
    ```
  - Production smoke:
    ```powershell
    $env:LOAD_TEST_BASE_URL='http://103.147.13.98:10001'
    $env:LOAD_TEST_USERNAME='admin'
    $env:LOAD_TEST_PASSWORD='qwe123'
    $env:LOAD_TEST_SAMPLE_MS='120000'
    npm run test:market-ws-latency
    ```
  - Evidence:
    - `npm run typecheck` passed.
    - `npm run test:market-tick-payload` passed.
    - `npm run test:frontend-display` passed.
    - `npm run test:regression` passed after updating regression assertions for the new fallback thresholds and compact transport snapshot.
    - `npm run build` passed.
    - `npm run test:market-ws-latency` now exits cleanly; a 20 second smoke against the currently deployed service recorded `firstTickMs=176`, `tickIntervalP95=414`, `tickServerToClientP95=883`, `fullPayloadBytesP95=165695`. This smoke proves the sampler works, but it does not validate these code changes until the new build is deployed.

- [x] **Addendum: Fix Chainlink Lower K-Line Not Drawing**
  - Status: Completed.
  - Root cause: `syncChainlinkHistoryCandles()` replaced `this.chainlinkCandlesByInterval[interval]` with connector history on every snapshot. Because connector history can be sparse or older than the Binance visible domain, accumulated realtime Chainlink bars were discarded repeatedly, so the lower Chainlink chart often had no drawable bars inside the shared chart window.
  - Files:
    - `D:\P_T\apps\server\src\services\simulation.ts`
    - `D:\P_T\scripts\flow-regression-check.ts`
  - Fix: Added `mergeChainlinkHistoryBars()` so connector history is merged with locally accumulated realtime Chainlink bars by `startTs`, with local realtime bars winning duplicate buckets and interval limits preserved.
  - Evidence:
    - `npm run test:regression` first failed on the missing merge contract, then passed after the fix.
    - `npm run typecheck` passed.
    - `npm run test:frontend-display` passed.
    - `npm run test:market-tick-payload` passed.
    - `npm run build` passed.

## Risks

- [ ] Binance source age can remain around 1 second even after frontend transport is fixed. If frontend metrics pass but official comparison still lags, investigate server egress, DNS, Binance WS path, reconnect behavior, and upstream processing separately.
- [ ] Tick payload can become too large. Keep tick limited to the current candle bar, displayed book depth, source status, latency, and small current-round chart increments.
- [ ] Chart redraw can dominate. Replace only the current bar and keep full history reconciliation low-frequency.
- [ ] Optimistic order updates can conflict with server correction. Use `clientOrderId` and `order.id` to deduplicate, with `user:trade` as the final authority.
- [ ] Full rollback to `d0f9934` would lose later permission, audit, matching, and deployment fixes. Borrow realtime semantics only.
