# Production Monitoring

This project exposes two monitoring surfaces:

- `GET /api/metrics`: JSON overview for the application UI and lightweight checks.
- `GET /metrics`: Prometheus text format for Docker-internal scraping.

The deploy Caddyfile blocks public `/metrics` by default. Prometheus scrapes `app-server:8787/metrics` over the Docker network when the monitoring profile is enabled.
The monitoring profile also starts `node-exporter` and cAdvisor for host and container resource history.

## Configuration

Required or recommended production variables:

```env
METRICS_ENABLED=true
METRICS_BASIC_AUTH_USER=
METRICS_BASIC_AUTH_PASSWORD=
PROMETHEUS_RETENTION=15d
GRAFANA_ADMIN_PASSWORD=change-me
```

For the bundled Docker deployment, no public `/metrics` exposure is needed. If you intentionally expose `/metrics` through a reverse proxy, configure Basic Auth or an allowlist at the proxy layer.

Redis is treated as cache, snapshot, and pub/sub storage in this deployment. Orders, balances, positions, and ledgers remain authoritative in PostgreSQL, so the production Redis command disables AOF/RDB persistence to avoid rewrite/BGSAVE disk I/O spikes on the order write path.

## Start Monitoring

Start the app stack with monitoring services:

```bash
docker compose -f docker-compose.deploy.yml --profile monitoring up -d --build
```

Prometheus and Grafana are only on the Docker internal network by default. If you expose Grafana through a private tunnel or a locked-down reverse proxy, change `GRAFANA_ADMIN_PASSWORD` first.

## What Is Collected

Prometheus metrics include:

- Host resources from node-exporter: CPU, memory, filesystem, and basic OS counters.
- Container resources from cAdvisor: per-container CPU, memory, and runtime pressure.
- Node runtime: heap, uptime, event loop lag, default Node metrics.
- HTTP: request count and duration by method, route, and status.
- WebSocket: market/user connections, payload bytes, send duration, disconnect reason.
- Trading: order outcomes, placement latency, pending order gauge, position close outcomes.
- Persistence: PostgreSQL and Redis health state.
- JSONL: queue depth, flush duration, write failures, file rotations, dropped records, backlog state, and current file size.
- External market sources: state and stale age for Binance, Chainlink, and CLOB.
- Export/import: dataset and log export requests, exported rows, bulk import outcomes.

## Alerts

Alert rules are shipped in `deploy/prometheus/alerts.yml`:

- `AppDown`
- `ReadinessFailed`
- `HighEventLoopLag`
- `HighHeapUsage`
- `PostgresUnavailable`
- `RedisUnavailable`
- `ExternalSourceStale`
- `HighOrderFailureRate`
- `JsonlQueueBacklog`
- `JsonlDroppedRecords`
- `WsBackpressure`
- `ExportFailures`
- `HostHighCpu`
- `HostLowMemory`
- `HostLowDiskSpace`
- `ContainerHighCpu`

The repository does not ship an Alertmanager route. In production, wire these rules to your environment's Alertmanager receiver, such as email, Enterprise WeChat, or an incident platform.

## Verification

Run repository checks before deployment:

```bash
npm run test:metrics
npm run test:jsonl-rotation
npm run test:monitoring-deploy
npm run test:deployment
```

After deployment:

```bash
docker compose -f docker-compose.deploy.yml --profile monitoring ps
docker compose -f docker-compose.deploy.yml exec app-server wget -qO- http://127.0.0.1:8787/api/health/ready
docker compose -f docker-compose.deploy.yml exec prometheus wget -qO- http://app-server:8787/metrics | head
docker compose -f docker-compose.deploy.yml exec prometheus wget -qO- http://node-exporter:9100/metrics | head
docker compose -f docker-compose.deploy.yml exec prometheus wget -qO- http://cadvisor:8080/metrics | head
```

If Caddy returns `404` for public `/metrics`, that is the intended default posture.
