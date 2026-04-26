# BTC 5-Minute Paper Trading / BTC 5 分钟模拟交易系统

## 1. Project Overview / 项目概述

**English**

This repository contains a BTC 5-minute paper trading system with:

- a Fastify backend
- a matching service
- a React + Vite frontend
- an Electron shell for desktop development
- Docker Compose files for local and deployment startup

The repository has been trimmed to keep only files required for development, build, and deployment. Test files, temporary files, runtime data, and old documentation are intentionally excluded.

**中文**

这个仓库包含一个 BTC 5 分钟模拟交易系统，主要由以下部分组成：

- Fastify 后端服务
- 撮合服务
- React + Vite 前端
- Electron 桌面开发壳
- 用于本地和部署启动的 Docker Compose 配置

当前仓库已经裁剪，只保留了开发、构建和部署所需文件。测试文件、临时文件、运行数据和旧说明文档都已主动排除。

---

## 2. Requirements / 环境要求

**English**

- Node.js 20+ or 24+
- npm 10+
- Docker Desktop (recommended for local dependencies and deployment)
- PostgreSQL 17 and Redis 8 if you do not use Docker

**中文**

- Node.js 20+ 或 24+
- npm 10+
- Docker Desktop（推荐，用于本地依赖和部署）
- 如果不使用 Docker，则需要自行准备 PostgreSQL 17 和 Redis 8

---

## 3. Repository Structure / 仓库结构

```text
apps/
  client/     React + Vite + Electron frontend
  server/     Fastify backend + matching service
scripts/
  setup-env.cjs
  local-doctor.cjs
.env.example
docker-compose.local.yml
docker-compose.deploy.yml
Dockerfile.server
package.json
```

---

## 4. Environment Setup / 环境配置

### 4.1 Create the env file / 创建环境变量文件

**English**

Run:

```bash
npm run setup:env
```

This creates `.env` from `.env.example`.

**中文**

执行：

```bash
npm run setup:env
```

这会基于 `.env.example` 生成 `.env`。

### 4.2 Update important env values / 修改关键环境变量

**English**

At minimum, review these fields in `.env`:

- `PORT`
- `MATCHING_SERVICE_PORT`
- `MATCHING_SERVICE_URL`
- `DATABASE_URL`
- `REDIS_URL`
- `JWT_SECRET`
- `INITIAL_BALANCE`
- `CHAINLINK_ENABLED`
- `UPSTREAM_PROXY_URL`
- `POLYMARKET_*`

If you want live Chainlink data, fill in:

- `CHAINLINK_RPC_URL`
- `CHAINLINK_FALLBACK_RPC_URLS`

**中文**

至少请检查 `.env` 中这些字段：

- `PORT`
- `MATCHING_SERVICE_PORT`
- `MATCHING_SERVICE_URL`
- `DATABASE_URL`
- `REDIS_URL`
- `JWT_SECRET`
- `INITIAL_BALANCE`
- `CHAINLINK_ENABLED`
- `UPSTREAM_PROXY_URL`
- `POLYMARKET_*`

如果你要接入真实 Chainlink 数据，还需要填写：

- `CHAINLINK_RPC_URL`
- `CHAINLINK_FALLBACK_RPC_URLS`

---

## 5. Local Development Startup / 本地开发启动

### 5.1 Install dependencies / 安装依赖

```bash
npm install
```

### 5.2 Start PostgreSQL and Redis with Docker / 用 Docker 启动 PostgreSQL 和 Redis

```bash
docker compose -f docker-compose.local.yml up -d postgres redis
```

### 5.3 Optional local check / 可选的本地检查

```bash
npm run doctor:local
```

### 5.4 Start the backend / 启动后端

**Backend only / 仅后端**

```bash
npm run dev:server
```

**Matching service only / 仅撮合服务**

```bash
npm run dev:matching
```

### 5.5 Start the full desktop development stack / 启动完整桌面开发栈

```bash
npm run dev:stack
```

This starts:

- backend
- renderer
- Electron shell

### 5.6 Start frontend + Electron only / 只启动前端和 Electron

```bash
npm run dev
```

---

## 6. Production Build / 生产构建

Run:

```bash
npm run build
```

This outputs:

- `dist/server`
- `dist/matching`
- `dist/renderer`

中文说明：

执行上面的命令后，会生成：

- `dist/server`
- `dist/matching`
- `dist/renderer`

---

## 7. Start Built Services Without Docker / 不使用 Docker 启动构建产物

### 7.1 Start the app server / 启动主服务

```bash
npm run start:server
```

### 7.2 Start the matching service / 启动撮合服务

```bash
npm run start:matching
```

---

## 8. Docker Deployment / Docker 部署

### 8.1 Prepare the env file / 准备环境变量文件

Make sure `.env` exists in the project root.

请确认项目根目录下已经存在 `.env`。

### 8.2 Build and start all deployment services / 构建并启动部署服务

```bash
docker compose -f docker-compose.deploy.yml up -d --build
```

This starts:

- `postgres`
- `redis`
- `matching-service`
- `app-server`

这会启动：

- `postgres`
- `redis`
- `matching-service`
- `app-server`

### 8.3 Check service status / 查看服务状态

```bash
docker compose -f docker-compose.deploy.yml ps
```

### 8.4 Check logs / 查看日志

```bash
docker compose -f docker-compose.deploy.yml logs -f
```

### 8.5 Stop deployment / 停止部署服务

```bash
docker compose -f docker-compose.deploy.yml down
```

---

## 9. Health Checks / 健康检查

**App server / 主服务**

```bash
curl http://127.0.0.1:8787/health
```

**Matching service / 撮合服务**

```bash
curl http://127.0.0.1:8788/health
```

Windows PowerShell can also use:

```powershell
Invoke-RestMethod http://127.0.0.1:8787/health
Invoke-RestMethod http://127.0.0.1:8788/health
```

---

## 10. Recommended Local Workflow / 推荐的本地工作流

**English**

1. `npm install`
2. `npm run setup:env`
3. Update `.env`
4. `docker compose -f docker-compose.local.yml up -d postgres redis`
5. `npm run doctor:local`
6. `npm run dev:stack`

**中文**

1. `npm install`
2. `npm run setup:env`
3. 修改 `.env`
4. `docker compose -f docker-compose.local.yml up -d postgres redis`
5. `npm run doctor:local`
6. `npm run dev:stack`

---

## 11. Deployment Notes / 部署注意事项

**English**

- Do not commit the real `.env`.
- Fill real RPC and market configuration before expecting full live-source behavior.
- The Docker image no longer depends on a committed `data/` directory; runtime log directories are created inside the container.
- If GitHub access requires a proxy, configure `HTTP_PROXY` and `HTTPS_PROXY`.

**中文**

- 不要提交真实的 `.env`。
- 如果你希望系统连接真实数据源，请先补全真实 RPC 和市场配置。
- 现在的 Docker 镜像不再依赖仓库里提交 `data/` 目录，容器会在运行时自行创建日志目录。
- 如果访问 GitHub 需要代理，请配置 `HTTP_PROXY` 和 `HTTPS_PROXY`。

---

## 12. Included and Excluded Content / 当前保留与排除内容

**Included / 已保留**

- source code
- env template
- build scripts
- Docker deployment files
- local setup helper scripts

**Excluded / 已排除**

- test scripts
- docs directory
- old descriptive documents
- runtime data
- build artifacts
- temporary logs

