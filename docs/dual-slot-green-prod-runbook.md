# Green / Prod 双环境部署与镜像晋升 Runbook

目标：

- `prod` 继续承载正式业务
- `green` 提供独立测试环境
- 测试和生产不共享 PostgreSQL、Redis、业务账号、订单、持仓、结算、日志
- 测试通过后，把同一组已验证镜像快速晋升到生产应用层

## 1. 文件准备

远程目录建议：

```text
/srv/p-t/app                当前正式环境代码目录
/srv/p-t/green              测试环境代码目录
/srv/p-t/releases           release 归档
/srv/p-t/rollback           正式环境代码回滚快照
```

环境文件：

```text
/srv/p-t/app/.env.production
/srv/p-t/green/.env.green
```

`.env.production` 关键值：

```text
PUBLIC_PORT=10001
POSTGRES_DB=paper_trading
POSTGRES_USER=paper_trading
APP_SERVER_IMAGE=p-t-app-server:latest
MATCHING_SERVICE_IMAGE=p-t-matching-service:latest
```

`.env.green` 关键值：

```text
PUBLIC_PORT=10002
POSTGRES_DB=paper_trading_green
POSTGRES_USER=paper_trading_green
APP_SERVER_IMAGE=p-t-app-server:0.6.3-rc1
MATCHING_SERVICE_IMAGE=p-t-matching-service:0.6.3-rc1
HYPER_BRIDGE_ENABLED=false
```

## 2. 启动 green 测试环境

复制当前 release 到测试目录：

```bash
mkdir -p /srv/p-t/green
cp -a /srv/p-t/releases/<release>/server/. /srv/p-t/green/
cp -a /srv/p-t/green/.env.green.example /srv/p-t/green/.env.green
```

构建并启动 `green`：

```bash
cd /srv/p-t/green
APP_ENV_FILE=.env.green docker compose -p app-green -f docker-compose.deploy.yml --env-file .env.green build app-server matching-service
APP_ENV_FILE=.env.green docker compose -p app-green -f docker-compose.deploy.yml --env-file .env.green up -d
APP_ENV_FILE=.env.green docker compose -p app-green -f docker-compose.deploy.yml --env-file .env.green run --rm app-server npm run db:migrate
APP_ENV_FILE=.env.green docker compose -p app-green -f docker-compose.deploy.yml --env-file .env.green run --rm app-server npm run db:status
```

测试入口：

```text
http://<PRODUCTION_HOST>:10002
```

## 3. green 业务验收

至少完成：

- 登录
- 行情 WS
- 用户 WS
- 下单
- 撤单
- 平仓
- 结算

同时确认：

```bash
curl -f http://<PRODUCTION_HOST>:10002/api/health/live
curl -f http://<PRODUCTION_HOST>:10002/api/health/ready
APP_ENV_FILE=.env.green docker compose -p app-green -f docker-compose.deploy.yml --env-file .env.green logs --tail=200 app-server matching-service
```

生产环境在 green 测试期间保持正常：

```bash
curl -f http://<PRODUCTION_HOST>:10001/api/health/live
curl -f http://<PRODUCTION_HOST>:10001/api/health/ready
APP_ENV_FILE=.env.production docker compose -p app-prod -f docker-compose.deploy.yml --env-file .env.production ps
```

如果当前线上仍使用历史项目名 `app`，把上面的 `-p app-prod` 暂时替换为 `-p app`。

## 4. 镜像晋升到生产

正式切换前，先保存生产代码快照：

```bash
cd /srv/p-t/app
ts=$(date +%Y%m%d_%H%M%S)
mkdir -p /srv/p-t/rollback/$ts
cp -a /srv/p-t/app /srv/p-t/rollback/$ts/app
```

把 green 已验证镜像晋升到生产应用层，不切换生产数据库：

```bash
cd /srv/p-t/app
APP_SERVER_IMAGE=p-t-app-server:0.6.3-rc1 \
MATCHING_SERVICE_IMAGE=p-t-matching-service:0.6.3-rc1 \
APP_ENV_FILE=.env.production \
docker compose -p app-prod -f docker-compose.deploy.yml --env-file .env.production up -d --no-build --force-recreate caddy app-server matching-service
```

如果当前线上仍使用历史项目名 `app`，把 `-p app-prod` 替换为 `-p app`。

## 5. 正式环境验收与回滚

正式切换后确认：

```bash
curl -f http://<PRODUCTION_HOST>:10001/api/health/live
curl -f http://<PRODUCTION_HOST>:10001/api/health/ready
```

回滚方式是重新指定上一版生产镜像并重建容器：

```bash
cd /srv/p-t/app
APP_SERVER_IMAGE=<previous-prod-app-image> \
MATCHING_SERVICE_IMAGE=<previous-prod-matching-image> \
APP_ENV_FILE=.env.production \
docker compose -p app-prod -f docker-compose.deploy.yml --env-file .env.production up -d --no-build --force-recreate caddy app-server matching-service
```

禁止事项：

- 不允许把 `green` 数据库直接扶正为生产数据库
- 不允许测试环境连接生产 PostgreSQL 或生产 Redis
- 不允许执行 `docker compose down -v`
