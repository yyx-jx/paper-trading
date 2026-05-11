# 打包与发布工作流

## 1. 本地检查

```bash
npm ci
npm run typecheck
npm run build
npm run test:config
npm run test:permissions
npm run test:deployment
npm run test:deployment-readiness
npm run test:migration-safety
npm run test:order-transactions
npm run test:redeem-idempotency
npm run test:electron-config
```

任何失败都先修复，不进入发布。

## 2. 服务端 release 打包

```bash
npm run package:release
```

release 包必须不包含：

```text
.env
data/
backups/
node_modules/
dist/
release/
```

打包脚本会生成 `checksums.sha256`。

## 3. 生产客户端打包

当前临时 HTTP 版本：

```bash
ALLOW_INSECURE_PROD_HTTP=true VITE_API_BASE_URL=http://<PRODUCTION_HOST>:10001 npm run package:win:prod
```

正式 HTTPS 版本应改为：

```bash
VITE_API_BASE_URL=https://<domain> npm run package:win:prod
```

## 4. 上传服务器

```bash
ssh -p 22 root@<PRODUCTION_HOST> "mkdir -p /srv/p-t/{app,releases,rollback,data/backups}"
rsync -av -e "ssh -p 22" release/<release>/server/ root@<PRODUCTION_HOST>:/srv/p-t/app/
```

升级前先保存回滚目录：

```bash
ssh -p 22 root@<PRODUCTION_HOST> "ts=$(date +%Y%m%d_%H%M%S); mkdir -p /srv/p-t/rollback/$ts; cp -a /srv/p-t/app/. /srv/p-t/rollback/$ts/"
```

## 5. 备份、迁移、启动

```bash
cd /srv/p-t/app
APP_ENV_FILE=.env.production docker compose -f docker-compose.deploy.yml --env-file .env.production run --rm app-server npm run db:backup
APP_ENV_FILE=.env.production docker compose -f docker-compose.deploy.yml --env-file .env.production run --rm app-server npm run db:migrate
APP_ENV_FILE=.env.production docker compose -f docker-compose.deploy.yml --env-file .env.production run --rm app-server npm run db:status
APP_ENV_FILE=.env.production docker compose -f docker-compose.deploy.yml --env-file .env.production up -d --build
```

首次部署还要创建 Admin：

```bash
APP_ENV_FILE=.env.production docker compose -f docker-compose.deploy.yml --env-file .env.production run --rm app-server npx tsx scripts/create-admin.ts
```

## 6. 验收

```bash
curl -f http://<PRODUCTION_HOST>:10001/api/health/live
curl -f http://<PRODUCTION_HOST>:10001/api/health/ready
```

客户端 smoke：

```text
登录
行情 WS
用户 WS
下单
撤单
卖出/平仓
日志查询
```

## 7. 数据库升级规则

允许：

```text
新增表
新增字段
新增索引
兼容式回填
双读/双写过渡
```

禁止：

```text
DROP TABLE/COLUMN/SCHEMA
TRUNCATE
无 WHERE 的 DELETE
重建生产 schema
docker compose down -v
```

## 8. 回滚

代码回滚：

```bash
cd /srv/p-t/app
APP_ENV_FILE=.env.production docker compose -f docker-compose.deploy.yml --env-file .env.production stop app-server matching-service
cp -a /srv/p-t/rollback/<timestamp>/. /srv/p-t/app/
APP_ENV_FILE=.env.production docker compose -f docker-compose.deploy.yml --env-file .env.production up -d --build
```

数据库回滚只在 schema 不兼容时使用，会覆盖备份后产生的数据。
