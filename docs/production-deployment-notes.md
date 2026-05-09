# 生产部署说明

## 当前部署形态

本次采用临时 HTTP 外部客户端部署：

```text
外部客户端 -> http://103.147.13.98:10001 -> Caddy -> app-server:8787
```

外部用户只使用客户端软件，不需要浏览器访问入口。HTTP 是临时方案，登录 token、用户数据和交易请求会明文传输；正式版本应升级为 HTTPS 后重新打包客户端。

## 主机与端口

```text
主机 IP：103.147.13.98
SSH 管理端口：22
客户端业务端口：10001
```

禁止公网暴露：

```text
8787 app-server
8788 matching-service
5432 PostgreSQL
6379 Redis
9090 Prometheus
3000 Grafana
```

## 中间件配置

```text
Caddy：监听 10001，反向代理到 app-server:8787，并阻止公网 /metrics。
PostgreSQL：Docker 内网访问，数据保存在 pgdata volume。
Redis：Docker 内网访问，启用 appendonly，数据保存在 redisdata volume。
matching-service：Docker 内网访问，仅 app-server 调用。
Prometheus/Grafana：默认 profile=monitoring，不对公网暴露。
```

## 生产环境变量

从 `.env.production.example` 复制生成 `.env.production`，必须替换：

```text
POSTGRES_PASSWORD
JWT_SECRET
EXPORT_ANONYMIZATION_SECRET
```

关键保护项：

```text
SERVER_STRICT_PERSISTENCE=true
SERVER_REQUIRE_MIGRATIONS=true
SERVER_ALLOW_DEV_SCHEMA_BOOTSTRAP=false
EXPECTED_SCHEMA_MIGRATION_ID=000004
SEED_DEFAULT_USERS=false
```

## 目录约定

```text
/srv/p-t/app       当前部署目录
/srv/p-t/releases  上传 release 归档
/srv/p-t/rollback  上一版本回滚目录
Docker volumes     pgdata、redisdata、logs、backups
```

## 首个 Admin

生产环境不创建默认 `admin/admin123`。首次部署后执行：

```bash
APP_ENV_FILE=.env.production docker compose -f docker-compose.deploy.yml --env-file .env.production run --rm app-server npx tsx scripts/create-admin.ts
```

如未传 `ADMIN_PASSWORD`，脚本会生成一次性强密码并输出到终端。
