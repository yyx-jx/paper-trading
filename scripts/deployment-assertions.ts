import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { buildServerConfig } from "../apps/server/src/config";

export function readText(path: string) {
  return readFileSync(path, "utf8");
}

export function assertProductionDeploymentBaseline() {
  const compose = readText("docker-compose.deploy.yml");
  const dockerfile = readText("Dockerfile.server");
  const caddy = readText("deploy/Caddyfile");
  const packageJson = readText("package.json");
  const deploymentDoc = readText("docs/deployment-production.md");

  assert.match(compose, /caddy:/);
  assert.match(compose, /"10001:10001"/);
  assert.doesNotMatch(compose, /"8787:8787"/);
  assert.doesNotMatch(compose, /"8788:8788"/);
  assert.doesNotMatch(compose, /"5432:5432"/);
  assert.doesNotMatch(compose, /"6379:6379"/);
  assert.match(compose, /SERVER_REQUIRE_MIGRATIONS: "true"/);
  assert.match(compose, /SERVER_ALLOW_DEV_SCHEMA_BOOTSTRAP: "false"/);
  assert.match(compose, /TRUST_PROXY: "true"/);
  assert.match(compose, /healthcheck:/);
  assert.match(compose, /http:\/\/127\.0\.0\.1:8787\/api\/health\/ready/);
  assert.doesNotMatch(dockerfile, /COPY data \.\/data/);
  assert.match(caddy, /:10001/);
  assert.match(caddy, /reverse_proxy app-server:8787/);
  assert.match(caddy, /respond @metrics 404/);
  assert.match(packageJson, /"package:win:prod": "node scripts\/package-win-prod\.cjs"/);
  assert.match(packageJson, /"test:electron-config": "tsx scripts\/electron-config-check\.ts"/);
  assert.match(deploymentDoc, /ALLOW_INSECURE_PROD_HTTP=true VITE_API_BASE_URL=http:\/\/103\.147\.13\.98:10001 npm run package:win:prod/);

  assert.throws(() =>
    buildServerConfig({
      NODE_ENV: "production",
      JWT_SECRET: "btc-paper-trading-secret",
      CORS_ORIGINS: "https://example.com",
      EXPORT_ANONYMIZATION_SECRET: "export-secret"
    })
  );
  assert.throws(() =>
    buildServerConfig({
      NODE_ENV: "production",
      JWT_SECRET: "jwt-secret",
      EXPORT_ANONYMIZATION_SECRET: "export-secret"
    })
  );
  assert.throws(() =>
    buildServerConfig({
      NODE_ENV: "production",
      JWT_SECRET: "jwt-secret",
      CORS_ORIGINS: "https://example.com",
      EXPORT_ANONYMIZATION_SECRET: "jwt-secret"
    })
  );

  const production = buildServerConfig({
    NODE_ENV: "production",
    JWT_SECRET: "jwt-secret",
    CORS_ORIGINS: "https://example.com",
    EXPORT_ANONYMIZATION_SECRET: "export-secret"
  });
  assert.equal(production.requireSchemaMigrations, true);
  assert.equal(production.allowDevSchemaBootstrap, false);
  assert.equal(production.strictPersistence, true);
  assert.equal(production.persistenceMode, "external");
  assert.equal(production.seedDefaultUsers, false);
}
