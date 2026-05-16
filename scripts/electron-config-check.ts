import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const packageJson = JSON.parse(readFileSync("package.json", "utf8")) as {
  scripts: Record<string, string>;
  build: {
    productName: string;
    appId: string;
    directories: { output: string };
    win: { artifactName: string };
  };
};
const mainSource = readFileSync("apps/client/electron/main.cjs", "utf8");
const prodPackageSource = readFileSync("scripts/package-win-prod.cjs", "utf8");
const testPackageSource = readFileSync("scripts/package-win-test.cjs", "utf8");
const apiSource = readFileSync("apps/client/src/utils/api.ts", "utf8");
const appSource = readFileSync("apps/client/src/App.tsx", "utf8");
const redactionSource = readFileSync("apps/client/src/utils/redaction.ts", "utf8");

assert.equal(packageJson.version, "0.2.0");
assert.equal(packageJson.scripts["package:win:test"], "node scripts/package-win-test.cjs");
assert.equal(packageJson.scripts["package:win:prod"], "node scripts/package-win-prod.cjs");
assert.equal(packageJson.scripts["test:electron-config"], "tsx scripts/electron-config-check.ts");
assert.equal(packageJson.build.productName, "BTC Paper Trading Test");
assert.match(packageJson.build.appId, /\.test$/);
assert.equal(packageJson.build.directories.output, "deploy/windows-test");
assert.match(packageJson.build.win.artifactName, /Test/);

assert.match(mainSource, /const LOCAL_API_BASE_URL = "http:\/\/127\.0\.0\.1:8787";/);
assert.match(mainSource, /function configureProxyBypass\(\)/);
assert.match(mainSource, /productionApiBaseUrl/);
assert.match(mainSource, /const PRODUCTION_PROXY_PORT = 18787/);
assert.match(mainSource, /function startProductionProxy\(\)/);
assert.match(mainSource, /pickProductionLocalAddress/);
assert.match(mainSource, /productionProxyServer\.on\("upgrade"/);
assert.match(mainSource, /proxy-bypass-list/);
assert.match(mainSource, /no-proxy-server/);
assert.match(mainSource, /function redactNetworkAddresses/);
assert.match(mainSource, /function shouldEmbedBackend\(\)/);
assert.match(mainSource, /process\.env\.ELECTRON_EMBED_BACKEND/);
assert.match(mainSource, /override === "true"/);
assert.match(mainSource, /override === "false"/);
assert.match(mainSource, /app\.isPackaged && app\.getName\(\)\.toLowerCase\(\)\.includes\("test"\)/);
assert.match(mainSource, /if \(shouldEmbedBackend\(\)\) \{\s*await startPackagedBackend\(\);/s);
assert.doesNotMatch(mainSource, /if \(app\.isPackaged\) \{\s*await startPackagedBackend\(\);/s);

assert.match(prodPackageSource, /VITE_API_BASE_URL is required/);
assert.match(prodPackageSource, /parsed\.protocol !== "https:"/);
assert.match(prodPackageSource, /ALLOW_INSECURE_PROD_HTTP/);
assert.match(prodPackageSource, /parsed\.protocol === "http:"/);
assert.match(prodPackageSource, /hostname === "localhost"/);
assert.match(prodPackageSource, /hostname === "127\.0\.0\.1"/);
assert.match(prodPackageSource, /ELECTRON_EMBED_BACKEND: "false"/);
assert.match(prodPackageSource, /-c\.productName=BTC Paper Trading/);
assert.match(prodPackageSource, /const packageVersion = String\(packageJson\.version \|\| "0\.0\.0"\);/);
assert.match(prodPackageSource, /const productionArtifactName = `BTC Paper Trading Setup \$\{packageVersion\}\.\\\$\{ext\}`;/);
assert.match(prodPackageSource, /-c\.extraMetadata\.productionApiBaseUrl=\$\{process\.env\.VITE_API_BASE_URL\}/);
assert.match(prodPackageSource, /-c\.win\.artifactName=\$\{productionArtifactName\}/);
assert.match(prodPackageSource, /-c\.directories\.output=deploy\/windows-production/);
assert.match(prodPackageSource, /const rendererApiBaseUrl = "http:\/\/127\.0\.0\.1:18787"/);
assert.match(prodPackageSource, /VITE_API_BASE_URL: rendererApiBaseUrl/);

assert.match(testPackageSource, /package:win:test|Windows test installer|memory backend|PERSISTENCE_MODE=memory/s);
assert.match(apiSource, /const API_BASE_URL = import\.meta\.env\.VITE_API_BASE_URL \|\| "http:\/\/127\.0\.0\.1:8787";/);
assert.match(apiSource, /redactNetworkAddresses/);
assert.match(apiSource, /async checkHealth\(\)/);
assert.doesNotMatch(apiSource, /baseUrl: API_BASE_URL/);
assert.match(apiSource, /replace\("https:\/\/", "wss:\/\/"\)/);
assert.match(appSource, /redactNetworkAddresses/);
assert.match(appSource, /v0\.2\.0 · Hyper Terminal/);
assert.match(appSource, /jsonPreview = \(value: unknown\) => redactNetworkAddresses/);
assert.doesNotMatch(appSource, /<span>\{api\.baseUrl\}<\/span>/);
assert.doesNotMatch(appSource, /api\.baseUrl/);
assert.match(redactionSource, /IPV4_WITH_OPTIONAL_PORT_PATTERN/);

console.log("electron-config-check ok");
