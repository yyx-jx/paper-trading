import assert from "node:assert/strict";
import { buildServerConfig, DEFAULT_CHAINLINK_FALLBACK_RPC_URLS, DEFAULT_CHAINLINK_RPC_URL } from "../apps/server/src/config";

const blankRpcConfig = buildServerConfig({
  CHAINLINK_ENABLED: "true",
  CHAINLINK_RPC_URL: "",
  CHAINLINK_FALLBACK_RPC_URLS: " , https://rpc.example/a ,  ,https://rpc.example/b "
});

assert.equal(blankRpcConfig.chainlinkEnabled, true);
assert.equal(blankRpcConfig.chainlinkRpcUrl, DEFAULT_CHAINLINK_RPC_URL);
assert.deepEqual(blankRpcConfig.chainlinkFallbackRpcUrls, ["https://rpc.example/a", "https://rpc.example/b"]);

const disabledConfig = buildServerConfig({
  CHAINLINK_ENABLED: "false",
  CHAINLINK_RPC_URL: "   ",
  UPSTREAM_PROXY_URL: "   "
});

assert.equal(disabledConfig.chainlinkEnabled, false);
assert.equal(disabledConfig.chainlinkRpcUrl, DEFAULT_CHAINLINK_RPC_URL);
assert.deepEqual(disabledConfig.chainlinkFallbackRpcUrls, DEFAULT_CHAINLINK_FALLBACK_RPC_URLS);
assert.equal(disabledConfig.upstreamProxyUrl, undefined);

console.log("config-check ok");
