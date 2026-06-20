const assert = require("node:assert/strict");
const http = require("node:http");
const { once } = require("node:events");
const WebSocket = require("ws");

const { startProductionProxyServer } = require("../apps/client/electron/production-proxy.cjs");

async function main() {
  const upstream = await startUpstreamServer();
  const proxy = await startProductionProxyServer({
    targetUrl: upstream.targetUrl,
    port: 0,
    host: "127.0.0.1"
  });

  try {
    await assertHttpOriginStripped(proxy.port);
    await assertWsUpgrade(proxy.port, upstream.state);
    await assertClientCloseClosesUpstream(proxy.port, upstream.state);
    await assertRepeatedClientCloseReleasesUpstream(proxy.port, upstream.state);
    await assertProxyCloseClosesUpstream(proxy, upstream.state);
    proxy.closed = true;
    console.log("production proxy websocket check passed");
  } finally {
    if (!proxy.closed) {
      await proxy.close();
    }
    await upstream.close();
  }
}

async function startUpstreamServer() {
  const state = {
    lastHttpOrigin: undefined,
    lastWsOrigin: undefined,
    wsServer: undefined,
    closedWsCount: 0
  };
  const server = http.createServer((request, response) => {
    state.lastHttpOrigin = request.headers.origin;
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ ok: true }));
  });
  const wsServer = new WebSocket.Server({ noServer: true });
  state.wsServer = wsServer;
  wsServer.on("connection", (socket, request) => {
    state.lastWsOrigin = request.headers.origin;
    socket.once("close", () => {
      state.closedWsCount += 1;
    });
    socket.send(JSON.stringify({ type: "market:tick", seq: 1 }));
  });
  server.on("upgrade", (request, socket, head) => {
    wsServer.handleUpgrade(request, socket, head, (upgraded) => {
      wsServer.emit("connection", upgraded, request);
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert(address && typeof address === "object");
  const close = async () => {
    wsServer.clients.forEach((client) => client.terminate());
    wsServer.close();
    await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  };
  return {
    state,
    targetUrl: `http://127.0.0.1:${address.port}`,
    close
  };
}

async function assertHttpOriginStripped(proxyPort) {
  const statusCode = await new Promise((resolve, reject) => {
    const request = http.get(
      {
        hostname: "127.0.0.1",
        port: proxyPort,
        path: "/api/health/live",
        headers: {
          Origin: "http://127.0.0.1:18787"
        }
      },
      (response) => {
        response.resume();
        resolve(response.statusCode);
      }
    );
    request.on("error", reject);
  });
  assert.equal(statusCode, 200);
}

async function assertWsUpgrade(proxyPort, state) {
  const socket = await openProxiedWebSocket(proxyPort);
  socket.terminate();

  assert.ok(state.lastHttpOrigin === undefined || state.lastHttpOrigin === "", "proxy should strip HTTP Origin");
  assert.ok(state.lastWsOrigin === undefined || state.lastWsOrigin === "", "proxy should strip WS Origin");
}

async function assertClientCloseClosesUpstream(proxyPort, state) {
  const closedBefore = state.closedWsCount;
  const socket = await openProxiedWebSocket(proxyPort);
  socket.close();
  await waitFor(() => state.closedWsCount > closedBefore, "upstream websocket should close after client close");
  await waitFor(() => state.wsServer.clients.size === 0, "upstream should have no websocket clients after client close");
}

async function assertRepeatedClientCloseReleasesUpstream(proxyPort, state) {
  for (let index = 0; index < 3; index += 1) {
    await assertClientCloseClosesUpstream(proxyPort, state);
  }
}

async function assertProxyCloseClosesUpstream(proxy, state) {
  const closedBefore = state.closedWsCount;
  const socket = await openProxiedWebSocket(proxy.port);
  assert.equal(state.wsServer.clients.size, 1);
  const closePromise = proxy.close();
  try {
    await withTimeout(closePromise, 1_000, "proxy.close should finish while upgraded websockets are still open");
  } catch (error) {
    socket.terminate();
    await closePromise.catch(() => undefined);
    proxy.closed = true;
    throw error;
  }
  await waitFor(() => state.closedWsCount > closedBefore, "upstream websocket should close when proxy closes");
  await waitFor(() => state.wsServer.clients.size === 0, "upstream should have no websocket clients after proxy close");
}

async function openProxiedWebSocket(proxyPort) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(`ws://127.0.0.1:${proxyPort}/ws/market`, {
      origin: "http://127.0.0.1:18787"
    });
    const timeout = setTimeout(() => {
      socket.terminate();
      reject(new Error("Timed out waiting for proxied websocket message."));
    }, 2_000);
    socket.once("message", (payload) => {
      clearTimeout(timeout);
      const parsed = JSON.parse(payload.toString());
      try {
        assert.equal(parsed.type, "market:tick");
        resolve(socket);
      } catch (error) {
        socket.terminate();
        reject(error);
      }
    });
    socket.once("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
  });
}

async function waitFor(predicate, message, timeoutMs = 1_000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.ok(predicate(), message);
}

async function withTimeout(promise, timeoutMs, message) {
  let timeout;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timeout = setTimeout(() => reject(new Error(message)), timeoutMs);
      })
    ]);
  } finally {
    clearTimeout(timeout);
  }
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.stack ?? error.message : error);
  process.exitCode = 1;
});
