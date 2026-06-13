const http = require("node:http");

function normalizeProxyHeaders(headers, target, websocket = false) {
  const nextHeaders = { ...headers };
  for (const key of Object.keys(nextHeaders)) {
    const lowerKey = key.toLowerCase();
    if (lowerKey === "origin") {
      delete nextHeaders[key];
      continue;
    }
    if (["proxy-authenticate", "proxy-authorization", "proxy-connection"].includes(lowerKey)) {
      delete nextHeaders[key];
      continue;
    }
    if (!websocket && ["connection", "keep-alive", "te", "trailer", "transfer-encoding", "upgrade"].includes(lowerKey)) {
      delete nextHeaders[key];
    }
  }
  nextHeaders.host = target.host;
  return nextHeaders;
}

function forwardUpgradeResponse(targetSocket, upstreamResponse, upstreamHead) {
  const statusLine = `HTTP/${upstreamResponse.httpVersion} ${upstreamResponse.statusCode} ${upstreamResponse.statusMessage}\r\n`;
  const headerLines = [];
  for (let index = 0; index < upstreamResponse.rawHeaders.length; index += 2) {
    const name = upstreamResponse.rawHeaders[index];
    const value = upstreamResponse.rawHeaders[index + 1];
    headerLines.push(`${name}: ${value}\r\n`);
  }
  targetSocket.write(`${statusLine}${headerLines.join("")}\r\n`);
  if (upstreamHead.length > 0) {
    targetSocket.write(upstreamHead);
  }
}

function destroySocket(socket) {
  if (!socket.destroyed) {
    socket.destroy();
  }
}

async function startProductionProxyServer(input) {
  const target = new URL(input.targetUrl);
  if (target.protocol !== "http:") {
    throw new Error("Production local proxy currently expects an HTTP backend origin.");
  }

  const upgradedSockets = new Set();
  const trackUpgradedSocket = (socket) => {
    upgradedSockets.add(socket);
    socket.once("close", () => upgradedSockets.delete(socket));
  };
  const linkUpgradedSockets = (socket, upstreamSocket) => {
    trackUpgradedSocket(upstreamSocket);
    let closed = false;
    const closePair = () => {
      if (closed) {
        return;
      }
      closed = true;
      destroySocket(socket);
      destroySocket(upstreamSocket);
      upgradedSockets.delete(socket);
      upgradedSockets.delete(upstreamSocket);
    };
    socket.once("close", closePair);
    socket.once("end", closePair);
    socket.once("error", closePair);
    upstreamSocket.once("close", closePair);
    upstreamSocket.once("end", closePair);
    upstreamSocket.once("error", closePair);
  };

  const server = http.createServer((request, response) => {
    const upstream = http.request(
      {
        hostname: target.hostname,
        port: target.port || 80,
        method: request.method,
        path: request.url,
        headers: normalizeProxyHeaders(request.headers, target),
        localAddress: input.localAddress
      },
      (upstreamResponse) => {
        response.writeHead(upstreamResponse.statusCode || 502, upstreamResponse.headers);
        upstreamResponse.pipe(response);
      }
    );
    upstream.on("error", () => {
      if (!response.headersSent) {
        response.writeHead(502, { "content-type": "application/json" });
      }
      response.end(JSON.stringify({ ok: false, error: "production_proxy_unavailable" }));
    });
    request.pipe(upstream);
  });

  server.on("upgrade", (request, socket, head) => {
    trackUpgradedSocket(socket);
    const upstream = http.request({
      hostname: target.hostname,
      port: target.port || 80,
      method: request.method,
      path: request.url,
      headers: normalizeProxyHeaders(request.headers, target, true),
      localAddress: input.localAddress
    });
    socket.once("close", () => upstream.destroy());
    socket.once("error", () => upstream.destroy());

    upstream.on("upgrade", (upstreamResponse, upstreamSocket, upstreamHead) => {
      forwardUpgradeResponse(socket, upstreamResponse, upstreamHead);
      if (head.length > 0) {
        upstreamSocket.write(head);
      }
      upstreamSocket.pipe(socket);
      socket.pipe(upstreamSocket);
      linkUpgradedSockets(socket, upstreamSocket);
    });

    upstream.on("response", (upstreamResponse) => {
      upstreamResponse.resume();
      upgradedSockets.delete(socket);
      socket.end(`HTTP/${upstreamResponse.httpVersion} ${upstreamResponse.statusCode} ${upstreamResponse.statusMessage}\r\n\r\n`);
    });
    upstream.on("error", () => {
      upgradedSockets.delete(socket);
      destroySocket(socket);
    });
    upstream.end();
  });

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(input.port ?? 0, input.host ?? "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });

  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("Production local proxy failed to bind to a TCP port.");
  }

  return {
    server,
    port: address.port,
    close: () => {
      for (const socket of Array.from(upgradedSockets)) {
        destroySocket(socket);
      }
      return new Promise((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    }
  };
}

module.exports = {
  normalizeProxyHeaders,
  startProductionProxyServer
};
