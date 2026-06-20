import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import path from "node:path";
import {
  HyperBridge,
  buildHyperPaperFill,
  buildHyperSignal,
  loadHyperBridgeConfig,
  type HyperBridgeSocket
} from "../apps/server/src/services/hyper-bridge";
import type { OrderBookSnapshot, UserRecord } from "../apps/server/src/domain/types";
import { readSimulationServiceSource } from "./source-contracts";

class FakeSocket extends EventEmitter implements HyperBridgeSocket {
  readonly sent: string[] = [];
  send(payload: string) {
    this.sent.push(payload);
  }
  close() {
    this.emit("close");
  }
  terminate() {
    this.emit("close");
  }
}

const sampleUser: UserRecord = {
  id: "user_1",
  username: "tester",
  password: "hashed",
  displayName: "Tester",
  role: "Tester",
  language: "zh-CN",
  permissionCodes: [],
  availableUsdc: 1000,
  isActive: true,
  createdAt: 1,
  updatedAt: 1
};

const sampleBook: OrderBookSnapshot = {
  snapshotId: "book_1",
  snapshotTs: 1_000,
  bestBid: 0.49,
  bestAsk: 0.51,
  midPrice: 0.5,
  bids: [{ price: 0.49, qty: 10 }],
  asks: [{ price: 0.51, qty: 12 }]
};

function testConfig() {
  const defaults = loadHyperBridgeConfig({});
  assert.equal(defaults.enabled, false);
  assert.equal(defaults.gatewayUrl, "ws://127.0.0.1:8770");
  assert.equal(defaults.reconnectMs, 3000);
  assert.equal(defaults.maxQueue, 5000);
  assert.equal(defaults.defaultMode, "filtered");

  const enabled = loadHyperBridgeConfig({
    HYPER_BRIDGE_ENABLED: "true",
    HYPER_GATEWAY_URL: "wss://example.invalid/ws",
    HYPER_BRIDGE_RECONNECT_MS: "900",
    HYPER_BRIDGE_MAX_QUEUE: "2",
    HYPER_BRIDGE_MODE: "direct"
  });
  assert.equal(enabled.enabled, true);
  assert.equal(enabled.gatewayUrl, "wss://example.invalid/ws");
  assert.equal(enabled.reconnectMs, 900);
  assert.equal(enabled.maxQueue, 2);
  assert.equal(enabled.defaultMode, "direct");
}

function testPayloadBuilders() {
  const signal = buildHyperSignal(
    {
      traceId: "tr_1",
      orderId: "ord_1",
      user: sampleUser,
      payload: {
        action: "buy",
        side: "UP",
        amount: 25,
        orderKind: "market",
        clientOrderId: "client_1"
      },
      bookSnapshot: sampleBook,
      estimatedFee: 0.12,
      marketId: "market_1",
      emittedAt: 2_000
    },
    "filtered"
  );
  assert.equal(signal.signal_id, "tr_1");
  assert.equal(signal.client_order_id, "client_1");
  assert.equal(signal.hyper_user_id, "tester");
  assert.equal(signal.side, "BUY");
  assert.equal(signal.direction, "UP");
  assert.equal(signal.amount_unit, "USDC");
  assert.equal(signal.notional_usdc, 25);
  assert.equal(signal.token_id, null);
  assert.equal(signal.mode, "filtered");
  assert.deepEqual(signal.market_snapshot, sampleBook);

  const sellSignal = buildHyperSignal(
    {
      traceId: "tr_2",
      orderId: "ord_2",
      user: sampleUser,
      payload: {
        action: "sell",
        side: "DOWN",
        qty: 3,
        orderKind: "limit",
        limitPrice: 0.6
      },
      bookSnapshot: sampleBook,
      midPrice: 0.55,
      marketId: "market_1",
      emittedAt: 2_001
    },
    "direct"
  );
  assert.equal(sellSignal.side, "SELL");
  assert.equal(sellSignal.amount_unit, "SHARES");
  assert.equal(sellSignal.notional_usdc, 1.65);
  assert.equal(sellSignal.order_kind, "LIMIT");

  const fill = buildHyperPaperFill({
    traceId: "tr_1",
    fillPrice: 0.51,
    slippageBps: 5,
    status: "filled",
    feeUsdc: 0.12,
    filledQty: 49,
    partial: false,
    filledAt: 3_000
  });
  assert.equal(fill.signal_id, "tr_1");
  assert.equal(fill.paper_status, "filled");
  assert.equal(fill.paper_fill_price, 0.51);
  assert.equal(fill.paper_fee_usdc, 0.12);
}

function testBridgeQueueAndSend() {
  let socket: FakeSocket | undefined;
  const logs: string[] = [];
  const bridge = new HyperBridge(
    {
      enabled: true,
      gatewayUrl: "ws://gateway.invalid",
      reconnectMs: 10_000,
      maxQueue: 2,
      defaultMode: "filtered"
    },
    {
      info: (message) => logs.push(`info:${message}`),
      warn: (message) => logs.push(`warn:${message}`)
    },
    {
      createSocket: () => {
        socket = new FakeSocket();
        return socket;
      }
    }
  );

  bridge.onPaperFilled({
    traceId: "queued_1",
    fillPrice: 0.5,
    slippageBps: 1,
    status: "filled",
    feeUsdc: 0.1,
    filledQty: 10,
    partial: false,
    filledAt: 1
  });
  assert.equal(bridge.getStats().queueDepth, 1);

  bridge.start();
  assert.ok(socket);
  socket!.emit("open");
  assert.equal(bridge.getStats().queueDepth, 0);
  assert.equal(socket!.sent.length, 1);
  assert.equal(JSON.parse(socket!.sent[0]!).type, "paper_fill");

  socket!.emit("message", Buffer.from("{\"type\":\"ack\"}"));
  assert.equal(bridge.getStats().receivedMessagesIgnored, 1);

  bridge.stop();
  assert.equal(bridge.getStats().connected, false);
  assert.ok(logs.some((line) => line.includes("connected")));
}

function testDisabledBridgeDoesNotConnect() {
  let socketCreated = false;
  const bridge = new HyperBridge(
    {
      enabled: false,
      gatewayUrl: "ws://gateway.invalid",
      reconnectMs: 10_000,
      maxQueue: 2,
      defaultMode: "filtered"
    },
    { info: () => undefined, warn: () => undefined },
    {
      createSocket: () => {
        socketCreated = true;
        return new FakeSocket();
      }
    }
  );
  bridge.start();
  assert.equal(socketCreated, false);
  assert.equal(bridge.getStats().enabled, false);
}

function testWiringIsPresent() {
  const root = process.cwd();
  const simulationSource = readSimulationServiceSource();
  const indexSource = fs.readFileSync(path.join(root, "apps/server/src/index.ts"), "utf8");
  const envExample = fs.readFileSync(path.join(root, ".env.example"), "utf8");
  const productionEnvExample = fs.readFileSync(path.join(root, ".env.production.example"), "utf8");

  assert.match(simulationSource, /readonly events = new EventEmitter\(\)/);
  assert.match(simulationSource, /signal:emitted/);
  assert.match(simulationSource, /paper:filled/);
  assertInOrder(
    simulationSource,
    "const executionBook = await this.fetchExecutionBook(payload.side, currentRound);",
    'this.emitBridgeEvent("signal:emitted"',
    "signal emit happens after execution book fetch"
  );
  assertInOrder(
    simulationSource,
    'this.emitBridgeEvent("signal:emitted"',
    "const estimate = estimateClobExecution({",
    "signal emit happens before local paper matching"
  );
  assertInOrder(
    simulationSource,
    "await this.store.recordBehaviorLog(log);",
    'this.emitBridgeEvent("paper:filled"',
    "paper fill emit happens after behavior log persistence"
  );
  assert.match(simulationSource, /if \(process\.env\.HYPER_BRIDGE_ENABLED !== "true"\)/);
  assert.match(simulationSource, /estimatedFee: 0/);
  assert.match(indexSource, /loadHyperBridgeConfig/);
  assert.match(indexSource, /hyperBridge\.start\(\)/);
  assert.match(indexSource, /hyperBridge\.stop\(\)/);
  assert.match(envExample, /HYPER_BRIDGE_ENABLED=false/);
  assert.match(envExample, /HYPER_GATEWAY_URL=ws:\/\/16\.162\.106\.88:8770\?token=<BRIDGE_TOKEN>/);
  assert.match(productionEnvExample, /HYPER_BRIDGE_ENABLED=false/);
  assert.match(productionEnvExample, /HYPER_GATEWAY_URL=ws:\/\/16\.162\.106\.88:8770\?token=<BRIDGE_TOKEN>/);
}

function assertInOrder(source: string, first: string, second: string, label: string) {
  const firstIndex = source.indexOf(first);
  const secondIndex = source.indexOf(second);
  assert.notEqual(firstIndex, -1, `${label}: missing first marker ${first}`);
  assert.notEqual(secondIndex, -1, `${label}: missing second marker ${second}`);
  assert.ok(firstIndex < secondIndex, label);
}

testConfig();
testPayloadBuilders();
testBridgeQueueAndSend();
testDisabledBridgeDoesNotConnect();
testWiringIsPresent();

console.log("hyper-bridge-check ok");
