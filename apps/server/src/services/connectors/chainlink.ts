import { createPublicClient, http } from "viem";
import type { ChainlinkConnectorState, SourceHealth } from "../../domain/types";

// This connector polls AggregatorV3 over RPC. It is intentionally separate from
// any future Chainlink Data Streams client, which would require report fetch,
// decode, signature verification, and fallback orchestration.

const AGGREGATOR_V3_ABI = [
  {
    inputs: [],
    name: "decimals",
    outputs: [
      {
        internalType: "uint8",
        name: "",
        type: "uint8"
      }
    ],
    stateMutability: "view",
    type: "function"
  },
  {
    inputs: [],
    name: "latestRoundData",
    outputs: [
      {
        internalType: "uint80",
        name: "roundId",
        type: "uint80"
      },
      {
        internalType: "int256",
        name: "answer",
        type: "int256"
      },
      {
        internalType: "uint256",
        name: "startedAt",
        type: "uint256"
      },
      {
        internalType: "uint256",
        name: "updatedAt",
        type: "uint256"
      },
      {
        internalType: "uint80",
        name: "answeredInRound",
        type: "uint80"
      }
    ],
    stateMutability: "view",
    type: "function"
  }
] as const;

function emptyStatus(symbol: string): SourceHealth {
  const now = Date.now();
  return {
    source: "Chainlink",
    symbol,
    state: "reconnecting",
    reconnectCount: 0,
    sourceEventTs: now,
    serverRecvTs: now,
    normalizedTs: now,
    serverPublishTs: now,
    acquireLatencyMs: 0,
    publishLatencyMs: 0,
    frontendLatencyMs: 0,
    message: "Waiting for Chainlink feed."
  };
}

export class ChainlinkConnector {
  private timer?: NodeJS.Timeout;
  private reconnectCount = 0;
  private readonly listeners = new Set<(state: ChainlinkConnectorState) => void>();
  private readonly clients: Array<ReturnType<typeof createPublicClient>>;
  private readonly rpcUrls: string[];
  private currentClientIndex = 0;
  private readonly decimalsByRpc = new Map<string, number>();
  private lastSuccessTs = 0;
  private state: ChainlinkConnectorState;

  constructor(
    private readonly config: {
      symbol: string;
      rpcUrl: string;
      fallbackRpcUrls: string[];
      proxyAddress: `0x${string}`;
      pollMs: number;
      requestTimeoutMs: number;
    }
  ) {
    this.rpcUrls = [config.rpcUrl, ...config.fallbackRpcUrls].filter(Boolean);
    this.clients = this.rpcUrls.map((rpcUrl) =>
      createPublicClient({
        transport: http(rpcUrl, {
          retryCount: 0,
          timeout: config.requestTimeoutMs
        })
      })
    );
    this.state = {
      price: 0,
      updatedAt: 0,
      status: emptyStatus(config.symbol)
    };
  }

  start() {
    void this.poll();
    this.timer = setInterval(() => {
      void this.poll();
    }, this.config.pollMs);
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
  }

  subscribe(listener: (state: ChainlinkConnectorState) => void) {
    this.listeners.add(listener);
    listener(this.state);
    return () => {
      this.listeners.delete(listener);
    };
  }

  getState() {
    return this.state;
  }

  private emit() {
    for (const listener of this.listeners) {
      listener(this.state);
    }
  }

  private async poll() {
    const errors: string[] = [];
    for (let offset = 0; offset < this.clients.length; offset += 1) {
      const index = (this.currentClientIndex + offset) % this.clients.length;
      const client = this.clients[index];
      const rpcUrl = this.rpcUrls[index];
      try {
        const serverRecvTs = Date.now();
        let decimals = this.decimalsByRpc.get(rpcUrl);
        if (decimals === undefined) {
          decimals = Number(
            await client.readContract({
              address: this.config.proxyAddress,
              abi: AGGREGATOR_V3_ABI,
              functionName: "decimals"
            })
          );
          this.decimalsByRpc.set(rpcUrl, decimals);
        }

        const roundData = await client.readContract({
          address: this.config.proxyAddress,
          abi: AGGREGATOR_V3_ABI,
          functionName: "latestRoundData"
        });

        const answer = Number(roundData[1]) / 10 ** decimals;
        const updatedAt = Number(roundData[3]) * 1000;
        const now = Date.now();
        this.currentClientIndex = index;
        this.lastSuccessTs = now;
        this.state = {
          price: answer,
          updatedAt,
          status: {
            source: "Chainlink",
            symbol: this.config.symbol,
            state: "healthy",
            reconnectCount: this.reconnectCount,
            sourceEventTs: updatedAt,
            serverRecvTs,
            normalizedTs: now,
            serverPublishTs: now,
            acquireLatencyMs: Math.max(serverRecvTs - updatedAt, 0),
            publishLatencyMs: 0,
            frontendLatencyMs: 0,
            message: `Reading Chainlink BTC/USD feed via ${new URL(rpcUrl).host}.`
          }
        };
        this.emit();
        return;
      } catch (error) {
        errors.push(`${rpcUrl}: ${error instanceof Error ? error.message : "unknown error"}`);
      }
    }

    this.reconnectCount += 1;
    this.state = {
      ...this.state,
      status: {
        ...this.state.status,
        state: this.lastSuccessTs > 0 ? "degraded" : "reconnecting",
        reconnectCount: this.reconnectCount,
        message: errors.join(" | ") || "Failed to read Chainlink feed."
      }
    };
    this.emit();
  }
}
