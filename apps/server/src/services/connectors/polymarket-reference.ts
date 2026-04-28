import { createProxyDispatcher, fetchJsonWithTimeout, fetchTextWithTimeout } from "./network";

const DEFAULT_STREAM_URL = "https://data.chain.link/streams/btc-usd-cexprice-streams";
const HISTORY_TIME_RANGE = "1D";
const HISTORY_CACHE_MS = 30_000;
const METADATA_CACHE_MS = 6 * 60 * 60_000;
const MAX_BOUNDARY_SKEW_MS = 60_000;

interface ChainlinkNextDataPayload {
  query?: {
    slug?: string;
  };
  props?: {
    pageProps?: {
      slug?: string;
      streamData?: {
        streamMetadata?: {
          feedId?: string;
          docs?: {
            schema?: string;
          };
          assetName?: string;
          pair?: string[];
        };
      };
    };
  };
}

interface ChainlinkHistoricalNode {
  bucket?: string;
  attributeName?: string;
  candlestick?: string;
}

interface ChainlinkHistoricalResponse {
  data?: {
    allStreamValuesGeneric1Minutes?: {
      nodes?: ChainlinkHistoricalNode[];
    };
  };
}

interface ChainlinkStreamMetadata {
  feedId: string;
  schema: string;
  streamSlug: string;
  streamUrl: string;
}

export interface ChainlinkReferenceSample {
  ts: number;
  value: number;
  kind: "open" | "close";
  bucket?: string;
}

export interface PolymarketReferenceResolution {
  price: number;
  ts: number;
  source: string;
}

function normalizeStreamUrl(url?: string) {
  const trimmed = url?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : DEFAULT_STREAM_URL;
}

function normalizeChainlinkTimestamp(raw: string) {
  const normalized = raw.trim().replace(" ", "T").replace(/([+-]\d{2})$/, "$1:00");
  const parsed = Date.parse(normalized);
  return Number.isFinite(parsed) ? parsed : undefined;
}

export function parseChainlinkStreamMetadataFromHtml(html: string, fallbackStreamUrl = DEFAULT_STREAM_URL) {
  const match = html.match(/<script id="__NEXT_DATA__" type="application\/json">([\s\S]*?)<\/script>/i);
  if (!match) {
    throw new Error("Chainlink stream page did not expose __NEXT_DATA__.");
  }
  const payload = JSON.parse(match[1]) as ChainlinkNextDataPayload;
  const streamMetadata = payload.props?.pageProps?.streamData?.streamMetadata;
  const feedId = streamMetadata?.feedId;
  const streamSlug = payload.props?.pageProps?.slug ?? payload.query?.slug;
  if (!feedId || !streamSlug) {
    throw new Error("Chainlink stream metadata was incomplete.");
  }
  return {
    feedId,
    schema: streamMetadata?.docs?.schema ?? "v3",
    streamSlug,
    streamUrl: normalizeStreamUrl(fallbackStreamUrl)
  } satisfies ChainlinkStreamMetadata;
}

export function parseChainlinkCandlestickSamples(candlestick?: string, bucket?: string) {
  if (!candlestick) {
    return [] as ChainlinkReferenceSample[];
  }
  const samples: ChainlinkReferenceSample[] = [];
  const matcher = /\b(open|close):\(ts:"([^"]+)",val:([0-9eE+.-]+)\)/g;
  for (const match of candlestick.matchAll(matcher)) {
    const kind = match[1] as ChainlinkReferenceSample["kind"];
    const ts = normalizeChainlinkTimestamp(match[2]);
    const value = Number(match[3]);
    if (!ts || !Number.isFinite(value) || value <= 1000) {
      continue;
    }
    samples.push({ ts, value, kind, bucket });
  }
  return samples.sort((left, right) => left.ts - right.ts);
}

export function pickFirstSampleAtOrAfter(
  samples: ChainlinkReferenceSample[],
  boundaryTs: number,
  maxSkewMs = MAX_BOUNDARY_SKEW_MS
) {
  return samples.find((sample) => sample.ts >= boundaryTs && sample.ts - boundaryTs <= maxSkewMs);
}

export class PolymarketReferenceResolver {
  private readonly proxyDispatcher;
  private metadataCache = new Map<string, { expiresAt: number; value: ChainlinkStreamMetadata }>();
  private historyCache = new Map<string, { fetchedAt: number; samples: ChainlinkReferenceSample[] }>();
  private metadataInFlight = new Map<string, Promise<ChainlinkStreamMetadata>>();
  private historyInFlight = new Map<string, Promise<ChainlinkReferenceSample[]>>();

  constructor(
    private readonly config: {
      requestTimeoutMs: number;
      upstreamProxyUrl?: string;
    }
  ) {
    this.proxyDispatcher = createProxyDispatcher(config.upstreamProxyUrl);
  }

  async resolveBoundaryPrice(
    boundaryTs: number,
    input?: {
      resolutionSource?: string;
      maxSkewMs?: number;
    }
  ): Promise<PolymarketReferenceResolution | undefined> {
    const metadata = await this.getStreamMetadata(input?.resolutionSource);
    const samples = await this.getHistoricalSamples(metadata);
    const sample = pickFirstSampleAtOrAfter(samples, boundaryTs, input?.maxSkewMs ?? MAX_BOUNDARY_SKEW_MS);
    if (!sample) {
      return undefined;
    }
    return {
      price: sample.value,
      ts: sample.ts,
      source: `Chainlink Data Streams ${metadata.streamSlug} ${sample.kind}`
    };
  }

  private async getStreamMetadata(resolutionSource?: string) {
    const streamUrl = normalizeStreamUrl(resolutionSource);
    const cached = this.metadataCache.get(streamUrl);
    if (cached && cached.expiresAt > Date.now()) {
      return cached.value;
    }
    const existing = this.metadataInFlight.get(streamUrl);
    if (existing) {
      return existing;
    }
    const request = (async () => {
      const html = await fetchTextWithTimeout(streamUrl, this.config.requestTimeoutMs, this.proxyDispatcher);
      const metadata = parseChainlinkStreamMetadataFromHtml(html, streamUrl);
      this.metadataCache.set(streamUrl, {
        value: metadata,
        expiresAt: Date.now() + METADATA_CACHE_MS
      });
      return metadata;
    })().finally(() => {
      this.metadataInFlight.delete(streamUrl);
    });
    this.metadataInFlight.set(streamUrl, request);
    return request;
  }

  private async getHistoricalSamples(metadata: ChainlinkStreamMetadata) {
    const cached = this.historyCache.get(metadata.feedId);
    if (cached && Date.now() - cached.fetchedAt < HISTORY_CACHE_MS) {
      return cached.samples;
    }
    const existing = this.historyInFlight.get(metadata.feedId);
    if (existing) {
      return existing;
    }
    const request = (async () => {
      const url =
        `https://data.chain.link/api/historical-data-engine-stream-data?feedId=${metadata.feedId}` +
        `&abiIndex=0&timeRange=${HISTORY_TIME_RANGE}`;
      const payload = await fetchJsonWithTimeout<ChainlinkHistoricalResponse>(
        url,
        this.config.requestTimeoutMs,
        this.proxyDispatcher
      );
      const nodes = payload.data?.allStreamValuesGeneric1Minutes?.nodes ?? [];
      const samples = nodes
        .filter((node) => node.attributeName === "benchmark")
        .flatMap((node) => parseChainlinkCandlestickSamples(node.candlestick, node.bucket))
        .sort((left, right) => left.ts - right.ts);
      this.historyCache.set(metadata.feedId, {
        fetchedAt: Date.now(),
        samples
      });
      return samples;
    })().finally(() => {
      this.historyInFlight.delete(metadata.feedId);
    });
    this.historyInFlight.set(metadata.feedId, request);
    return request;
  }
}
