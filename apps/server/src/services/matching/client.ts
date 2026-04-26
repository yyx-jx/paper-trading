import type {
  MatchingBookState,
  MatchingCancelResult,
  MatchingExecutionRequest,
  MatchingExecutionResult,
  MatchingReplayResult,
  MatchingSyncRequest
} from "../../domain/types";

async function requestJson<T>(
  url: string,
  init: RequestInit,
  timeoutMs: number
): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      ...init,
      signal: controller.signal,
      headers: {
        "content-type": "application/json",
        ...(init.headers ?? {})
      }
    });
    const payload = (await response.json()) as T & { error?: boolean; message?: string };
    if (!response.ok || payload.error) {
      throw new Error(payload.message ?? `Matching service request failed with status ${response.status}.`);
    }
    return payload;
  } finally {
    clearTimeout(timer);
  }
}

export class MatchingServiceClient {
  constructor(
    private readonly config: {
      baseUrl: string;
      timeoutMs: number;
    }
  ) {}

  async health() {
    return requestJson<{
      ok: boolean;
      serverNow: number;
      persistence: {
        postgres: boolean;
        redis: boolean;
      };
    }>(`${this.config.baseUrl}/health`, { method: "GET" }, this.config.timeoutMs);
  }

  async syncBook(request: MatchingSyncRequest) {
    return requestJson<{ book: MatchingBookState }>(
      `${this.config.baseUrl}/books/sync`,
      {
        method: "POST",
        body: JSON.stringify(request)
      },
      this.config.timeoutMs
    );
  }

  async execute(request: MatchingExecutionRequest) {
    return requestJson<MatchingExecutionResult>(
      `${this.config.baseUrl}/orders/execute`,
      {
        method: "POST",
        body: JSON.stringify(request)
      },
      this.config.timeoutMs
    );
  }

  async cancel(bookKey: string, orderId: string, bookSide: MatchingExecutionRequest["bookSide"], cancelledAt: number) {
    return requestJson<MatchingCancelResult>(
      `${this.config.baseUrl}/orders/${orderId}/cancel?bookKey=${encodeURIComponent(bookKey)}`,
      {
        method: "POST",
        body: JSON.stringify({
          bookSide,
          cancelledAt
        })
      },
      this.config.timeoutMs
    );
  }

  async getCurrentBook(bookKey: string) {
    return requestJson<{ book?: MatchingBookState }>(
      `${this.config.baseUrl}/books/${encodeURIComponent(bookKey)}/current`,
      {
        method: "GET"
      },
      this.config.timeoutMs
    );
  }

  async replay(
    bookKey: string,
    options?: { fromSequence?: number; toSequence?: number; limit?: number }
  ) {
    const query = new URLSearchParams();
    if (typeof options?.fromSequence === "number") {
      query.set("fromSequence", String(options.fromSequence));
    }
    if (typeof options?.toSequence === "number") {
      query.set("toSequence", String(options.toSequence));
    }
    if (typeof options?.limit === "number") {
      query.set("limit", String(options.limit));
    }
    return requestJson<MatchingReplayResult>(
      `${this.config.baseUrl}/books/${encodeURIComponent(bookKey)}/replay${
        query.size > 0 ? `?${query.toString()}` : ""
      }`,
      {
        method: "GET"
      },
      this.config.timeoutMs
    );
  }
}
