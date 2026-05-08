import { appendFile, mkdir, stat, writeFile } from "node:fs/promises";
import path from "node:path";

export type AsyncJsonlWriterOptions = {
  flushIntervalMs?: number;
  batchSize?: number;
  maxFileBytes?: number;
  maxQueueDepth?: number;
  rotateByDate?: boolean;
  now?: () => Date;
};

const DEFAULT_MAX_FILE_BYTES = 50 * 1024 * 1024;
const DEFAULT_MAX_QUEUE_DEPTH = 10_000;

function dateKey(date: Date) {
  return date.toISOString().slice(0, 10);
}

function rotatedPath(filePath: string, day: string, sequence: number) {
  const parsed = path.parse(filePath);
  const suffix = sequence > 0 ? `-${String(sequence).padStart(4, "0")}` : "";
  return path.join(parsed.dir, `${parsed.name}-${day}${suffix}${parsed.ext}`);
}

async function fileSize(filePath: string) {
  try {
    return (await stat(filePath)).size;
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
      return 0;
    }
    throw error;
  }
}

export class AsyncJsonlWriter<T> {
  private queue: T[] = [];
  private flushTimer?: NodeJS.Timeout;
  private flushing?: Promise<void>;
  private currentFilePath = "";
  private currentFileDate = "";
  private currentFileSequence = 0;
  private currentFileBytes = 0;
  private currentFileInitialized = false;
  private flushCount = 0;
  private flushFailureCount = 0;
  private rotationCount = 0;
  private droppedRecordCount = 0;
  private lastFlushDurationMs = 0;
  private lastFlushAt?: number;
  private lastError?: string;
  private backlogSinceAt?: number;
  private closed = false;

  constructor(
    private readonly filePath: string,
    private readonly options: AsyncJsonlWriterOptions = {}
  ) {}

  write(row: T) {
    if (this.closed) {
      this.droppedRecordCount += 1;
      this.lastError = "writer is closed";
      return;
    }
    if (this.queue.length >= (this.options.maxQueueDepth ?? DEFAULT_MAX_QUEUE_DEPTH)) {
      this.droppedRecordCount += 1;
      this.backlogSinceAt ??= Date.now();
      return;
    }
    this.queue.push(row);
    if (this.queue.length >= (this.options.maxQueueDepth ?? DEFAULT_MAX_QUEUE_DEPTH)) {
      this.backlogSinceAt ??= Date.now();
    }
    if (this.queue.length >= (this.options.batchSize ?? 100)) {
      void this.flush();
      return;
    }
    this.flushTimer ??= setTimeout(() => {
      this.flushTimer = undefined;
      void this.flush();
    }, this.options.flushIntervalMs ?? 500);
  }

  async flush() {
    if (this.flushing) {
      await this.flushing;
    }
    const batch = this.queue.splice(0, this.queue.length);
    if (batch.length === 0) {
      if (this.queue.length === 0) {
        this.backlogSinceAt = undefined;
      }
      return;
    }
    const startedAt = Date.now();
    const payload = batch.map((row) => JSON.stringify(row)).join("\n") + "\n";
    this.flushing = this.ensureCurrentFile(Buffer.byteLength(payload, "utf8"))
      .then(() => appendFile(this.currentFilePath, payload, "utf8"))
      .then(() => {
        this.currentFileBytes += Buffer.byteLength(payload, "utf8");
      })
      .catch((error) => {
        this.flushFailureCount += 1;
        this.lastError = error instanceof Error ? error.message : String(error);
        console.warn(`[log-writer] failed to append ${this.currentFilePath || this.filePath}:`, error);
      })
      .finally(() => {
        this.flushCount += 1;
        this.lastFlushAt = Date.now();
        this.lastFlushDurationMs = Math.max(this.lastFlushAt - startedAt, 0);
        if (this.queue.length === 0) {
          this.backlogSinceAt = undefined;
        }
        this.flushing = undefined;
      });
    await this.flushing;
  }

  async rewrite(rows: T[]) {
    await this.flush();
    await this.ensureCurrentFile(0);
    const payload = rows.map((row) => JSON.stringify(row)).join("\n") + (rows.length ? "\n" : "");
    await writeFile(this.currentFilePath, payload, "utf8");
    this.currentFileBytes = Buffer.byteLength(payload, "utf8");
  }

  getStats() {
    const maxQueueDepth = this.options.maxQueueDepth ?? DEFAULT_MAX_QUEUE_DEPTH;
    return {
      queueDepth: this.queue.length,
      maxQueueDepth,
      flushCount: this.flushCount,
      flushFailureCount: this.flushFailureCount,
      rotationCount: this.rotationCount,
      droppedRecordCount: this.droppedRecordCount,
      backlog: this.queue.length >= maxQueueDepth,
      backlogSinceAt: this.backlogSinceAt,
      currentFilePath: this.currentFilePath || this.previewCurrentFilePath(),
      currentFileBytes: this.currentFileBytes,
      flushing: Boolean(this.flushing),
      lastFlushDurationMs: this.lastFlushDurationMs,
      lastFlushAt: this.lastFlushAt,
      lastError: this.lastError
    };
  }

  async close() {
    this.closed = true;
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = undefined;
    }
    await this.flush();
  }

  private previewCurrentFilePath() {
    if (this.options.rotateByDate === false) {
      return this.filePath;
    }
    return rotatedPath(this.filePath, dateKey(this.options.now?.() ?? new Date()), 0);
  }

  private async ensureCurrentFile(bytesToAppend: number) {
    await mkdir(path.dirname(this.filePath), { recursive: true });
    const shouldRotateByDate = this.options.rotateByDate !== false;
    const maxFileBytes = this.options.maxFileBytes ?? DEFAULT_MAX_FILE_BYTES;
    const day = shouldRotateByDate ? dateKey(this.options.now?.() ?? new Date()) : "";
    const firstPath = shouldRotateByDate ? rotatedPath(this.filePath, day, 0) : this.filePath;
    const previousPath = this.currentFilePath;

    if (!this.currentFileInitialized || this.currentFileDate !== day) {
      this.currentFileDate = day;
      this.currentFileSequence = 0;
      this.currentFilePath = firstPath;
      this.currentFileBytes = await fileSize(this.currentFilePath);
      this.currentFileInitialized = true;
      if (previousPath && previousPath !== this.currentFilePath) {
        this.rotationCount += 1;
      }
    }

    while (maxFileBytes > 0 && this.currentFileBytes > 0 && this.currentFileBytes + bytesToAppend > maxFileBytes) {
      this.currentFileSequence += 1;
      this.currentFilePath = shouldRotateByDate
        ? rotatedPath(this.filePath, day, this.currentFileSequence)
        : rotatedPath(this.filePath, "roll", this.currentFileSequence);
      this.currentFileBytes = await fileSize(this.currentFilePath);
      this.rotationCount += 1;
      if (this.currentFileBytes === 0 || this.currentFileBytes + bytesToAppend <= maxFileBytes) {
        break;
      }
    }
  }
}
