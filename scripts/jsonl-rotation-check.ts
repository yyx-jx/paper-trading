import assert from "node:assert/strict";
import { mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { AsyncJsonlWriter } from "../apps/server/src/services/log-writer";

async function parseJsonlFile(filePath: string) {
  const text = await readFile(filePath, "utf8");
  return text
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

async function parseAllJsonl(dir: string) {
  const files = (await readdir(dir)).filter((name) => name.endsWith(".jsonl")).sort();
  const rows: Record<string, unknown>[] = [];
  for (const file of files) {
    rows.push(...(await parseJsonlFile(path.join(dir, file))));
  }
  return { files, rows };
}

async function main() {
  const tmp = await mkdtemp(path.join(tmpdir(), "btc-jsonl-rotation-"));
  try {
    let now = new Date("2026-05-07T00:00:00.000Z");
    const writer = new AsyncJsonlWriter(path.join(tmp, "audit-events.jsonl"), {
      batchSize: 1,
      flushIntervalMs: 60_000,
      maxFileBytes: 80,
      maxQueueDepth: 10,
      now: () => now
    });

    writer.write({ event: "first", payload: "x".repeat(20) });
    await writer.flush();
    writer.write({ event: "second", payload: "y".repeat(20) });
    await writer.flush();
    now = new Date("2026-05-08T00:00:00.000Z");
    writer.write({ event: "third", payload: "z".repeat(20) });
    await writer.close();

    const rotated = await parseAllJsonl(tmp);
    assert.ok(rotated.files.includes("audit-events-2026-05-07.jsonl"));
    assert.ok(rotated.files.some((file) => file.startsWith("audit-events-2026-05-07-0001")));
    assert.ok(rotated.files.includes("audit-events-2026-05-08.jsonl"));
    assert.deepEqual(
      rotated.rows.map((row) => row.event).sort(),
      ["first", "second", "third"]
    );
    assert.ok(writer.getStats().rotationCount >= 2);
    assert.equal(writer.getStats().queueDepth, 0);
    assert.equal(writer.getStats().flushing, false);

    const shutdownWriter = new AsyncJsonlWriter(path.join(tmp, "shutdown.jsonl"), {
      batchSize: 100,
      flushIntervalMs: 60_000,
      now: () => new Date("2026-05-07T00:00:00.000Z")
    });
    shutdownWriter.write({ event: "queued-before-close" });
    assert.equal(shutdownWriter.getStats().queueDepth, 1);
    await shutdownWriter.close();
    assert.equal(shutdownWriter.getStats().queueDepth, 0);
    const shutdownRows = await parseJsonlFile(path.join(tmp, "shutdown-2026-05-07.jsonl"));
    assert.equal(shutdownRows[0]?.event, "queued-before-close");

    const cappedWriter = new AsyncJsonlWriter(path.join(tmp, "capped.jsonl"), {
      batchSize: 100,
      flushIntervalMs: 60_000,
      maxQueueDepth: 2,
      now: () => new Date("2026-05-07T00:00:00.000Z")
    });
    cappedWriter.write({ event: "one" });
    cappedWriter.write({ event: "two" });
    cappedWriter.write({ event: "dropped" });
    assert.equal(cappedWriter.getStats().queueDepth, 2);
    assert.equal(cappedWriter.getStats().droppedRecordCount, 1);
    assert.equal(cappedWriter.getStats().backlog, true);
    await cappedWriter.close();
    const cappedRows = await parseJsonlFile(path.join(tmp, "capped-2026-05-07.jsonl"));
    assert.deepEqual(
      cappedRows.map((row) => row.event),
      ["one", "two"]
    );
  } finally {
    await rm(tmp, { recursive: true, force: true });
  }

  console.log("jsonl-rotation-check ok");
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
