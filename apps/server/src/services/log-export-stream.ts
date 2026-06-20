import { Readable } from "node:stream";

export function createJsonlStream(records: Iterable<unknown>) {
  return Readable.from((function* jsonlRows() {
    for (const record of records) {
      yield `${JSON.stringify(record)}\n`;
    }
  })());
}
