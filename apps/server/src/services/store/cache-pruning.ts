export type CachePruneResult<T> = {
  retained: T[];
  trimmed: T[];
  retainedCount: number;
  trimmedCount: number;
};

export function trimArrayByCreatedAt<T>(
  records: T[],
  maxItems: number,
  getCreatedAt: (record: T) => number
): CachePruneResult<T> {
  if (records.length <= maxItems) {
    return {
      retained: records,
      trimmed: [],
      retainedCount: records.length,
      trimmedCount: 0
    };
  }

  const retained = [...records]
    .sort((left, right) => getCreatedAt(right) - getCreatedAt(left))
    .slice(0, Math.max(0, maxItems));
  const retainedSet = new Set(retained);
  const trimmed = records.filter((record) => !retainedSet.has(record));

  records.splice(0, records.length, ...retained);

  return {
    retained,
    trimmed,
    retainedCount: retained.length,
    trimmedCount: trimmed.length
  };
}
