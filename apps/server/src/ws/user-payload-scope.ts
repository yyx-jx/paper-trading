export type UserPayloadScope = "full" | "trade";

export type UserPayloadRequest =
  | { scope: "full" }
  | { scope: "trade"; positionIds?: string[] };

function uniqueIds(ids?: Iterable<string>) {
  if (!ids) {
    return undefined;
  }
  const values = [...new Set([...ids].filter((id) => typeof id === "string" && id.trim().length > 0))];
  return values.length > 0 ? values : [];
}

export function createUserPayloadRequest(scope: UserPayloadScope = "full", positionIds?: Iterable<string>): UserPayloadRequest {
  return scope === "full"
    ? { scope: "full" }
    : { scope: "trade", positionIds: uniqueIds(positionIds) };
}

export function mergeUserPayloadRequest(
  current: UserPayloadRequest | undefined,
  next: UserPayloadRequest
): UserPayloadRequest {
  if (!current) {
    return next;
  }
  if (current.scope === "full" || next.scope === "full") {
    return { scope: "full" };
  }
  return {
    scope: "trade",
    positionIds: uniqueIds([...(current.positionIds ?? []), ...(next.positionIds ?? [])])
  };
}
