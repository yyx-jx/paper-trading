export function addPendingOrderClientId(currentIds: readonly string[], clientOrderId: string) {
  return [clientOrderId, ...currentIds.filter((currentId) => currentId !== clientOrderId)];
}

export function removePendingOrderClientId(currentIds: readonly string[], clientOrderId: string) {
  return currentIds.filter((currentId) => currentId !== clientOrderId);
}

export function latestPendingOrderClientId(currentIds: readonly string[]) {
  return currentIds[0];
}
