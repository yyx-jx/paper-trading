const URL_PATTERN = /\b(?:https?|wss?):\/\/[^\s<>"'`]+/gi;
const IPV4_WITH_OPTIONAL_PORT_PATTERN =
  /\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)(?::\d{1,5})?\b/g;

export function redactNetworkAddresses(value: unknown) {
  return String(value ?? "")
    .replace(URL_PATTERN, "[service address]")
    .replace(IPV4_WITH_OPTIONAL_PORT_PATTERN, "[service address]");
}

export function safeErrorMessage(error: unknown, fallback: string) {
  return redactNetworkAddresses(error instanceof Error ? error.message : fallback) || fallback;
}
