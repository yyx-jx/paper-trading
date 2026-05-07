import type { FastifyReply } from "fastify";
import { z } from "zod";

export type ApiErrorResponse = {
  message: string;
  code?: string;
};

export class ApiError extends Error {
  readonly statusCode: number;
  readonly code?: string;

  constructor(statusCode: number, message: string, code?: string) {
    super(message);
    this.name = "ApiError";
    this.statusCode = statusCode;
    this.code = code;
  }
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function readMessage(error: unknown, fallback: string) {
  return error instanceof Error ? error.message : fallback;
}

export function mapApiError(error: unknown, fallbackMessage = "Unexpected server error."): ApiError {
  if (error instanceof ApiError) {
    return error;
  }
  if (error instanceof z.ZodError) {
    const first = error.issues[0];
    return new ApiError(400, first?.message ?? "Request validation failed.", "VALIDATION_FAILED");
  }

  const message = readMessage(error, fallbackMessage);
  const statusCode =
    isPlainObject(error) && typeof error.statusCode === "number" && error.statusCode >= 400
      ? error.statusCode
      : undefined;
  const code = isPlainObject(error) && typeof error.code === "string" ? error.code : undefined;
  if (statusCode) {
    return new ApiError(statusCode, message, code);
  }

  if (message.includes("persistent storage is unavailable") || message.includes("PostgreSQL is reconnecting")) {
    return new ApiError(503, message, "PERSISTENCE_UNAVAILABLE");
  }
  if (message.includes("Matching persistence is unavailable") || message.includes("Matching PostgreSQL persistence is unavailable")) {
    return new ApiError(503, message, "MATCHING_UNAVAILABLE");
  }
  if (message.includes("Missing authorization token") || message.includes("User session is invalid")) {
    return new ApiError(401, message, "AUTH_FAILED");
  }
  if (message.includes("User account is disabled")) {
    return new ApiError(403, message, "ACCOUNT_DISABLED");
  }
  if (message.includes("Missing permission") || message.includes("outside your management scope") || message.includes("not available for this user")) {
    return new ApiError(403, message, "PERMISSION_DENIED");
  }
  if (message.includes("was not found") || message.includes("Target user was not found") || message.includes("Order not found")) {
    return new ApiError(404, message, "RESOURCE_NOT_FOUND");
  }
  if (
    message.includes("confirmation does not match") ||
    message.includes("invalid") ||
    message.includes("insufficient") ||
    message.includes("require ") ||
    message.includes("cannot enter manual settlement")
  ) {
    return new ApiError(422, message, "TRADE_CONFLICT");
  }
  if (
    message.includes("cannot disable your own account") ||
    message.includes("already exists") ||
    message.includes("freeze") ||
    message.includes("Frozen") ||
    message.includes("Trading is closed")
  ) {
    return new ApiError(409, message, "TRADE_CONFLICT");
  }
  if (
    message.includes("required") ||
    message.includes("must point to") ||
    message.includes("validation failed") ||
    message.includes("bookKey is required")
  ) {
    return new ApiError(400, message, "VALIDATION_FAILED");
  }
  return new ApiError(500, message, "INTERNAL_ERROR");
}

export function sendApiError(reply: FastifyReply, error: unknown, fallbackMessage?: string) {
  const mapped = mapApiError(error, fallbackMessage);
  return reply.status(mapped.statusCode).send({
    message: mapped.message,
    ...(mapped.code ? { code: mapped.code } : {})
  } satisfies ApiErrorResponse);
}
