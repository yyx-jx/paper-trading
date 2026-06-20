import jwt from "jsonwebtoken";
import { nanoid } from "nanoid";
import { canViewUserRecords } from "../auth/scope";
import type { UserRecord } from "../domain/types";

export type WsChannel = "market" | "user";

export type WsSession = {
  actor: UserRecord;
  viewedUser: UserRecord;
  clientInstanceId?: string;
};

type WsTicketRecord = {
  userId: string;
  viewedUserId: string;
  channel: WsChannel;
  clientInstanceId?: string;
  expiresAt: number;
};

type WsSessionStore = {
  getUserById(userId: string): UserRecord | undefined;
  listUserRecords(): UserRecord[];
};

export type WsSessionManager = {
  readToken(raw?: string): string | undefined;
  getUserFromRequest(request: { headers: Record<string, string | string[] | undefined> }): UserRecord;
  readViewUserId(query: unknown): string | undefined;
  resolveViewedUser(actor: UserRecord, viewUserId?: string): UserRecord;
  getViewedUserFromRequest(actor: UserRecord, request: { query?: unknown }): UserRecord;
  createWsTicket(
    user: UserRecord,
    channel: WsChannel,
    viewUserId?: string,
    clientInstanceId?: string
  ): { ticket: string; expiresAt: number };
  consumeWsTicket(rawTicket: string | undefined, channel: WsChannel): WsSession | undefined;
  getWsSession(
    query: { token?: string; ticket?: string; viewUserId?: string; clientInstanceId?: string },
    channel: WsChannel
  ): WsSession | undefined;
};

export function normalizeClientInstanceId(raw: unknown) {
  if (typeof raw !== "string") {
    return undefined;
  }
  const value = raw.trim();
  return /^[A-Za-z0-9_-]{8,80}$/.test(value) ? value : undefined;
}

export function createWsSessionManager(input: {
  jwtSecret: string;
  store: WsSessionStore;
  ticketTtlMs?: number;
  tickets?: Map<string, WsTicketRecord>;
}): WsSessionManager {
  const ticketTtlMs = input.ticketTtlMs ?? 60_000;
  const tickets = input.tickets ?? new Map<string, WsTicketRecord>();

  const readToken = (raw?: string) => {
    if (!raw) {
      return undefined;
    }
    if (raw.startsWith("Bearer ")) {
      return raw.slice("Bearer ".length);
    }
    return raw;
  };

  const getUserFromRequest = (request: { headers: Record<string, string | string[] | undefined> }) => {
    const token = readToken(
      typeof request.headers.authorization === "string" ? request.headers.authorization : undefined
    );
    if (!token) {
      throw new Error("Missing authorization token.");
    }
    const payload = jwt.verify(token, input.jwtSecret) as { userId: string };
    const user = input.store.getUserById(payload.userId);
    if (!user) {
      throw new Error("User session is invalid.");
    }
    if (!user.isActive) {
      throw new Error("User account is disabled.");
    }
    return user;
  };

  const readViewUserId = (query: unknown) => {
    const raw = query && typeof query === "object" ? (query as { viewUserId?: unknown }).viewUserId : undefined;
    return typeof raw === "string" && raw.trim() ? raw.trim() : undefined;
  };

  const resolveViewedUser = (actor: UserRecord, viewUserId?: string) => {
    const target = viewUserId ? input.store.getUserById(viewUserId) : actor;
    if (!target) {
      throw new Error("Target user was not found.");
    }
    if (!canViewUserRecords(actor, target, input.store.listUserRecords())) {
      throw new Error("Records are not available for this user.");
    }
    return target;
  };

  const getViewedUserFromRequest = (actor: UserRecord, request: { query?: unknown }) =>
    resolveViewedUser(actor, readViewUserId(request.query));

  const createWsTicket = (
    user: UserRecord,
    channel: WsChannel,
    viewUserId?: string,
    clientInstanceId?: string
  ) => {
    const viewedUser = resolveViewedUser(user, viewUserId);
    const ticket = `wst_${nanoid(32)}`;
    const expiresAt = Date.now() + ticketTtlMs;
    tickets.set(ticket, {
      userId: user.id,
      viewedUserId: viewedUser.id,
      channel,
      clientInstanceId: normalizeClientInstanceId(clientInstanceId),
      expiresAt
    });
    return {
      ticket,
      expiresAt
    };
  };

  const consumeWsTicket = (rawTicket: string | undefined, channel: WsChannel): WsSession | undefined => {
    if (!rawTicket) {
      return undefined;
    }
    const ticket = tickets.get(rawTicket);
    tickets.delete(rawTicket);
    if (!ticket || ticket.channel !== channel || ticket.expiresAt < Date.now()) {
      return undefined;
    }
    const actor = input.store.getUserById(ticket.userId);
    if (!actor) {
      return undefined;
    }
    return {
      actor,
      viewedUser: resolveViewedUser(actor, ticket.viewedUserId),
      clientInstanceId: ticket.clientInstanceId
    };
  };

  const getWsSession = (
    query: { token?: string; ticket?: string; viewUserId?: string; clientInstanceId?: string },
    channel: WsChannel
  ): WsSession | undefined => {
    const ticketSession = consumeWsTicket(query.ticket, channel);
    if (ticketSession) {
      return ticketSession;
    }
    const token = readToken(query.token);
    if (!token) {
      return undefined;
    }
    const payload = jwt.verify(token, input.jwtSecret) as { userId: string };
    const actor = input.store.getUserById(payload.userId);
    if (!actor) {
      return undefined;
    }
    return {
      actor,
      viewedUser: resolveViewedUser(actor, query.viewUserId),
      clientInstanceId: normalizeClientInstanceId(query.clientInstanceId)
    };
  };

  return {
    readToken,
    getUserFromRequest,
    readViewUserId,
    resolveViewedUser,
    getViewedUserFromRequest,
    createWsTicket,
    consumeWsTicket,
    getWsSession
  };
}

export const createWsTicket = (manager: WsSessionManager) => manager.createWsTicket;
export const consumeWsTicket = (manager: WsSessionManager) => manager.consumeWsTicket;
