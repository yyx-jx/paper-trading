import type { FastifyInstance } from "fastify";
import { z } from "zod";
import type { Language, UserRecord } from "../domain/types";
import type { MarketPayloadBuilder } from "../services/market-payloads";
import type { SimulationEngine } from "../services/simulation";
import type { AppStore } from "../services/store";
import type { UserPayloadBuilder } from "../services/user-payloads";

const loginSchema = z.object({
  username: z.string().min(1),
  password: z.string().min(1)
});

const wsTicketSchema = z.object({
  channel: z.enum(["market", "user"])
});

const languageSchema = z.object({
  language: z.enum(["zh-CN", "en-US"])
});

const selfProfileSchema = z.object({
  displayName: z.string().trim().min(1).optional(),
  language: z.enum(["zh-CN", "en-US"]).optional()
});

const changePasswordSchema = z.object({
  currentPassword: z.string().min(1),
  password: z.string().min(1),
  confirmPassword: z.string().min(1)
});

type LoginAuditInput = {
  username?: string;
  user?: UserRecord;
  success: boolean;
  serverRecvTs: number;
  resultMessage: string;
};

type UserManagementAuditInput = {
  actor: UserRecord;
  actionType:
    | "user.create"
    | "user.bulkCreate"
    | "user.update"
    | "user.disable"
    | "user.enable"
    | "user.resetPassword"
    | "user.changePassword"
    | "user.balance.set";
  success: boolean;
  serverRecvTs: number;
  targetUserId?: string;
  resultMessage: string;
  details?: Record<string, unknown>;
};

export function registerAuthMeRoutes(
  app: FastifyInstance,
  context: {
    store: AppStore;
    engine: SimulationEngine;
    marketPayloads: MarketPayloadBuilder;
    userPayloads: UserPayloadBuilder;
    safeRoute: <T>(handler: () => Promise<T>) => Promise<T>;
    getUserFromRequest: (request: { headers: Record<string, string | string[] | undefined> }) => UserRecord;
    requirePermission: (user: UserRecord, code: string) => void;
    signToken: (user: UserRecord) => string;
    createWsTicket: (user: UserRecord, channel: "market" | "user") => { ticket: string; expiresAt: number };
    recordLoginAudit: (input: LoginAuditInput) => Promise<void>;
    recordUserManagementAudit: (input: UserManagementAuditInput) => Promise<void>;
  }
) {
  const createBootstrapPayload = (user: UserRecord) => {
    const market = context.marketPayloads.createCurrentRoundPayload();
    return {
      ...market,
      history: context.marketPayloads.getHistoryWithSettlementPreview(60, user.id),
      me: context.store.sanitizeUser(user),
      ...context.userPayloads.createFullPayload(user),
      sourceStatus: user.permissionCodes.includes("system:status:view" as never) ? context.store.getSourceStatus() : []
    };
  };

  app.post("/api/auth/login", async (request, reply) => {
    const serverRecvTs = Date.now();
    const parsed = loginSchema.safeParse(request.body);
    if (!parsed.success) {
      const rawUsername = (request.body as { username?: unknown } | undefined)?.username;
      await context.recordLoginAudit({
        username: typeof rawUsername === "string" ? rawUsername : undefined,
        success: false,
        serverRecvTs,
        resultMessage: "Invalid login payload."
      });
      reply.code(400);
      return { message: "Invalid login payload.", code: "VALIDATION_FAILED" };
    }

    const candidate = context.store.findUserByUsername(parsed.data.username);
    if (candidate && context.store.isUserLocked(candidate)) {
      await context.recordLoginAudit({
        username: parsed.data.username,
        user: candidate,
        success: false,
        serverRecvTs,
        resultMessage: "User account is temporarily locked."
      });
      reply.code(423);
      return { message: "User account is temporarily locked.", code: "ACCOUNT_LOCKED" };
    }

    const user = context.store.findUserByCredentials(parsed.data.username, parsed.data.password);
    if (!user) {
      const disabledMatch = candidate && context.store.verifyUserPassword(candidate, parsed.data.password) && !candidate.isActive;
      if (candidate && !disabledMatch) {
        await context.store.recordFailedLogin(candidate);
      }
      await context.recordLoginAudit({
        username: parsed.data.username,
        user: disabledMatch ? candidate : undefined,
        success: false,
        serverRecvTs,
        resultMessage: disabledMatch ? "User account is disabled." : "Invalid username or password."
      });
      reply.code(disabledMatch ? 403 : 401);
      return {
        message: disabledMatch ? "User account is disabled." : "Invalid username or password.",
        code: disabledMatch ? "ACCOUNT_DISABLED" : "AUTH_FAILED"
      };
    }

    const token = context.signToken(user);
    await context.store.recordSuccessfulLogin(user);
    await context.recordLoginAudit({
      user,
      success: true,
      serverRecvTs,
      resultMessage: "Login succeeded."
    });
    return {
      token,
      user_id: user.id,
      role: user.role,
      language: user.language,
      display_name: user.displayName,
      permission_codes: user.permissionCodes,
      username: user.username,
      available_usdc: user.availableUsdc,
      is_active: user.isActive,
      senior_tester_id: user.seniorTesterId,
      manager_user_id: user.managerUserId ?? user.seniorTesterId,
      permission_level: user.permissionLevel ?? "Standard",
      created_at: user.createdAt,
      updated_at: user.updatedAt
    };
  });

  app.post("/api/ws/tickets", async (request) =>
    context.safeRoute(async () => {
      const user = context.getUserFromRequest(request);
      const parsed = wsTicketSchema.parse(request.body);
      return context.createWsTicket(user, parsed.channel);
    })
  );

  app.get("/api/me", async (request) =>
    context.safeRoute(async () => {
      const user = context.getUserFromRequest(request);
      return context.store.sanitizeUser(user);
    })
  );

  app.get("/api/bootstrap/full", async (request) =>
    context.safeRoute(async () => {
      const user = context.getUserFromRequest(request);
      context.requirePermission(user, "trade:view");
      context.requirePermission(user, "profile:view");
      return createBootstrapPayload(user);
    })
  );

  app.post("/api/me/language", async (request) =>
    context.safeRoute(async () => {
      const user = context.getUserFromRequest(request);
      const parsed = languageSchema.parse(request.body);
      await context.engine.updateLanguage(user, parsed.language as Language);
      context.store.emitUserPayload(user.id);
      return context.store.sanitizeUser(user);
    })
  );

  app.patch("/api/me", async (request) =>
    context.safeRoute(async () => {
      const user = context.getUserFromRequest(request);
      const serverRecvTs = Date.now();
      const parsed = selfProfileSchema.parse(request.body);
      const updated = await context.store.updateUserProfile(user.id, {
        displayName: parsed.displayName,
        language: parsed.language as Language | undefined
      });
      await context.recordUserManagementAudit({
        actor: user,
        actionType: "user.update",
        success: true,
        serverRecvTs,
        targetUserId: updated.id,
        resultMessage: "User profile was updated.",
        details: {
          username: updated.username,
          role: updated.role,
          selfService: true
        }
      });
      return context.store.sanitizeUser(updated);
    })
  );

  app.post("/api/me/password", async (request) =>
    context.safeRoute(async () => {
      const user = context.getUserFromRequest(request);
      const serverRecvTs = Date.now();
      const parsed = changePasswordSchema.parse(request.body);
      if (parsed.password !== parsed.confirmPassword) {
        throw new Error("Password confirmation does not match.");
      }
      const verifiedUser = context.store.findUserByCredentials(user.username, parsed.currentPassword);
      if (verifiedUser?.id !== user.id) {
        throw new Error("Current password is invalid.");
      }
      const updated = await context.store.resetUserPassword(user.id, parsed.password);
      await context.recordUserManagementAudit({
        actor: user,
        actionType: "user.changePassword",
        success: true,
        serverRecvTs,
        targetUserId: updated.id,
        resultMessage: "User password was changed.",
        details: {
          username: updated.username,
          role: updated.role
        }
      });
      return context.store.sanitizeUser(updated);
    })
  );
}
