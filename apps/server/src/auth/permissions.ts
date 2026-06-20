import type { PermissionCode, Role, UserRecord } from "../domain/types";

export const ROLE_PERMISSIONS: Record<Role, PermissionCode[]> = {
  Tester: [
    "trade:view",
    "trade:order",
    "trade:cancel",
    "trade:sell",
    "profile:view",
    "profile:update",
    "profile:password:change",
    "logs:view:self"
  ],
  "Senior Tester": [
    "trade:view",
    "trade:order",
    "trade:cancel",
    "trade:sell",
    "profile:view",
    "profile:update",
    "profile:password:change",
    "users:list",
    "users:create",
    "users:disable",
    "users:enable",
    "users:reset-password",
    "users:update",
    "users:balance:set",
    "logs:view:team",
    "logs:view:managed",
    "data:export:managed",
    "quality:review"
  ],
  "Test Engineer": [
    "trade:view",
    "trade:order",
    "trade:cancel",
    "trade:sell",
    "profile:view",
    "profile:update",
    "profile:password:change",
    "system:status:view",
    "users:list",
    "users:create",
    "users:disable",
    "users:enable",
    "users:reset-password",
    "users:update",
    "users:balance:set",
    "users:permission-level:update",
    "logs:view:team",
    "logs:view:managed",
    "data:export:managed",
    "quality:review",
    "strategy:config"
  ],
  Admin: [
    "trade:view",
    "trade:order",
    "trade:cancel",
    "trade:sell",
    "profile:view",
    "profile:update",
    "profile:password:change",
    "system:status:view",
    "audit:view",
    "audit:export",
    "users:list",
    "users:create",
    "users:bulk-create",
    "users:update",
    "users:disable",
    "users:enable",
    "users:reset-password",
    "users:balance:set",
    "users:manager:update",
    "users:permission-level:update",
    "logs:view:all",
    "logs:view:team",
    "logs:view:managed",
    "data:export:all",
    "data:export:managed",
    "data:export:include-d",
    "quality:review",
    "strategy:config",
    "market:config",
    "settlement:manual"
  ]
};

export function normalizeRolePermissions(role: Role, existing: PermissionCode[] = []) {
  return [...new Set([...existing, ...ROLE_PERMISSIONS[role]])];
}

export function hasPermission(user: Pick<UserRecord, "permissionCodes" | "role">, code: PermissionCode) {
  return user.permissionCodes.includes(code) || ROLE_PERMISSIONS[user.role].includes(code);
}
