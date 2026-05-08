import { ApiError } from "../http-errors";
import type { PermissionCode, UserRecord } from "../domain/types";
import { canManageUser, getVisibleUserIdsForActor } from "./scope";
import { hasPermission } from "./permissions";

export function assertCan(user: UserRecord, code: PermissionCode) {
  if (!hasPermission(user, code)) {
    throw new ApiError(403, `Missing permission: ${code}`, "PERMISSION_DENIED");
  }
}

export function assertCanAccessUser(actor: UserRecord, targetUserId: string, users: UserRecord[]) {
  if (!getVisibleUserIdsForActor(actor, users).includes(targetUserId)) {
    throw new ApiError(403, "Target user is outside your visible scope.", "PERMISSION_DENIED");
  }
}

export function assertCanManageUser(actor: UserRecord, target: UserRecord, users: UserRecord[]) {
  if (!canManageUser(actor, target, users)) {
    throw new ApiError(403, "Target user is outside your management scope.", "PERMISSION_DENIED");
  }
}
