import type { UserRecord } from "../domain/types";
import { hasPermission } from "./permissions";

export function managedUserIdsForActor(actor: UserRecord, users: UserRecord[]) {
  if (hasPermission(actor, "logs:view:all") || hasPermission(actor, "data:export:all")) {
    return users.map((user) => user.id);
  }
  const managed = new Set<string>([actor.id]);
  let changed = true;
  while (changed) {
    changed = false;
    for (const user of users) {
      const managerId = user.managerUserId ?? user.seniorTesterId;
      if (managerId && managed.has(managerId) && !managed.has(user.id)) {
        managed.add(user.id);
        changed = true;
      }
    }
  }
  return [...managed];
}

export function getVisibleUserIdsForActor(actor: UserRecord, users: UserRecord[]) {
  if (actor.role === "Admin" || hasPermission(actor, "logs:view:all")) {
    return users.map((user) => user.id);
  }
  if (hasPermission(actor, "logs:view:managed") || hasPermission(actor, "logs:view:team")) {
    return managedUserIdsForActor(actor, users);
  }
  return [actor.id];
}

export function canManageUser(actor: UserRecord, target: UserRecord, users: UserRecord[]) {
  if (actor.role === "Admin") {
    return true;
  }
  if (actor.id === target.id) {
    return false;
  }
  return managedUserIdsForActor(actor, users).includes(target.id);
}

export function canExportUser(actor: UserRecord, targetUserId: string, users: UserRecord[]) {
  if (hasPermission(actor, "data:export:all")) {
    return true;
  }
  if (hasPermission(actor, "data:export:managed")) {
    return managedUserIdsForActor(actor, users).includes(targetUserId);
  }
  return hasPermission(actor, "data:export:self") && actor.id === targetUserId;
}
