import type { UserRecord } from "../domain/types";
import { hasPermission } from "./permissions";

function isGroupManager(user: Pick<UserRecord, "role">) {
  return user.role === "Senior Tester" || user.role === "Test Engineer";
}

function managerIdFor(user: Pick<UserRecord, "managerUserId" | "seniorTesterId">) {
  return user.managerUserId ?? user.seniorTesterId;
}

function isDirectTesterForManager(actor: UserRecord, target: UserRecord) {
  return isGroupManager(actor) && target.role === "Tester" && managerIdFor(target) === actor.id;
}

export function managedUserIdsForActor(actor: UserRecord, users: UserRecord[]) {
  if (actor.role === "Admin") {
    return users.map((user) => user.id);
  }
  const managed = new Set<string>([actor.id]);
  if (isGroupManager(actor)) {
    for (const user of users) {
      if (isDirectTesterForManager(actor, user)) {
        managed.add(user.id);
      }
    }
  }
  return [...managed];
}

export function getVisibleUserIdsForActor(actor: UserRecord, users: UserRecord[]) {
  if (actor.role === "Admin") {
    return users.map((user) => user.id);
  }
  if (isGroupManager(actor)) {
    return managedUserIdsForActor(actor, users);
  }
  return [actor.id];
}

export function canViewUserRecords(actor: UserRecord, target: UserRecord, users: UserRecord[]) {
  return getVisibleUserIdsForActor(actor, users).includes(target.id);
}

export function canCreateUserForActor(actor: UserRecord, targetRole: UserRecord["role"]) {
  if (actor.role === "Admin") {
    return true;
  }
  return isGroupManager(actor) && targetRole === "Tester";
}

export function canChangeUserGroupForActor(actor: UserRecord, target: UserRecord) {
  return actor.role === "Admin" && target.role === "Tester";
}

export function canManageUser(actor: UserRecord, target: UserRecord, _users: UserRecord[]) {
  if (actor.role === "Admin") {
    return true;
  }
  if (actor.id === target.id) {
    return false;
  }
  return isDirectTesterForManager(actor, target);
}

export function canExportUser(actor: UserRecord, targetUserId: string, users: UserRecord[]) {
  const target = users.find((user) => user.id === targetUserId);
  return Boolean(target && canViewUserRecords(actor, target, users) && (hasPermission(actor, "data:export:self") || hasPermission(actor, "data:export:managed") || hasPermission(actor, "data:export:all")));
}
