export type UserConnectionCloseReason = "replaced" | "legacy_limit";

export type UserWsRegistryConnection = {
  id: string;
  actorId: string;
  viewedUserId: string;
  clientInstanceId?: string;
  openedAt: number;
  close(reason: UserConnectionCloseReason): void;
};

export type UserConnectionRegistration = {
  groupSize: number;
  replacedCount: number;
  evictedCount: number;
  unregister(): boolean;
};

type RegistryEntry = {
  connection: UserWsRegistryConnection;
  groupKey: string;
};

function createGroupKey(connection: Pick<UserWsRegistryConnection, "actorId" | "viewedUserId" | "clientInstanceId">) {
  const base = `${connection.actorId}:${connection.viewedUserId}`;
  return connection.clientInstanceId ? `${base}:${connection.clientInstanceId}` : `${base}:legacy`;
}

export function createUserConnectionRegistry(input?: { legacyLimit?: number }) {
  const legacyLimit = Math.max(1, input?.legacyLimit ?? 5);
  const entriesById = new Map<string, RegistryEntry>();
  const idsByGroup = new Map<string, Set<string>>();

  const removeById = (id: string) => {
    const entry = entriesById.get(id);
    if (!entry) {
      return undefined;
    }
    entriesById.delete(id);
    const group = idsByGroup.get(entry.groupKey);
    group?.delete(id);
    if (group && group.size === 0) {
      idsByGroup.delete(entry.groupKey);
    }
    return entry;
  };

  const register = (connection: UserWsRegistryConnection): UserConnectionRegistration => {
    const groupKey = createGroupKey(connection);
    const groupIds = idsByGroup.get(groupKey) ?? new Set<string>();
    const replaced: RegistryEntry[] = [];
    const evicted: RegistryEntry[] = [];

    if (connection.clientInstanceId) {
      for (const id of [...groupIds]) {
        const entry = removeById(id);
        if (entry) {
          replaced.push(entry);
        }
      }
    }

    entriesById.set(connection.id, { connection, groupKey });
    const nextGroupIds = idsByGroup.get(groupKey) ?? new Set<string>();
    nextGroupIds.add(connection.id);
    idsByGroup.set(groupKey, nextGroupIds);

    if (!connection.clientInstanceId && nextGroupIds.size > legacyLimit) {
      const sorted = [...nextGroupIds]
        .map((id) => entriesById.get(id))
        .filter((entry): entry is RegistryEntry => Boolean(entry))
        .sort((left, right) => left.connection.openedAt - right.connection.openedAt);
      while (sorted.length > legacyLimit) {
        const entry = sorted.shift();
        if (!entry) {
          break;
        }
        const removed = removeById(entry.connection.id);
        if (removed) {
          evicted.push(removed);
        }
      }
    }

    for (const entry of replaced) {
      entry.connection.close("replaced");
    }
    for (const entry of evicted) {
      entry.connection.close("legacy_limit");
    }

    return {
      groupSize: idsByGroup.get(groupKey)?.size ?? 0,
      replacedCount: replaced.length,
      evictedCount: evicted.length,
      unregister: () => Boolean(removeById(connection.id))
    };
  };

  return {
    register,
    size: () => entriesById.size,
    groupSize: (connection: Pick<UserWsRegistryConnection, "actorId" | "viewedUserId" | "clientInstanceId">) =>
      idsByGroup.get(createGroupKey(connection))?.size ?? 0
  };
}
