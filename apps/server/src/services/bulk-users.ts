import type { Language, Role } from "../domain/types";

export interface BulkCreateUserInput {
  username: string;
  password: string;
  displayName?: string;
  role?: Role;
  language?: Language;
  seniorTesterId?: string;
  availableUsdc?: number;
}

export interface NormalizedBulkUser {
  rowNumber: number;
  username: string;
  password: string;
  displayName: string;
  role: Role;
  language: Language;
  seniorTesterId?: string;
  availableUsdc: number;
}

export interface BulkUserValidationFailure {
  rowNumber: number;
  username?: string;
  error: string;
}

export interface BulkUserValidationContext {
  initialBalance: number;
  usernameExists: (username: string) => boolean;
  seniorTesterExists: (userId: string) => boolean;
}

export function validateBulkCreateUsers(rows: BulkCreateUserInput[], context: BulkUserValidationContext) {
  const seen = new Set<string>();
  const failed: BulkUserValidationFailure[] = [];
  const normalized = rows.map((row, index) => {
    const rowNumber = index + 1;
    const username = row.username.trim();
    const role = (row.role ?? "Tester") as Role;
    const displayName = row.displayName?.trim() || username;
    const language = (row.language ?? "zh-CN") as Language;
    const availableUsdc = typeof row.availableUsdc === "number" ? row.availableUsdc : context.initialBalance;
    const seniorTesterId = row.seniorTesterId?.trim() || undefined;

    if (!username) {
      failed.push({ rowNumber, error: "username is required." });
    }
    if (!row.password) {
      failed.push({ rowNumber, username, error: "password is required." });
    }
    if (seen.has(username)) {
      failed.push({ rowNumber, username, error: "Duplicate username in import batch." });
    }
    seen.add(username);
    if (context.usernameExists(username)) {
      failed.push({ rowNumber, username, error: "Username already exists." });
    }
    if (role === "Tester" && seniorTesterId && !context.seniorTesterExists(seniorTesterId)) {
      failed.push({ rowNumber, username, error: "seniorTesterId must point to a Senior Tester or Test Engineer." });
    }
    if (role !== "Tester" && seniorTesterId) {
      failed.push({ rowNumber, username, error: "seniorTesterId is only valid for Tester accounts." });
    }
    if (!Number.isFinite(availableUsdc) || availableUsdc < 0) {
      failed.push({ rowNumber, username, error: "availableUsdc must be a non-negative number." });
    }

    return {
      rowNumber,
      username,
      password: row.password,
      displayName,
      role,
      language,
      seniorTesterId: role === "Tester" ? seniorTesterId : undefined,
      availableUsdc
    };
  });

  return {
    normalized,
    failed
  };
}
