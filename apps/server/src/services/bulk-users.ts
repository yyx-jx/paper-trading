import type { Language, PermissionLevel, Role, PublicUser } from "../domain/types";

export interface BulkCreateUserInput {
  username: string;
  password: string;
  displayName?: string;
  role?: Role;
  language?: Language;
  seniorTesterId?: string;
  managerUserId?: string;
  permissionLevel?: PermissionLevel;
  mustChangePassword?: boolean;
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
  managerUserId?: string;
  permissionLevel: PermissionLevel;
  mustChangePassword: boolean;
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

export const CSV_BULK_USER_TEMPLATE =
  "username,password,displayName,role,language,managerUsername,availableUsdc,permissionLevel,mustChangePassword\n" +
  "# username: required unique login name; password: required initial password; displayName: optional display name\n" +
  "# role: Tester/Senior Tester/Test Engineer/Admin; language: zh-CN/en-US; managerUsername: required only when a Tester belongs to a Senior Tester or Test Engineer\n" +
  "# availableUsdc: optional non-negative number; permissionLevel: Initial/Standard; mustChangePassword: true/false\n" +
  "alice,ChangeMe123,Alice Tester,Tester,zh-CN,senior01,10000,Standard,true\n";

const ROLES = new Set<Role>(["Tester", "Senior Tester", "Test Engineer", "Admin"]);
const LANGUAGES = new Set<Language>(["zh-CN", "en-US"]);
const PERMISSION_LEVELS = new Set<PermissionLevel>(["Initial", "Standard"]);

function parseCsvRows(text: string) {
  const delimiter = text.split("\t").length > text.split(",").length ? "\t" : ",";
  const rows: string[][] = [];
  let row: string[] = [];
  let cell = "";
  let quoted = false;
  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];
    if (quoted) {
      if (char === '"' && next === '"') {
        cell += '"';
        index += 1;
      } else if (char === '"') {
        quoted = false;
      } else {
        cell += char;
      }
      continue;
    }
    if (char === '"') {
      quoted = true;
    } else if (char === delimiter) {
      row.push(cell);
      cell = "";
    } else if (char === "\n") {
      row.push(cell);
      rows.push(row);
      row = [];
      cell = "";
    } else if (char !== "\r") {
      cell += char;
    }
  }
  if (cell || row.length > 0) {
    row.push(cell);
    rows.push(row);
  }
  return rows;
}

function parseBoolean(value: string | undefined) {
  const normalized = value?.trim().toLowerCase();
  if (!normalized) return undefined;
  if (["true", "1", "yes", "y"].includes(normalized)) return true;
  if (["false", "0", "no", "n"].includes(normalized)) return false;
  return undefined;
}

function looksLikeHeaderRow(row: string[]) {
  const normalized = row.map((cell) => cell.trim().toLowerCase());
  return normalized[0] === "username" && normalized[1] === "password";
}

function looksLikeTemplateDescription(row: string[]) {
  const first = row[0]?.trim().toLowerCase() ?? "";
  return first.startsWith("#") || first.startsWith("//");
}


export function parseBulkUsersCsv(
  text: string,
  context: {
    findManagerByUsername: (username: string) => PublicUser | undefined;
  }
) {
  const failed: BulkUserValidationFailure[] = [];
  const rawRows = parseCsvRows(text.replace(/^\uFEFF/, "")).filter((row) => row.some((cell) => cell.trim()));
  const rows = rawRows.filter((row) => !looksLikeHeaderRow(row) && !looksLikeTemplateDescription(row));
  if (rows.length > 100) {
    failed.push({ rowNumber: 0, error: "CSV import supports at most 100 users." });
  }
  const users = rows.slice(0, 100).map((row, index) => {
    const rowNumber = index + 1;
    const username = row[0]?.trim() ?? "";
    const password = row[1] ?? "";
    const displayName = row[2]?.trim() || undefined;
    const roleText = row[3]?.trim() || "Tester";
    const languageText = row[4]?.trim() || "zh-CN";
    const managerUsername = row[5]?.trim();
    const availableUsdcText = row[6]?.trim();
    const permissionLevelText = row[7]?.trim() || "Standard";
    const mustChangePassword = parseBoolean(row[8]);
    const role = ROLES.has(roleText as Role) ? (roleText as Role) : undefined;
    const language = LANGUAGES.has(languageText as Language) ? (languageText as Language) : undefined;
    const permissionLevel = PERMISSION_LEVELS.has(permissionLevelText as PermissionLevel)
      ? (permissionLevelText as PermissionLevel)
      : undefined;
    const manager = managerUsername ? context.findManagerByUsername(managerUsername) : undefined;

    if (roleText && !role) {
      failed.push({ rowNumber, username, error: `Invalid role: ${roleText}.` });
    }
    if (languageText && !language) {
      failed.push({ rowNumber, username, error: `Invalid language: ${languageText}.` });
    }
    if (permissionLevelText && !permissionLevel) {
      failed.push({ rowNumber, username, error: `Invalid permissionLevel: ${permissionLevelText}.` });
    }
    if (managerUsername && !manager) {
      failed.push({ rowNumber, username, error: `managerUsername was not found: ${managerUsername}.` });
    }
    const availableUsdc = availableUsdcText ? Number(availableUsdcText) : undefined;
    if (availableUsdcText && (!Number.isFinite(availableUsdc) || Number(availableUsdc) < 0)) {
      failed.push({ rowNumber, username, error: "availableUsdc must be a non-negative number." });
    }

    return {
      username,
      password,
      displayName,
      role,
      language,
      seniorTesterId: manager?.id,
      managerUserId: manager?.id,
      permissionLevel,
      mustChangePassword,
      availableUsdc
    } satisfies BulkCreateUserInput;
  });

  return { users, failed, total: rows.length };
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
    const seniorTesterId = row.managerUserId?.trim() || row.seniorTesterId?.trim() || undefined;
    const permissionLevel = (row.permissionLevel ?? "Standard") as PermissionLevel;
    const mustChangePassword = row.mustChangePassword ?? false;

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
    if (!PERMISSION_LEVELS.has(permissionLevel)) {
      failed.push({ rowNumber, username, error: "permissionLevel must be Initial or Standard." });
    }

    return {
      rowNumber,
      username,
      password: row.password,
      displayName,
      role,
      language,
      seniorTesterId: role === "Tester" ? seniorTesterId : undefined,
      managerUserId: role === "Tester" ? seniorTesterId : undefined,
      permissionLevel,
      mustChangePassword,
      availableUsdc
    };
  });

  return {
    normalized,
    failed
  };
}
