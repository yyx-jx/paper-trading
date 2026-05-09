import "dotenv/config";
import { randomBytes, randomUUID } from "node:crypto";
import { Pool } from "pg";
import { ROLE_PERMISSIONS } from "../apps/server/src/auth/permissions";
import { hashPassword } from "../apps/server/src/auth/password";

function argValue(name: string) {
  const prefix = `--${name}=`;
  const inline = process.argv.find((item) => item.startsWith(prefix));
  if (inline) {
    return inline.slice(prefix.length);
  }
  const index = process.argv.indexOf(`--${name}`);
  return index >= 0 ? process.argv[index + 1] : undefined;
}

const username = argValue("username") ?? process.env.ADMIN_USERNAME ?? "admin";
const displayName = argValue("display-name") ?? process.env.ADMIN_DISPLAY_NAME ?? "Admin";
const password = argValue("password") ?? process.env.ADMIN_PASSWORD ?? randomBytes(18).toString("base64url");
const databaseUrl = process.env.DATABASE_URL;

async function main() {
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required.");
  }
  if (username === "admin" && password === "admin123") {
    throw new Error("Refusing to create the default weak admin/admin123 account.");
  }

  const pool = new Pool({ connectionString: databaseUrl });
  try {
    const existing = await pool.query<{ id: string }>("SELECT id FROM users WHERE username = $1", [username]);
    if (existing.rowCount && existing.rows[0]) {
      console.log(`Admin user already exists: ${username} (${existing.rows[0].id})`);
      return;
    }

    const now = Date.now();
    const id = `u_admin_${randomUUID().replace(/-/g, "").slice(0, 16)}`;
    await pool.query(
      `
        INSERT INTO users (
          id, username, password, display_name, role, language, permission_codes, available_usdc,
          is_active, manager_user_id, permission_level, failed_login_count, must_change_password,
          password_changed_at, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, 'Admin', 'zh-CN', $5::jsonb, 10000, TRUE, NULL, 'Standard', 0, TRUE, $6, $6, $6)
      `,
      [id, username, hashPassword(password), displayName, JSON.stringify(ROLE_PERMISSIONS.Admin), now]
    );

    console.log(`Created Admin user: ${username}`);
    if (!process.env.ADMIN_PASSWORD && !argValue("password")) {
      console.log(`Generated one-time password: ${password}`);
      console.log("Store this password now; it will not be shown again.");
    }
  } finally {
    await pool.end();
  }
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
