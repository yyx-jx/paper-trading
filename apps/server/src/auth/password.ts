import bcrypt from "bcryptjs";

const BCRYPT_COST = 10;

export function isBcryptHash(value: string) {
  return /^\$2[aby]\$\d{2}\$/.test(value);
}

export function hashPassword(password: string) {
  return bcrypt.hashSync(password, BCRYPT_COST);
}

export function verifyPassword(password: string, stored: string) {
  return isBcryptHash(stored) ? bcrypt.compareSync(password, stored) : password === stored;
}
