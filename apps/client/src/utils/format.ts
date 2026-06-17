import type { Language } from "./api";

export const money = (value = 0, digits = 2) =>
  new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  }).format(value);

export const decimal = (value = 0, digits = 2) => value.toFixed(digits);
export const signedMoney = (value = 0) => `${value >= 0 ? "+" : "-"}${money(Math.abs(value))}`;

export function pad2(value: number) {
  return String(value).padStart(2, "0");
}

export function utcParts(value: number) {
  const date = new Date(value);
  return {
    year: date.getUTCFullYear(),
    month: pad2(date.getUTCMonth() + 1),
    day: pad2(date.getUTCDate()),
    hour: pad2(date.getUTCHours()),
    minute: pad2(date.getUTCMinutes()),
    second: pad2(date.getUTCSeconds())
  };
}

export const timeText = (value?: number) => {
  if (!value) {
    return "--";
  }
  const { hour, minute, second } = utcParts(value);
  return `${hour}:${minute}:${second} UTC`;
};

export const dateTimeText = (value?: number) => {
  if (!value) {
    return "--";
  }
  const { year, month, day, hour, minute, second } = utcParts(value);
  return `${year}-${month}-${day} ${hour}:${minute}:${second} UTC`;
};

export function localLabel(language: Language, zh: string, en: string) {
  return language === "zh-CN" ? zh : en;
}

export function tokenPriceText(value?: number, digits = 1) {
  return `${decimal((value ?? 0) * 100, digits)}\u00A2`;
}

export function tradeDisplayPriceText(value?: number) {
  return `${Math.round((value ?? 0) * 100)}\u00A2`;
}
