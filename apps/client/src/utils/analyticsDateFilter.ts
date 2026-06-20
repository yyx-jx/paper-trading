import { utcParts } from "./format";

export type AnalyticsDateFilter =
  | { kind: "none" }
  | { kind: "year"; value: string }
  | { kind: "month"; value: string }
  | { kind: "day"; value: string };

export type AnalyticsDateQueryError = "year" | "month" | "day";

export function isValidAnalyticsYearQuery(value: string) {
  return /^\d{4}$/.test(value);
}

export function isValidAnalyticsMonthQuery(value: string) {
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(value)) {
    return false;
  }
  const [yearText, monthText] = value.split("-");
  const year = Number(yearText);
  const month = Number(monthText);
  return Number.isInteger(year) && Number.isInteger(month) && month >= 1 && month <= 12;
}

export function isValidAnalyticsDayQuery(value: string) {
  if (!/^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/.test(value)) {
    return false;
  }
  const [yearText, monthText, dayText] = value.split("-");
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const date = new Date(Date.UTC(year, month - 1, day));
  return (
    date.getUTCFullYear() === year &&
    date.getUTCMonth() === month - 1 &&
    date.getUTCDate() === day
  );
}

export function resolveAnalyticsDateFilter(input: { year: string; month: string; day: string }):
  | { filter: AnalyticsDateFilter }
  | { error: AnalyticsDateQueryError } {
  const day = input.day.trim();
  const month = input.month.trim();
  const year = input.year.trim();
  if (day) {
    return isValidAnalyticsDayQuery(day) ? { filter: { kind: "day", value: day } } : { error: "day" };
  }
  if (month) {
    return isValidAnalyticsMonthQuery(month) ? { filter: { kind: "month", value: month } } : { error: "month" };
  }
  if (year) {
    return isValidAnalyticsYearQuery(year) ? { filter: { kind: "year", value: year } } : { error: "year" };
  }
  return { filter: { kind: "none" } };
}

export function filterRowsByAnalyticsDate<T extends { ts: number }>(rows: T[], filter: AnalyticsDateFilter) {
  if (filter.kind === "none") {
    return rows;
  }
  return rows.filter((row) => {
    const parts = utcParts(row.ts);
    const year = String(parts.year);
    const month = `${year}-${parts.month}`;
    const day = `${month}-${parts.day}`;
    if (filter.kind === "year") {
      return year === filter.value;
    }
    if (filter.kind === "month") {
      return month === filter.value;
    }
    return day === filter.value;
  });
}
