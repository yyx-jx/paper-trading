import { z } from "zod";

export const USER_HISTORY_PAGE_SIZE = 25;
export const USER_HISTORY_PAGE_MAX_SIZE = 500;

export interface HistoryPageQuery {
  viewUserId?: string;
  limit: number;
  offset: number;
}

export interface PagedResult<T> {
  rows: T[];
  limit: number;
  offset: number;
  nextOffset?: number;
  hasMore: boolean;
}

const historyPageQuerySchema = z.object({
  viewUserId: z.string().optional(),
  limit: z.coerce.number().int().positive().max(USER_HISTORY_PAGE_MAX_SIZE).default(USER_HISTORY_PAGE_SIZE),
  offset: z.coerce.number().int().nonnegative().default(0)
});

export function normalizeHistoryPageQuery(query: unknown): HistoryPageQuery {
  return historyPageQuerySchema.parse(query);
}

export function buildPagedResult<T>(rowsWithLookahead: T[], page: Pick<HistoryPageQuery, "limit" | "offset">): PagedResult<T> {
  const rows = rowsWithLookahead.slice(0, page.limit);
  const hasMore = rowsWithLookahead.length > page.limit;
  return {
    rows,
    limit: page.limit,
    offset: page.offset,
    nextOffset: hasMore ? page.offset + page.limit : undefined,
    hasMore
  };
}
