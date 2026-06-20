import { useCallback, useEffect, useState } from "react";
import {
  USER_HISTORY_PAGE_SIZE,
  api,
  type AuditEvent,
  type OrderLifecycleRecord,
  type OrderRecord
} from "../../utils/api";
import { redactNetworkAddresses } from "../../utils/redaction";

interface PagedUserHistoryInput {
  token?: string;
  viewedUserId?: string;
  ordersCount: number;
  orderLifecyclesCount: number;
  logsCount: number;
  appendUserHistory: (data: {
    orders?: OrderRecord[];
    orderLifecycles?: OrderLifecycleRecord[];
    logs?: AuditEvent[];
  }) => void;
  onError: (message: string) => void;
}

const initialHasMore = {
  orders: true,
  orderLifecycles: true,
  logs: true
};

export function usePagedUserHistory(input: PagedUserHistoryInput) {
  const {
    token,
    viewedUserId,
    ordersCount,
    orderLifecyclesCount,
    logsCount,
    appendUserHistory,
    onError
  } = input;
  const [hasMore, setHasMore] = useState(initialHasMore);
  const [tradeLoading, setTradeLoading] = useState(false);
  const [logsLoading, setLogsLoading] = useState(false);

  useEffect(() => {
    setHasMore(initialHasMore);
    setTradeLoading(false);
    setLogsLoading(false);
  }, [token, viewedUserId]);

  const canLoadTradeHistory =
    Boolean(token) &&
    !tradeLoading &&
    ((hasMore.orders && ordersCount >= USER_HISTORY_PAGE_SIZE) ||
      (hasMore.orderLifecycles && orderLifecyclesCount >= USER_HISTORY_PAGE_SIZE));
  const canLoadLogs =
    Boolean(token) &&
    !logsLoading &&
    hasMore.logs &&
    logsCount >= USER_HISTORY_PAGE_SIZE;

  const loadMoreTradeHistory = useCallback(async () => {
    if (!token || tradeLoading) {
      return;
    }
    setTradeLoading(true);
    try {
      const [ordersPage, lifecyclePage] = await Promise.all([
        hasMore.orders && ordersCount >= USER_HISTORY_PAGE_SIZE
          ? api.getOrdersPage(token, viewedUserId, {
            limit: USER_HISTORY_PAGE_SIZE,
            offset: ordersCount
          })
          : Promise.resolve(undefined),
        hasMore.orderLifecycles && orderLifecyclesCount >= USER_HISTORY_PAGE_SIZE
          ? api.getOrderLifecyclesPage(token, viewedUserId, {
            limit: USER_HISTORY_PAGE_SIZE,
            offset: orderLifecyclesCount
          })
          : Promise.resolve(undefined)
      ]);
      appendUserHistory({
        orders: ordersPage?.rows,
        orderLifecycles: lifecyclePage?.rows
      });
      setHasMore((current) => ({
        ...current,
        orders: ordersPage?.hasMore ?? current.orders,
        orderLifecycles: lifecyclePage?.hasMore ?? current.orderLifecycles
      }));
    } catch (error) {
      onError(error instanceof Error ? redactNetworkAddresses(error.message) : "Load more history failed.");
    } finally {
      setTradeLoading(false);
    }
  }, [
    appendUserHistory,
    hasMore.orders,
    hasMore.orderLifecycles,
    onError,
    orderLifecyclesCount,
    ordersCount,
    token,
    tradeLoading,
    viewedUserId
  ]);

  const loadMoreLogs = useCallback(async () => {
    if (!token || logsLoading || !hasMore.logs || logsCount < USER_HISTORY_PAGE_SIZE) {
      return;
    }
    setLogsLoading(true);
    try {
      const logsPage = await api.getLogsPage(token, viewedUserId, {
        limit: USER_HISTORY_PAGE_SIZE,
        offset: logsCount
      });
      appendUserHistory({ logs: logsPage.rows });
      setHasMore((current) => ({ ...current, logs: logsPage.hasMore }));
    } catch (error) {
      onError(error instanceof Error ? redactNetworkAddresses(error.message) : "Load more logs failed.");
    } finally {
      setLogsLoading(false);
    }
  }, [appendUserHistory, hasMore.logs, logsCount, logsLoading, onError, token, viewedUserId]);

  return {
    canLoadTradeHistory,
    canLoadLogs,
    tradeLoading,
    logsLoading,
    loadMoreTradeHistory,
    loadMoreLogs
  };
}
