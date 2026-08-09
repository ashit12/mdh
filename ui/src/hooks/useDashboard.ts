import { useCallback, useEffect, useRef, useState } from 'react';
import { getAccount, getBook, listAccounts } from '../api';
import { useEventStream } from './useEventStream';
import type { Account, Book, StreamEvent } from '../types';

export interface ActivityEntry {
  id: number;
  at: Date;
  text: string;
}

const MAX_ACTIVITY_ENTRIES = 200;

// Same fixed instrument catalog UiGatewayOptions::demo_instrument_ids
// defaults to (see ui_gateway.hpp) -- there is no "list instruments"
// endpoint because the backend has no static instrument catalog either
// (see that struct's own comment); this is just this dashboard's
// convenience list of instrument ids worth a quick-select button.
export const DEMO_INSTRUMENT_IDS = [1, 2];

// Single hook owning every piece of server-derived state the dashboard
// renders: the demo account catalog, the selected account's full detail,
// the selected instrument's book, and a running activity feed -- all kept
// fresh by one shared SSE connection (see useEventStream) plus the
// initial/on-selection-change REST fetches that hydrate state a stream
// alone can't (SSE only pushes deltas, never a full snapshot on its own
// for a *newly selected* account/instrument).
export function useDashboard() {
  const [accountIds, setAccountIds] = useState<number[]>([]);
  const [selectedAccountId, setSelectedAccountId] = useState<number | null>(null);
  const [selectedInstrumentId, setSelectedInstrumentId] = useState<number>(DEMO_INSTRUMENT_IDS[0]);
  const [account, setAccount] = useState<Account | null>(null);
  const [book, setBook] = useState<Book | null>(null);
  const [activity, setActivity] = useState<ActivityEntry[]>([]);
  const [error, setError] = useState<string | null>(null);
  const nextActivityId = useRef(0);

  const pushActivity = useCallback((text: string) => {
    setActivity((prev) => {
      const entry: ActivityEntry = { id: nextActivityId.current++, at: new Date(), text };
      const next = [entry, ...prev];
      return next.length > MAX_ACTIVITY_ENTRIES ? next.slice(0, MAX_ACTIVITY_ENTRIES) : next;
    });
  }, []);

  useEffect(() => {
    listAccounts()
      .then((res) => {
        setAccountIds(res.account_ids);
        setSelectedAccountId((current) => current ?? res.account_ids[0] ?? null);
      })
      .catch((err) => setError(String(err)));
  }, []);

  const refreshAccount = useCallback((accountId: number) => {
    getAccount(accountId)
      .then((next) => setAccount((prev) => (prev?.account_id === accountId || prev === null ? next : prev)))
      .catch((err) => setError(String(err)));
  }, []);

  useEffect(() => {
    if (selectedAccountId === null) return;
    setAccount(null);
    refreshAccount(selectedAccountId);
  }, [selectedAccountId, refreshAccount]);

  const refreshBook = useCallback((instrumentId: number) => {
    getBook(instrumentId)
      .then((next) => setBook(next))
      .catch((err) => setError(String(err)));
  }, []);

  useEffect(() => {
    setBook(null);
    refreshBook(selectedInstrumentId);
  }, [selectedInstrumentId, refreshBook]);

  useEventStream((event: StreamEvent) => {
    if (event.type === 'order') {
      pushActivity(
        `account ${event.account_id}: order #${event.order.client_order_id} ` +
          `${event.order.side} ${event.order.remaining_quantity}/${event.order.quantity} @ ${event.order.price} -> ${event.order.state}`,
      );
      // The event carries only the one changed order, not the account's
      // cash/positions (those live in the OMS's own local mirror, not on
      // the wire) -- re-pulling the whole account over REST is simpler
      // and just as cheap as reconciling deltas by hand, and guarantees
      // this view can never drift from what the backend actually has.
      if (event.account_id === selectedAccountId) {
        refreshAccount(event.account_id);
      }
    } else {
      const total_bid_qty = event.bids.reduce((sum, level) => sum + level.quantity, 0);
      const total_ask_qty = event.asks.reduce((sum, level) => sum + level.quantity, 0);
      pushActivity(`instrument ${event.instrument_id}: book update (${total_bid_qty} bid / ${total_ask_qty} ask qty)`);
      if (event.instrument_id === selectedInstrumentId) {
        setBook(event);
      }
    }
  });

  return {
    accountIds,
    selectedAccountId,
    setSelectedAccountId,
    selectedInstrumentId,
    setSelectedInstrumentId,
    account,
    book,
    activity,
    error,
    setError,
    refreshAccount: () => selectedAccountId !== null && refreshAccount(selectedAccountId),
  };
}
