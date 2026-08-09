import type {
  Account,
  Book,
  CancelOrderResult,
  ReplaceOrderResult,
  Side,
  SubmitOrderResult,
  TimeInForce,
} from './types';

// Every path here is relative ("/api/...") on purpose: in dev it goes
// through vite.config.ts's proxy, in the built app it's already
// same-origin because trading_server serves ui/dist itself (see
// ui/README.md) -- neither case needs a base URL hardcoded here.

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(path);
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(body.error ?? `${res.status} ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: body === undefined ? '' : JSON.stringify(body),
  });
  if (!res.ok) {
    const parsed = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(parsed.error ?? `${res.status} ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

export function listAccounts(): Promise<{ account_ids: number[] }> {
  return getJson('/api/accounts');
}

export function getAccount(accountId: number): Promise<Account> {
  return getJson(`/api/accounts/${accountId}`);
}

export function getBook(instrumentId: number, depth = 10): Promise<Book> {
  return getJson(`/api/book/${instrumentId}?depth=${depth}`);
}

export interface SubmitOrderRequest {
  account_id: number;
  instrument_id: number;
  side: Side;
  price: number;
  quantity: number;
  time_in_force?: TimeInForce;
}

export function submitOrder(request: SubmitOrderRequest): Promise<SubmitOrderResult> {
  return postJson('/api/orders', request);
}

export function cancelOrder(accountId: number, clientOrderId: number): Promise<CancelOrderResult> {
  return postJson(`/api/orders/${accountId}/${clientOrderId}/cancel`, undefined);
}

export function replaceOrder(
  accountId: number,
  clientOrderId: number,
  newPrice: number,
  newQuantity: number,
): Promise<ReplaceOrderResult> {
  return postJson(`/api/orders/${accountId}/${clientOrderId}/replace`, {
    new_price: newPrice,
    new_quantity: newQuantity,
  });
}
