// Mirrors the JSON shapes UiGateway::*_to_json()/handle_*() produce in
// mdh/src/ui_gateway/ui_gateway.cpp -- kept as one file, by hand, rather
// than generated, because the wire surface is small and stable (this is
// the one and only client of it). If a field is added on the C++ side,
// add it here too; nothing enforces the two stay in sync automatically.

export type Side = 'Buy' | 'Sell';
export type TimeInForce = 'GTC' | 'IOC' | 'FOK';

// trader::oms::to_string(OrderState) -- see order_management_system.hpp.
export type OrderState =
  | 'PendingNew'
  | 'Live'
  | 'PartiallyFilled'
  | 'Filled'
  | 'PendingCancel'
  | 'Cancelled'
  | 'PendingReplace'
  | 'Replaced'
  | 'Rejected';

export type PendingAction = 'None' | 'Cancel' | 'Replace';

export interface ClientOrder {
  account_id: number;
  client_order_id: number;
  exchange_order_id: number | null;
  instrument_id: number;
  side: Side;
  price: number;
  quantity: number;
  remaining_quantity: number;
  order_type: string;
  time_in_force: string;
  state: OrderState;
  pending_action: PendingAction;
  last_reject_reason: string;
}

export interface Position {
  instrument_id: number;
  quantity: number;
}

export interface Account {
  account_id: number;
  cash: number;
  positions: Position[];
  orders: ClientOrder[];
}

export interface BookLevel {
  price: number;
  quantity: number;
  order_count: number;
}

export interface Book {
  instrument_id: number;
  bids: BookLevel[];
  asks: BookLevel[];
}

export interface SubmitOrderResult {
  accepted: boolean;
  client_order_id?: number;
  reject_reason?: string;
}

export interface CancelOrderResult {
  ok: boolean;
}

export interface ReplaceOrderResult {
  ok: boolean;
  new_client_order_id?: number;
}

// The two SSE event shapes UiGateway::broadcast_order()/broadcast_book()
// publish -- every "data: ..." payload on /api/stream is one of these,
// discriminated by `type`.
export type StreamEvent =
  | ({ type: 'order'; account_id: number; order: ClientOrder })
  | ({ type: 'book' } & Book);
