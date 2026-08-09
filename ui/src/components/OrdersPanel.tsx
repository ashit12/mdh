import { useState } from 'react';
import { cancelOrder, replaceOrder } from '../api';
import { formatPrice, unitsToTicks } from '../format';
import type { ClientOrder } from '../types';

interface OrdersPanelProps {
  accountId: number | null;
  orders: ClientOrder[];
  onChanged: () => void;
}

// OrderManagementSystem::cancel_order()/replace_order() both require
// ClientOrderState::Live or ::PartiallyFilled (see is_live() in
// order_management_system.cpp) -- anything else (PendingNew,
// {Pending,}Cancel, {Pending,}Replace, Filled, Rejected) has no valid
// cancel/replace action, so those buttons are simply not rendered rather
// than rendered-then-rejected.
function isLive(order: ClientOrder): boolean {
  return order.state === 'Live' || order.state === 'PartiallyFilled';
}

function ReplaceRow({ accountId, order, onDone }: { accountId: number; order: ClientOrder; onDone: () => void }) {
  const [price, setPrice] = useState((order.price / 10_000).toString());
  const [quantity, setQuantity] = useState(order.remaining_quantity.toString());
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    setBusy(true);
    try {
      await replaceOrder(accountId, order.client_order_id, unitsToTicks(Number(price)), Number(quantity));
    } finally {
      setBusy(false);
      onDone();
    }
  };

  return (
    <tr className="replace-row">
      <td colSpan={7}>
        <input type="number" step="0.01" value={price} onChange={(e) => setPrice(e.target.value)} />
        <input type="number" step="1" value={quantity} onChange={(e) => setQuantity(e.target.value)} />
        <button onClick={submit} disabled={busy}>
          confirm
        </button>
        <button onClick={onDone} disabled={busy}>
          cancel
        </button>
      </td>
    </tr>
  );
}

export function OrdersPanel({ accountId, orders, onChanged }: OrdersPanelProps) {
  const [replacingId, setReplacingId] = useState<number | null>(null);

  const handleCancel = async (order: ClientOrder) => {
    if (accountId === null) return;
    await cancelOrder(accountId, order.client_order_id);
    onChanged();
  };

  return (
    <div className="panel">
      <h2>Orders</h2>
      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>Instr</th>
            <th>Side</th>
            <th className="num">Price</th>
            <th className="num">Qty left</th>
            <th>State</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {orders.length === 0 && (
            <tr>
              <td colSpan={7} className="muted empty">
                no orders yet
              </td>
            </tr>
          )}
          {orders.map((order) =>
            replacingId === order.client_order_id ? (
              <ReplaceRow
                key={order.client_order_id}
                accountId={accountId!}
                order={order}
                onDone={() => {
                  setReplacingId(null);
                  onChanged();
                }}
              />
            ) : (
              <tr key={order.client_order_id}>
                <td className="muted">{order.client_order_id}</td>
                <td>{order.instrument_id}</td>
                <td className={order.side === 'Buy' ? 'side-buy-text' : 'side-sell-text'}>{order.side}</td>
                <td className="num">{formatPrice(order.price)}</td>
                <td className="num">
                  {order.remaining_quantity}/{order.quantity}
                </td>
                <td>
                  <span className={`state-badge state-${order.state.toLowerCase()}`}>{order.state}</span>
                </td>
                <td className="actions">
                  {isLive(order) && (
                    <>
                      <button onClick={() => setReplacingId(order.client_order_id)}>replace</button>
                      <button onClick={() => handleCancel(order)}>cancel</button>
                    </>
                  )}
                </td>
              </tr>
            ),
          )}
        </tbody>
      </table>
    </div>
  );
}
