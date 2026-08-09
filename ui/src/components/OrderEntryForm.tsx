import { useState } from 'react';
import { submitOrder } from '../api';
import { unitsToTicks } from '../format';
import type { Side } from '../types';

interface OrderEntryFormProps {
  accountId: number | null;
  instrumentId: number;
  onSubmitted: () => void;
}

export function OrderEntryForm({ accountId, instrumentId, onSubmitted }: OrderEntryFormProps) {
  const [side, setSide] = useState<Side>('Buy');
  const [price, setPrice] = useState('100.00');
  const [quantity, setQuantity] = useState('10');
  const [status, setStatus] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (accountId === null) return;
    setBusy(true);
    setStatus(null);
    try {
      const result = await submitOrder({
        account_id: accountId,
        instrument_id: instrumentId,
        side,
        price: unitsToTicks(Number(price)),
        quantity: Number(quantity),
      });
      if (result.accepted) {
        setStatus(`accepted -- client order #${result.client_order_id}`);
        onSubmitted();
      } else {
        setStatus(`rejected -- ${result.reject_reason}`);
      }
    } catch (err) {
      setStatus(`error -- ${String(err)}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <form className="panel order-entry" onSubmit={handleSubmit}>
      <h2>Order Entry</h2>
      <div className="side-toggle">
        <button
          type="button"
          className={`side-btn side-buy ${side === 'Buy' ? 'side-active' : ''}`}
          onClick={() => setSide('Buy')}
        >
          Buy
        </button>
        <button
          type="button"
          className={`side-btn side-sell ${side === 'Sell' ? 'side-active' : ''}`}
          onClick={() => setSide('Sell')}
        >
          Sell
        </button>
      </div>
      <label>
        Price
        <input type="number" step="0.01" min="0" value={price} onChange={(e) => setPrice(e.target.value)} required />
      </label>
      <label>
        Quantity
        <input type="number" step="1" min="1" value={quantity} onChange={(e) => setQuantity(e.target.value)} required />
      </label>
      <button type="submit" className={`submit-btn ${side === 'Buy' ? 'side-buy' : 'side-sell'}`} disabled={busy || accountId === null}>
        {busy ? 'Submitting...' : `${side} Instrument ${instrumentId}`}
      </button>
      {status && <p className="status-line">{status}</p>}
    </form>
  );
}
