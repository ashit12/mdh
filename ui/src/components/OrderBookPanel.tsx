import type { Book, BookLevel } from '../types';
import { formatPrice } from '../format';

interface OrderBookPanelProps {
  book: Book | null;
}

function BidRow({ level }: { level: BookLevel }) {
  return (
    <tr>
      <td className="num quantity">{level.quantity}</td>
      <td className="num price bid">{formatPrice(level.price)}</td>
      <td className="num muted">{level.order_count}</td>
    </tr>
  );
}

function AskRow({ level }: { level: BookLevel }) {
  return (
    <tr>
      <td className="num muted">{level.order_count}</td>
      <td className="num price ask">{formatPrice(level.price)}</td>
      <td className="num quantity">{level.quantity}</td>
    </tr>
  );
}

export function OrderBookPanel({ book }: OrderBookPanelProps) {
  const bids = book?.bids ?? [];
  const asks = [...(book?.asks ?? [])].reverse();

  return (
    <div className="panel">
      <h2>Order Book {book && <span className="muted">#{book.instrument_id}</span>}</h2>
      <div className="book-grid">
        <table className="book-side">
          <thead>
            <tr>
              <th className="num">Qty</th>
              <th className="num">Bid</th>
              <th className="num">#</th>
            </tr>
          </thead>
          <tbody>
            {bids.length === 0 && (
              <tr>
                <td colSpan={3} className="muted empty">
                  no bids
                </td>
              </tr>
            )}
            {bids.map((level) => (
              <BidRow key={`bid-${level.price}`} level={level} />
            ))}
          </tbody>
        </table>
        <table className="book-side">
          <thead>
            <tr>
              <th className="num">#</th>
              <th className="num">Ask</th>
              <th className="num">Qty</th>
            </tr>
          </thead>
          <tbody>
            {asks.length === 0 && (
              <tr>
                <td colSpan={3} className="muted empty">
                  no asks
                </td>
              </tr>
            )}
            {asks.map((level) => (
              <AskRow key={`ask-${level.price}`} level={level} />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
