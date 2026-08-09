import type { Account } from '../types';
import { formatMoney } from '../format';

interface PositionsPanelProps {
  account: Account | null;
}

export function PositionsPanel({ account }: PositionsPanelProps) {
  return (
    <div className="panel">
      <h2>Account</h2>
      {account === null ? (
        <p className="muted">loading...</p>
      ) : (
        <>
          <div className="cash-line">
            <span className="muted">Cash</span>
            <span className="num">{formatMoney(account.cash)}</span>
          </div>
          <table>
            <thead>
              <tr>
                <th>Instrument</th>
                <th className="num">Position</th>
              </tr>
            </thead>
            <tbody>
              {account.positions.map((position) => (
                <tr key={position.instrument_id}>
                  <td>{position.instrument_id}</td>
                  <td className={`num ${position.quantity >= 0 ? 'side-buy-text' : 'side-sell-text'}`}>
                    {position.quantity}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}
