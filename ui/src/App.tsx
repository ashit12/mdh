import { AccountSelector } from './components/AccountSelector';
import { ActivityFeed } from './components/ActivityFeed';
import { InstrumentSelector } from './components/InstrumentSelector';
import { OrderBookPanel } from './components/OrderBookPanel';
import { OrderEntryForm } from './components/OrderEntryForm';
import { OrdersPanel } from './components/OrdersPanel';
import { PositionsPanel } from './components/PositionsPanel';
import { DEMO_INSTRUMENT_IDS, useDashboard } from './hooks/useDashboard';
import './App.css';

export default function App() {
  const {
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
    refreshAccount,
  } = useDashboard();

  return (
    <div className="app">
      <header className="app-header">
        <h1>mdh trading dashboard</h1>
        <span className="live-dot" title="connected to /api/stream" />
      </header>

      {error && (
        <div className="error-banner" onClick={() => setError(null)}>
          {error} (click to dismiss)
        </div>
      )}

      <div className="selector-row">
        <div>
          <span className="selector-label">Account</span>
          <AccountSelector accountIds={accountIds} selectedAccountId={selectedAccountId} onSelect={setSelectedAccountId} />
        </div>
        <div>
          <span className="selector-label">Instrument</span>
          <InstrumentSelector
            instrumentIds={DEMO_INSTRUMENT_IDS}
            selectedInstrumentId={selectedInstrumentId}
            onSelect={setSelectedInstrumentId}
          />
        </div>
      </div>

      <div className="dashboard-grid">
        <div className="column">
          <OrderEntryForm accountId={selectedAccountId} instrumentId={selectedInstrumentId} onSubmitted={refreshAccount} />
          <PositionsPanel account={account} />
        </div>
        <div className="column">
          <OrderBookPanel book={book} />
        </div>
        <div className="column">
          <OrdersPanel accountId={selectedAccountId} orders={account?.orders ?? []} onChanged={refreshAccount} />
        </div>
      </div>

      <ActivityFeed entries={activity} />
    </div>
  );
}
