interface InstrumentSelectorProps {
  instrumentIds: number[];
  selectedInstrumentId: number;
  onSelect: (instrumentId: number) => void;
}

export function InstrumentSelector({ instrumentIds, selectedInstrumentId, onSelect }: InstrumentSelectorProps) {
  return (
    <div className="pill-group">
      {instrumentIds.map((id) => (
        <button
          key={id}
          className={`pill ${id === selectedInstrumentId ? 'pill-active' : ''}`}
          onClick={() => onSelect(id)}
        >
          Instrument {id}
        </button>
      ))}
    </div>
  );
}
