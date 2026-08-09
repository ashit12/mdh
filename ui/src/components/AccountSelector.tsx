interface AccountSelectorProps {
  accountIds: number[];
  selectedAccountId: number | null;
  onSelect: (accountId: number) => void;
}

export function AccountSelector({ accountIds, selectedAccountId, onSelect }: AccountSelectorProps) {
  return (
    <div className="pill-group">
      {accountIds.map((id) => (
        <button
          key={id}
          className={`pill ${id === selectedAccountId ? 'pill-active' : ''}`}
          onClick={() => onSelect(id)}
        >
          {id}
        </button>
      ))}
    </div>
  );
}
