import { formatTime } from '../format';
import type { ActivityEntry } from '../hooks/useDashboard';

interface ActivityFeedProps {
  entries: ActivityEntry[];
}

export function ActivityFeed({ entries }: ActivityFeedProps) {
  return (
    <div className="panel activity-feed">
      <h2>Activity</h2>
      <div className="activity-list">
        {entries.length === 0 && <p className="muted">waiting for exchange activity...</p>}
        {entries.map((entry) => (
          <div key={entry.id} className="activity-entry">
            <span className="muted activity-time">{formatTime(entry.at)}</span>
            <span>{entry.text}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
