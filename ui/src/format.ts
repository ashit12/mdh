// 1 tick = 0.0001 currency unit, fixed for this whole project -- see
// common/types.hpp's own comment on Price. Every price/cash value coming
// off the wire (REST or SSE) is in raw ticks; this is the one place that
// scale gets applied for display.
const TICKS_PER_UNIT = 10_000;

export function formatMoney(ticks: number): string {
  return (ticks / TICKS_PER_UNIT).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export function formatPrice(ticks: number): string {
  return (ticks / TICKS_PER_UNIT).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 });
}

export function unitsToTicks(units: number): number {
  return Math.round(units * TICKS_PER_UNIT);
}

export function formatTime(date: Date): string {
  return date.toLocaleTimeString(undefined, { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
}
