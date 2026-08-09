import { useEffect, useRef } from 'react';
import type { StreamEvent } from '../types';

// One EventSource for the whole app (see App.tsx -- this hook is called
// exactly once, at the top level) subscribing to UiGateway's SSE hub. The
// backend's own hub is last-value-wins per topic (see SseHub's class
// comment in ui_gateway.cpp), not a guaranteed-delivery log, so this
// hook's job is purely "hand every payload that does arrive to the
// caller" -- reconciling/deduping state from those payloads is the
// caller's job (see useDashboard.ts), same division of responsibility the
// backend already documents for REST vs push.
export function useEventStream(onEvent: (event: StreamEvent) => void) {
  const onEventRef = useRef(onEvent);
  onEventRef.current = onEvent;

  useEffect(() => {
    const source = new EventSource('/api/stream');
    source.onmessage = (message) => {
      try {
        const parsed = JSON.parse(message.data) as StreamEvent;
        onEventRef.current(parsed);
      } catch {
        // Malformed/partial payload -- drop it, the next SSE frame (or a
        // REST poll elsewhere in the app) will catch the dashboard up.
      }
    };
    return () => source.close();
  }, []);
}
