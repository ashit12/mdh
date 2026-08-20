# mdh trading dashboard

A React + Vite + TypeScript single-page app for `ui_gateway::UiGateway`'s
REST + Server-Sent-Events API (see `../include/ui_gateway/ui_gateway.hpp`
and `../docs/exchange_flow.md`'s UI gateway section for the backend side of
this). Order book, order entry, positions/cash, open orders
(cancel/replace), and a live activity feed -- all kept in sync by one shared
`EventSource` connection to `/api/stream` plus the REST calls a newly
selected account/instrument needs to hydrate.

This app talks to exactly one backend, `trading_server`
(`../apps/trading_server/main.cpp`) — there is no mock/standalone mode.
Start that first.

## Development

```bash
# from mdh/, build once
cmake --build build --target trading_server
./build/trading_server   # tcp:7000, market-data udp:7001, http:8080 by default

# from mdh/ui/, in another terminal
npm install
npm run dev              # http://localhost:5173, proxies /api/* to :8080 (see vite.config.ts)
```

`npm run dev` gives you Vite's dev server (HMR, fast rebuilds); requests to
`/api/...` are proxied to `trading_server`'s `--http-port` (8080 by default
in both `main.cpp` and `vite.config.ts` -- pass `--http-port` to
`trading_server` and change `vite.config.ts`'s `server.proxy` target
together if you need a different port).

## Production build

```bash
npm run build             # emits ui/dist
../build/trading_server --static-dir ../ui/dist
```

With `--static-dir` set, `trading_server` serves the built dashboard itself
at `/` (via cpp-httplib's `set_mount_point`, see `UiGatewayOptions::
static_files_dir`) alongside its own `/api/*` routes -- one process, one
port, no separate web server, no dev proxy needed since everything is
already same-origin.

## Demo accounts and instruments

`UiGatewayOptions`' defaults (`../include/ui_gateway/ui_gateway.hpp`) are a
fixed catalog of three pre-seeded demo accounts (`1001`, `1002`, `1003`) and
two instruments (`1`, `2`), each account starting with the same cash and
starting position on both instruments. This dashboard's account/instrument
selectors are just quick-select buttons over that same fixed list (see
`DEMO_INSTRUMENT_IDS` in `src/hooks/useDashboard.ts`) -- there is
deliberately no "create account" flow; see `ui_gateway.hpp`'s own class
comment for why the account catalog is fixed rather than free-form (a
`Ledger` single-writer-thread constraint, not a UI limitation).

## Wire format / price scale

Every price and cash value on the wire is in raw ticks, 1 tick = 0.0001
currency unit (see `../include/common/types.hpp`'s comment on `Price`).
`src/format.ts` is the one place that scale is applied for display
(`formatMoney`/`formatPrice`) and reversed for submission
(`unitsToTicks`) -- the order-entry form lets you type "100.00" and submits
`1000000` ticks, matching what `GET /api/accounts/:id` and `GET
/api/book/:id` return.

## Project layout

- `src/types.ts` — hand-written TypeScript mirror of every JSON shape
  `UiGateway`'s handlers/`to_json()` helpers produce (see
  `src/ui_gateway/ui_gateway.cpp`). Not generated; the wire surface is small
  and this is its only client.
- `src/api.ts` — thin `fetch()` wrappers, one per REST endpoint.
- `src/hooks/useEventStream.ts` — one `EventSource` subscription to
  `/api/stream`, parsing each SSE payload's `type` discriminator.
- `src/hooks/useDashboard.ts` — the one hook owning all server-derived
  state: account catalog, selected account detail, selected instrument's
  book, and the activity feed, reconciling REST fetches with SSE pushes.
- `src/components/` — presentational components only; all data flows down
  from `useDashboard()` in `App.tsx`.
