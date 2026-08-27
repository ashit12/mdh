# End-to-End Architecture — Trader Side and Exchange Side

**Status:** everything described below is implemented and tested. See
`docs/exchange_flow.md` for the exchange-side walkthrough, `docs/benchmarks.md` for
performance, and `docs/failure_injection.md`/`docs/live_demo.md` for the hardening
proof and the live run.

---

## 1. The core rule this document exists to state

**There will be two order-book implementations, and they must never be merged into one
class just because both hold bids and asks:**

- **`book::OrderBook` (trader side)** reconstructs a view of exchange state
  *from published market-data events*. It never decides whether two orders cross; it only
  records that an add/cancel/modify *already happened* upstream.
- **`exchange::matching::MatchingBook`** is the *authoritative* book. It accepts
  inbound commands, decides whether they cross, mutates remaining quantity in place on a
  fill, and is the source of truth that market data is later derived *from*.

A trader-side book that is missing a market-data update is a **recoverable, expected**
condition (gap detection + snapshot recovery already exist for exactly this). An exchange
matching book with incorrect state is a **correctness failure of the authoritative system**.
These are different failure domains and different responsibilities, and keeping them in
separate classes/namespaces is what keeps that distinction enforceable in code rather than
just in prose.

---

## 2. System diagram

```
┌───────────────────────────────────── EXCHANGE SIDE ──────────────────────────────────────────┐
│                                                                                                │
│  Trading client                                                                               │
│       │ TCP binary order-entry protocol                                                       │
│       ▼                                                                                       │
│  Order-entry gateway ──────────────────────────────────────────  exchange/gateway/            │
│       │                                                                                       │
│       ▼                                                                                       │
│  Exchange validation + pre-trade risk ─────────────────────────  exchange/risk/               │
│       │                                                                                       │
│       ▼                                                                                       │
│  Command sequencer ─────────────────────────────────────────────  exchange/sequencing/        │
│       │  strictly increasing CommandSequence                                                  │
│       ▼                                                                                       │
│  SPSC queue (common::SpscQueue) ────────────────────────────────  common/spsc_queue.hpp       │
│       │                                                                                        │
│       ▼                                                                                        │
│  ┌─────────────────────────────────────────┐                                                   │
│  │  MATCHING ENGINE                         │  single-threaded, deterministic                  │
│  │  ExchangeCommand -> process() -> events  │  owns the AUTHORITATIVE book                     │
│  └─────────────────────────────────────────┘                                                   │
│       │                                                                                        │
│       ├──► Execution reports          (gateway return path)                                    │
│       ├──► Balance / ledger updates   (exchange/ledger/)                                       │
│       ├──► Market-data events         (exchange/market_data/) ─────────────────┐                │
│       └──► Command journal / replay   (exchange/persistence/)                  │                │
│                                                                                 │                │
└─────────────────────────────────────────────────────────────────────────────────┼────────────────┘
                                                                                  │
                                                                     UDP market-data feed
                                                                   (the same wire format the
                                                                    trader side already reads)
                                                                                  │
┌─────────────────────────────────────────────────────────────────────────────────┼────────────────┐
│                                    TRADER-FIRM SIDE                             │                │
│                                                                                  ▼                │
│  net::UdpReceiver / net::run_udp_listen()                                                         │
│       │  producer thread: receive_batch() → unpack_frames() → DroppingQueue.push()                │
│       ▼                                                                                           │
│  replay::apply_frame_result()                                                                     │
│       │  consumer thread: SequenceValidator classifies; on Missing + configured                    │
│       │  snapshot path, replay::read_snapshot() recovers instead of stopping                       │
│       ▼                                                                                            │
│  book::BookManager / book::OrderBook                                                               │
│       │  reconstructed, non-authoritative view — "what the exchange published so far"              │
│       ▼                                                                                            │
│  Strategy runtime ─────────────────────────────────────────────────  trader/strategies/            │
│       ▼                                                                                            │
│  Trader-side risk ─────────────────────────────────────────────────  trader/risk/                  │
│       ▼                                                                                            │
│  OMS ──────────────────────────────────────────────────────────────  trader/oms/                   │
│       ▼                                                                                            │
│  TCP order-entry client ────────────────────────────────────────────  trader/oms/ ─────────────────┘
│       (connects back up to the exchange gateway at the top of this diagram)
│
└───────────────────────────────────────────────────────────────────────────────────────────────────┘

                     Outside the latency-sensitive path on both halves, wired
                     into a real running process (apps/trading_server/main.cpp):
                     UI gateway (REST + Server-Sent Events) + React dashboard
                          │
                          ├──► listens to the same UDP market-data feed above, in its own
                          │    background thread, to reconstruct a live book::BookManager
                          └──► holds one TraderRiskGatedOms + OrderEntryClient per UI
                               account, connected back to the order-entry gateway over
                               real loopback TCP -- exactly like any other trader or
                               strategy, just driven by HTTP requests instead
```

Every box in the diagram is built and tested; `docs/exchange_flow.md` walks
through each in turn. The order-entry gateway really does sit in front of a
risk-gated matching engine, over a real TCP socket, and the trader side's OMS +
TCP order-entry client closes the loop back up into that same gateway (proven by
`tests/test_oms_gateway_e2e.cpp`), with trader-side positions/P&L and a second,
independent risk check on top (`trader::risk::TraderRiskGatedOms`, proven by
`tests/test_trader_risk_gated_oms_e2e.cpp`) and a strategy layer above that
(`trader::strategies::StrategyRuntime`/`MarketMakerStrategy`/
`CrossVenueArbStrategy`, proven by `tests/test_market_maker_strategy_e2e.cpp`
and `tests/test_cross_venue_arbitrage_strategy_e2e.cpp` — the latter running two
complete, independent exchange stacks side by side with one strategy trading
both).

`MarketDataPublisher` is wired into a live, long-running process:
`apps/trading_server/main.cpp` constructs an
`OrderEntryGateway` with a purely additive
`OrderEntryGatewayOptions::extra_event_sink` hook that fans every matching-
thread event out to `MarketDataPublisher` in addition to `RiskGatedEngine`'s
own `Ledger` wiring, publishing real UDP frames on a configurable port.
`ui_gateway::UiGateway` (`include/ui_gateway/`, `src/ui_gateway/`) listens to
that same UDP port in its own background thread to reconstruct a live
`book::BookManager` (reusing the trader side's own
`replay::apply_frame_result()` pipeline, exactly like `market_data_replay
--listen` already does, just never stopping on idle), and exposes it — plus
one `TraderRiskGatedOms` + `OrderEntryClient` session per pre-seeded demo
account, connected back to the gateway over real loopback TCP just like any
other trader — as a REST + Server-Sent-Events API
(`cpp-httplib`, a small vendored header-only library — see
`ui_gateway.hpp`'s own comment on why a library was used here specifically,
unlike every hand-rolled wire format elsewhere in this project) for a
separate React + Vite + TypeScript dashboard (`ui/`) to render and trade
against.

`StrategyRuntime` is fed by that live feed too, via
`trader::market_data::FeedSubscriber` (`include/trader/market_data/`), which
owns a receive thread running the same `UdpReceiver::receive_batch()` →
`net::unpack_frames()` → `replay::apply_frame_result()` pipeline `UiGateway` and
`market_data_replay --listen` already use, and then calls
`StrategyRuntime::on_event()` with the book as it stands after each event. It
was the one piece that had never existed: `StrategyRuntime`'s own header
documented that something had to drive it from a live feed, and until now the
only live UDP listener was `UiGateway`, which has no strategies to dispatch to.

`apps/market_simulator/` is what uses it, and is the fullest exercise of this
whole diagram: two simulated participants — a `LadderMarketMaker` quoting
around a seeded random walk and a `MomentumStrategy` reacting to the
reconstructed midpoint — each with its own account, TCP session,
`TraderRiskGatedOms`, `PositionTracker` and `PnlTracker`, trading each other
through the real gateway, pipeline, risk, ledger and matching engine, and
learning what happened from real execution reports over TCP and real market
data over UDP. Because market data now fans out to several ports, the dashboard
can watch it happen live. See `docs/market_simulation.md` for the design and a
real run.

`apps/live_strategy_demo/` predates `FeedSubscriber` and is deliberately left on
its original path: it polls `UiGateway`'s REST book endpoint instead of
subscribing to the feed. That is what `docs/live_demo.md`'s recorded run
documents, and rewriting it would invalidate the run rather than improve it.

---

## 3. Trader-side market-data components

| Component | Files | Role |
|---|---|---|
| Wire codec | `include/protocol/{messages,decoder,encoder,errors}.hpp`, `src/protocol/{decoder,encoder}.cpp` | Big-endian, explicit-shift encode/decode of `AddOrder`/`CancelOrder`/`ModifyOrder`/`Trade`/`ClearBook`; structured `DecodeError`, never exceptions |
| Byte I/O | `include/common/byte_io.hpp` | Bounds-checked `ByteReader`, big-endian put/get helpers |
| File replay | `include/replay/event_file_{reader,writer}.hpp`, `src/replay/event_file_{reader,writer}.cpp` | Streaming, reused-buffer read/write of a flat binary event file |
| Sequencing (classification) | `include/common/sequence_validator.hpp` | `InOrder`/`Duplicate`/`OutOfOrder`/`Missing` classification of an already-assigned sequence number — used for both event-level and packet-level gap detection today |
| Replay engine | `include/replay/replay_engine.hpp`, `src/replay/replay_engine.cpp` | `apply_frame_result()` — the single shared function both file replay and UDP replay funnel through |
| UDP transport | `include/net/{udp_socket,udp_receiver,packet,packet_sequence_tracker,udp_listener}.hpp` + `src/net/*.cpp` | RAII POSIX UDP socket, batched non-blocking receive, packet framing/batching, two-thread producer/consumer pipeline (`net::run_udp_listen()`) |
| Concurrency primitives | `include/common/spsc_queue.hpp`, `include/common/dropping_queue.hpp` | Lock-free bounded SPSC ring buffer; drop-newest + counted backpressure wrapper |
| Snapshot / recovery | `include/replay/snapshot.hpp`, `src/replay/snapshot.cpp` | Book-state snapshot format reusing the `AddOrder` wire frame; `apply_frame_result()` loads a snapshot on a `Missing` classification when configured |
| Local book (trader-side, non-authoritative) | `include/book/{order_book,price_level,book_manager,book_errors}.hpp` + `src/book/*.cpp` | **Reconstructs** state from received events; O(1) hash + O(log P) map + O(1) list-erase cancel/modify; **does not match crossing orders** |
| Live feed subscription | `include/trader/market_data/feed_subscriber.hpp`, `src/trader/market_data/feed_subscriber.cpp` | Owns a receive thread running the `receive_batch` → `unpack_frames` → `apply_frame_result` pipeline above, then dispatches each applied event to a `StrategyRuntime`; the live driver `StrategyRuntime` previously lacked |
| Apps | `apps/{feed_generator,market_data_replay,udp_sender}/main.cpp` | CLI drivers exercising the above end to end |

The exchange side feeds into these through `MarketDataPublisher`; nothing above had to
change to accommodate it.

---

## 4. Component map

| Component | Lives under |
|---|---|
| `ExchangeCommand` / `ExchangeEvent` / `EventSink` types | `exchange/core/` |
| Matching engine (authoritative book, price-time priority, fills) | `exchange/matching/` |
| Exchange command journal + deterministic replay | `exchange/persistence/` |
| Command sequencer + matching-thread pipeline | `exchange/sequencing/` |
| Account balances + pre-trade risk | `exchange/risk/`, `exchange/ledger/` |
| Market-data publisher (matching events → the trader-side wire format) | `exchange/market_data/` |
| TCP order-entry protocol + gateway | `exchange/gateway/`, `protocol/order_entry/` |
| Trader-side OMS + order-entry client | `trader/oms/` |
| Trader-side positions/P&L/risk | `trader/positions/`, `trader/risk/` |
| Strategy runtime, market maker, cross-venue arbitrage, ladder maker, momentum | `trader/strategies/` |
| Live market-data subscription (feed → book → strategies) | `trader/market_data/` |
| UI gateway + React dashboard | `ui_gateway/`, `ui/`, `apps/trading_server/` |
| Benchmarks | `benchmarks/` |
| Failure injection + live demonstration | `tests/test_failure_injection_*.cpp`, `apps/live_strategy_demo/` |
| Simulated market participants | `apps/market_simulator/` |

See `docs/exchange_flow.md` for a full walkthrough of each; this table only
says where things live.

---

## 5. Why the two books cannot share a class

| | Trader-side `book::OrderBook` | Exchange `MatchingBook` |
|---|---|---|
| Input | Already-decided market-data facts (`AddOrder` etc., arriving as *events*) | Inbound *commands* requesting a decision (`NewOrderCommand` etc.) |
| Core operation | Insert/erase a resting order at a known price | Decide whether an incoming order crosses the book *before* it rests |
| Mutates on a fill | Never — no fill concept exists (`Trade` is informational-only, doesn't touch book depth, see `src/replay/replay_engine.cpp`) | Reduces a resting order's *remaining* quantity in place, or removes it entirely |
| Crossed state | Tolerated — a reconstructed book can show a crossed spread and this is documented as expected, not a bug | Never valid — a crossed authoritative book is a correctness bug |
| Source of truth | No — it is a *reconstruction*, allowed to be temporarily wrong/stale pending recovery | Yes — it is the one true record of what the exchange has done |
| Recovery story | Snapshot + resume | Command journal replay — a different mechanism for a different failure mode |

Reusing `RestingOrder`/`OrderBook` unchanged for matching would conflate "record of a fact"
with "adjudicator of a decision," and would make the crossed-spread tolerance of the
reconstruction path (correct for its purpose) silently apply to the matching book (where it
would be a bug). The exchange side therefore has its own resting-order type and its own
book, deliberately not shared.

---

## 6. Verified baseline

Debug, ASan+UBSan, and TSan builds all succeed with zero warnings, and the whole C++
test suite passes under all three configurations. `ui/` (React + Vite + TypeScript)
type-checks (`tsc -b`) and builds (`npm run build`) cleanly, and its output was verified
serving from a real, running `trading_server` process in an actual browser (a headless
Chrome screenshot showing a live order book, positions, orders, and activity feed, all
updating from real SSE events).

`docs/exchange_flow.md` has the full test breakdown by layer. The tests that matter most
under TSan are the multi-threaded TCP round trips —
`test_order_entry_gateway_e2e.cpp`, `test_oms_gateway_e2e.cpp`,
`test_trader_risk_gated_oms_e2e.cpp`, `test_market_maker_strategy_e2e.cpp`,
`test_cross_venue_arbitrage_strategy_e2e.cpp`, `test_ui_gateway.cpp`, and
`test_market_simulator_e2e.cpp` — each of which runs gateway
accept/reader/writer/matching threads on one side against client reader threads on the
other, all concurrently. The last of those adds a `FeedSubscriber` receive thread and
two independent participant sessions to that mix, so a single test has the exchange's
threads, two clients' reader threads, and a UDP receive thread dispatching into a
strategy that sends orders, all live at once.

### Bugs these tests actually caught

- **A self-crossing market maker.** Replacing a widening bid before its own stale ask had
  moved could momentarily cross the strategy's own book, so `MarketMakerStrategy` now
  sequences which side it replaces first. Caught by
  `test_market_maker_strategy_e2e.cpp`.
- **A use-after-free during teardown.** The `RiskGatedTrader` test helper declared its
  `OrderEntryClient` member before its `TraderRiskGatedOms` member, so the client's
  background reader thread — which calls into the risk-gated OMS asynchronously — could
  still be running while the OMS was being destroyed (C++ destroys members in reverse
  declaration order). All three `RiskGatedTrader` copies now declare `TraderRiskGatedOms`
  first so the client, and its thread, tears down before the OMS it calls into does.
  Caught by TSan.
- **A `snapshot()` race.** Both gateway e2e tests called `OrderEntryGateway::snapshot()`
  — only safe once the matching thread has been joined — while the gateway was still
  running; both now call `stop()` first. Caught by TSan.
- **A bind failure that reported success.** `FeedSubscriber::start()` checked
  `UdpReceiver::is_open()` to decide whether it had the port, but that reports only that
  the *socket* was created — `UdpReceiver`'s constructor creates the socket before
  attempting the bind and keeps it either way — so a subscriber that lost a port race
  started a thread and received nothing, silently, forever. It now also requires a
  non-zero bound port, which is the observable difference (`getsockname()` reports 0 for
  an unbound socket, and a successful bind never yields 0 even when 0 was requested).
  Caught by `test_feed_subscriber.cpp`. `UiGateway::start()` makes the same check and has
  the same latent gap; it is left alone here rather than changed as a side effect of
  unrelated work.
- **A book that would freeze at the first dropped datagram.** `FeedSubscriber` initially
  took `replay::ReplayOptions`' defaults, which stop at the first sequence gap or decode
  error. That is right for file replay, where an anomaly means the file is corrupt, and
  wrong for a live feed, where a gap means a lost packet: `apply_frame_result()` does not
  apply the event that revealed the gap, so the subscriber would have gone on handing
  strategies a book frozen at the moment of the loss while continuing to receive. It now
  defaults to counting the anomaly and carrying on. Caught by
  `test_feed_subscriber.cpp`.

Two further concurrency bugs surfaced during manual end-to-end smoke testing of the UI
gateway, before `tests/test_ui_gateway.cpp` existed (which is exactly why that manual
smoke test, later turned into the test file, mattered):
- `UiGateway::start()` passed `http_port_` as cpp-httplib's `socket_flags` argument to
  `Server::bind_to_any_port(host, socket_flags)` — a method that, despite the call
  compiling and returning a seemingly valid port, *always* binds an ephemeral port and
  ignores any specific port a caller wants. Fixed by branching on `http_port_ == 0`
  between `bind_to_any_port(host)` (ephemeral) and `Server::bind_to_port(host, port)`
  (specific), matching `OrderEntryGateway::local_port()`'s own ephemeral-port convention.
- `UiGateway::stop()` hung indefinitely (a real deadlock, not a slow shutdown) for two
  independent reasons, both now fixed:
  - The market-data background thread's lambda took a `std::stop_token` as its first
    parameter, which makes `std::jthread` silently inject *its own* internal stop token
    instead of the class's own `stop_source_` — so `stop_source_.request_stop()` in
    `stop()` was requesting a stop nobody was listening for. Fixed by *not* taking a
    `std::stop_token` parameter on that lambda at all and explicitly passing
    `stop_source_.get_token()` into `market_data_loop()` instead — the same
    explicit-shared-`stop_source_` convention `OrderEntryGateway::accept_loop()` already
    uses, for exactly this reason.
  - A `stop()` call racing immediately behind `start()` could run before cpp-httplib's
    own `listen_after_bind()` thread had reached its internal `is_running_ = true` --
    `Server::stop()` guards its entire body on `if (is_running_)`, so in that race it
    silently did nothing, the listening socket was never closed, and `http_thread_.join()`
    then waited forever on an accept loop with nothing left to wake it. Fixed by calling
    `Server::wait_until_ready()` (cpp-httplib's own documented fix for exactly this race)
    right after spawning `http_thread_`, before `start()` returns.

`tests/test_ui_gateway.cpp` is the UI gateway's loop-closing test, same shape as every
other `*_e2e.cpp` in this codebase: a real `OrderEntryGateway` (with
`extra_event_sink` wired to a real `MarketDataPublisher` over a real UDP socket) plus a
real `UiGateway`, driven entirely through `httplib::Client` against the actual REST/SSE
surface — never by reaching into either class's internals. It proves, over real sockets,
real threads, and real JSON: account auto-provisioning and 404s for accounts outside the
demo catalog, order submit/cancel/replace round trips (including the `is_live()` gating
both operations require), a resting order becoming visible on `GET /api/book/:id` only
because a real UDP frame really was published and received, a crossing order producing a
real fill visible on both accounts, and `/api/stream` actually delivering a live "order"
SSE event to a subscribed client after an order is submitted.

### Failure injection

The 11 failure-injection tests (`tests/test_failure_injection_gateway.cpp`,
`tests/test_failure_injection_market_data.cpp` — 6 and 5 respectively) all pass clean
under TSan, which matters here because several of these faults (a slow non-reading
client, a flood of random bytes, duplicated/gapped UDP packets) are only actually
testing anything real if the reader/writer/matching/market-data threads they exercise
are genuinely running concurrently, not accidentally serialized by test timing. See
`docs/failure_injection.md` for the full fault matrix.

### Benchmarks

Benchmarks are a Release-only artifact, not correctness tests.
`benchmarks/` (five Google-Benchmark executables plus one hand-rolled latency harness,
gated behind `-DMDH_BUILD_BENCHMARKS=ON`, the default) were built and actually run in
Release on this machine; every number is recorded, with methodology and interpretation, in
`docs/benchmarks.md` — summarized: every hot-path component measured (protocol codec,
matching engine, trader-side book, SPSC queue) costs tens to low thousands of nanoseconds
per operation. The one live, real-TCP measurement (`bench_end_to_end_latency`, a real
round trip against a real `OrderEntryGateway`) initially measured ~0.7-1.3 ms at
p50/mean — three orders of magnitude larger, traced (by reading the code, not guessed)
to a specific, deliberate design choice: `connection_writer_loop()`'s 1 ms sleep-based
poll, not anything slow in matching/risk/ledger. That finding was then actually acted
on, not just recorded: the writer thread now blocks on a condition variable notified
directly by `route_event()` instead of polling on a timer, plus `TCP_NODELAY` was added
to every connected `TcpSocket` (a second, previously-unexamined latency source the same
investigation turned up). Re-measured after both fixes, p50 dropped to ~73 μs — roughly
a 17x reduction, verified with two independent 20,000-sample runs, documented in
`docs/benchmarks.md` §7.2.

### The live demonstration

The one scenario every e2e test proved the *pieces* for but none assembled into a single
live run: a real `MarketMakerStrategy` (`apps/live_strategy_demo/`) trading against a
real, running `trading_server`, a real second account crossing both sides of its quotes
over the same REST API a browser dashboard uses, both processes' independently-computed
fill accounting agreeing exactly, and a real headless-Chrome screenshot of the resulting
live dashboard state. `docs/live_demo.md` has the full run, every JSON response and log
line involved, and an explicit list of the demo's deliberate scope limits (REST-polling
the book instead of a second UDP subscriber; the pre-existing, documented IOC/FOK
no-fill-remainder behavior of the matching engine).
