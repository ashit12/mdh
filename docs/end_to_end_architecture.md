# End-to-End Architecture — Trader Side (existing) and Exchange Side (planned)

**Status:** originally a Milestone 0 deliverable, describing a system where no
exchange-side production code existed yet. As of Milestone 14, Milestones 1–14 below are
implemented and tested (see `docs/exchange_flow.md` for the exchange-side walkthrough,
`docs/benchmarks.md` for Milestone 13, and `docs/failure_injection.md`/`docs/live_demo.md`
for Milestone 14) — every milestone this document originally scoped is now built. Sections
below that predate that completion are kept largely as originally written, for history;
where a section's own claim has since been superseded, a note says so explicitly rather
than silently rewriting it.

---

## 1. The core rule this document exists to state

**There will be two order-book implementations, and they must never be merged into one
class just because both hold bids and asks:**

- **`book::OrderBook` (existing, trader side)** reconstructs a view of exchange state
  *from published market-data events*. It never decides whether two orders cross; it only
  records that an add/cancel/modify *already happened* upstream.
- **The exchange matching book (new, Milestone 2)** is the *authoritative* book. It accepts
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
┌───────────────────────────── EXCHANGE SIDE (new, not yet built) ─────────────────────────────┐
│                                                                                                │
│  Trading client                                                                               │
│       │ TCP binary order-entry protocol                        (Milestone 7)                 │
│       ▼                                                                                       │
│  Order-entry gateway ──────────────────────────────────────────  (Milestone 7)                │
│       │                                                                                       │
│       ▼                                                                                       │
│  Exchange validation + pre-trade risk ─────────────────────────  (Milestone 5)                │
│       │                                                                                       │
│       ▼                                                                                       │
│  Command sequencer ─────────────────────────────────────────────  (Milestone 4)               │
│       │  strictly increasing CommandSequence                                                  │
│       ▼                                                                                       │
│  SPSC queue (reuse common::SpscQueue) ──────────────────────────  (Milestone 4)                │
│       │                                                                                        │
│       ▼                                                                                        │
│  ┌─────────────────────────────────────────┐                                                   │
│  │  MATCHING ENGINE  (Milestone 1 + 2)      │  single-threaded, deterministic                  │
│  │  ExchangeCommand -> process() -> events  │  owns the AUTHORITATIVE book                     │
│  └─────────────────────────────────────────┘                                                   │
│       │                                                                                        │
│       ├──► Execution reports          (Milestone 7 return path)                                │
│       ├──► Balance / ledger updates   (Milestone 5)                                             │
│       ├──► Market-data events         (Milestone 6) ───────────────────────────┐                │
│       └──► Command journal / replay   (Milestone 3)                            │                │
│                                                                                 │                │
└─────────────────────────────────────────────────────────────────────────────────┼────────────────┘
                                                                                  │
                                                                     UDP market-data feed
                                                                     (existing wire format,
                                                                      reused by Milestone 6)
                                                                                  │
┌─────────────────────────────────────────────────────────────────────────────────┼────────────────┐
│                          TRADER-FIRM SIDE (existing, verified in this milestone)│                │
│                                                                                  ▼                │
│  net::UdpReceiver / net::run_udp_listen() ──────────────────────────  EXISTING                    │
│       │  producer thread: receive_batch() → unpack_frames() → DroppingQueue.push()                │
│       ▼                                                                                           │
│  replay::apply_frame_result()  ─────────────────────────────────────  EXISTING                    │
│       │  consumer thread: SequenceValidator classifies; on Missing + configured                    │
│       │  snapshot path, replay::read_snapshot() recovers instead of stopping                       │
│       ▼                                                                                            │
│  book::BookManager / book::OrderBook  ──────────────────────────────  EXISTING                     │
│       │  reconstructed, non-authoritative view — "what the exchange published so far"              │
│       ▼                                                                                            │
│  Strategy runtime ─────────────────────────────────────────────────  (Milestones 10-11)             │
│       ▼                                                                                            │
│  Trader-side risk ─────────────────────────────────────────────────  (Milestone 9)                 │
│       ▼                                                                                            │
│  OMS ──────────────────────────────────────────────────────────────  (Milestone 8)                 │
│       ▼                                                                                            │
│  TCP order-entry client ────────────────────────────────────────────  (Milestone 8) ───────────────┘
│       (connects back up to the exchange gateway at the top of this diagram)
│
└───────────────────────────────────────────────────────────────────────────────────────────────────┘

                     Outside the latency-sensitive path on both halves, but now wired
                     into a real running process (apps/trading_server/main.cpp):
                     UI gateway (REST + Server-Sent Events) + React dashboard  (Milestone 12)
                          │
                          ├──► listens to the same UDP market-data feed above, in its own
                          │    background thread, to reconstruct a live book::BookManager
                          └──► holds one TraderRiskGatedOms + OrderEntryClient per UI
                               account, connected back to the order-entry gateway over
                               real loopback TCP -- exactly like Milestones 8-11's own
                               traders/strategies, just driven by HTTP requests instead
```

**Status of the milestone numbers in the diagram above, as of this writing:**
Milestones 1–11 are built and tested (see `docs/exchange_flow.md` for 1–9's own
walkthrough, and its "Milestones 10-11" section below the milestone-by-milestone
list for the strategy layer) — this includes the "EXCHANGE SIDE" box in full (the
order-entry gateway really does sit in front of a risk-gated matching engine
today, over a real TCP socket) and, in the "TRADER-FIRM SIDE" box, the OMS + TCP
order-entry client (Milestone 8) that closes the loop back up into that same
gateway — proven by `tests/test_oms_gateway_e2e.cpp` — trader-side positions/P&L
and a second, independent trader-side risk check (Milestone 9,
`trader::risk::TraderRiskGatedOms`, wrapping that same OMS+client pair) —
proven by `tests/test_trader_risk_gated_oms_e2e.cpp` — and now a strategy layer
sitting on top of all of that (Milestones 10-11, `trader::strategies::
StrategyRuntime`/`MarketMakerStrategy`/`CrossVenueArbStrategy`), proven by
`tests/test_market_maker_strategy_e2e.cpp` (a real market maker quoting,
getting filled, and requoting over a real gateway) and
`tests/test_cross_venue_arbitrage_strategy_e2e.cpp` (two complete, independent
exchange stacks running side by side, with one strategy trading both). The UDP
market-data path in that same box predates this document (labeled "EXISTING"
above) and is untouched — `StrategyRuntime` is unit-tested against synthetic
market-data events the same way `replay::apply_frame_result()` itself is, but
is not yet fed by a live UDP feed against a running gateway; strategies
remain driven by their own e2e tests' simulated book updates, not a live feed.

As of Milestone 12, `MarketDataPublisher` *is* now wired into a live,
long-running process: `apps/trading_server/main.cpp` constructs an
`OrderEntryGateway` with a new, purely additive
`OrderEntryGatewayOptions::extra_event_sink` hook that fans every matching-
thread event out to `MarketDataPublisher` in addition to `RiskGatedEngine`'s
own `Ledger` wiring, publishing real UDP frames on a configurable port. A new
`ui_gateway::UiGateway` (`include/ui_gateway/`, `src/ui_gateway/`) listens to
that same UDP port in its own background thread to reconstruct a live
`book::BookManager` (reusing the trader side's own
`replay::apply_frame_result()` pipeline, exactly like `market_data_replay
--listen` already does, just never stopping on idle), and exposes it — plus
one `TraderRiskGatedOms` + `OrderEntryClient` session per pre-seeded demo
account, connected back to the gateway over real loopback TCP just like a
Milestone 8-11 trader would — as a REST + Server-Sent-Events API
(`cpp-httplib`, a small vendored header-only library — see
`ui_gateway.hpp`'s own comment on why a library was used here specifically,
unlike every hand-rolled wire format elsewhere in this project) for a
separate React + Vite + TypeScript dashboard (`ui/`) to render and trade
against. `StrategyRuntime` is still not fed by this live feed — that remains
open, see Milestone 12's own entry in section 4's table below — but the live
feed itself now exists and is exercised end to end by
`tests/test_ui_gateway.cpp`. As of Milestone 14, a real `MarketMakerStrategy`
does trade live against a running `trading_server` (`apps/live_strategy_demo/`),
by polling `UiGateway`'s own REST book endpoint rather than by
`StrategyRuntime` reacting to the raw UDP feed directly -- a narrower,
demo-appropriate substitution for that same open gap; see
`docs/exchange_flow.md`'s Milestone 14 section and `docs/live_demo.md` for
the full run. Milestone 13 (benchmarks, `docs/benchmarks.md`) and the rest of
Milestone 14 (failure injection, `docs/failure_injection.md`) are likewise
now complete.

---

## 3. Existing market-data components (trader side — verified working this milestone)

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
| Apps | `apps/{feed_generator,market_data_replay,udp_sender}/main.cpp` | CLI drivers exercising the above end to end |

**None of the above changed in this milestone.** They are the foundation the exchange side
will eventually feed into (Milestone 6) and otherwise remain untouched, per the working
rules governing this migration.

---

## 4. Exchange-side components: built vs. still to come

| Component | Target milestone | Status | Lives under |
|---|---|---|---|
| `ExchangeCommand` / `ExchangeEvent` / `EventSink` types | 1 | Built | `exchange/core/` |
| Matching engine (authoritative book, price-time priority, fills) | 2 | Built | `exchange/matching/` |
| Exchange command journal + deterministic replay | 3 | Built | `exchange/persistence/` |
| Command sequencer + matching-thread pipeline | 4 | Built | `exchange/sequencing/` |
| Account balances + pre-trade risk | 5 | Built | `exchange/risk/`, `exchange/ledger/` |
| Market-data publisher (matching events → existing wire format) | 6 | Built | `exchange/market_data/` |
| TCP order-entry protocol + gateway | 7 | Built | `exchange/gateway/`, `protocol/order_entry/` |
| Trader-side OMS + order-entry client | 8 | Built | `trader/oms/` |
| Trader-side positions/P&L/risk | 9 | Built | `trader/positions/`, `trader/risk/` |
| Strategy runtime + market maker | 10 | Built | `trader/strategies/` |
| Additional strategies, two-venue simulation | 11 | Built | `trader/strategies/` |
| UI gateway + React dashboard | 12 | Built | `ui_gateway/`, `ui/`, `apps/trading_server/` |
| Benchmarks | 13 | Built | `benchmarks/` |
| Failure injection + final demonstration | 14 | Built | `tests/test_failure_injection_*.cpp`, `apps/live_strategy_demo/` |

See `docs/exchange_flow.md` for a full walkthrough of Milestones 1–9's
implementation (and its own "Milestones 10-11" section for the strategy
layer); this table only tracks scope and status.

Per the working rules, headers stay where they are today until the exchange-side
architecture is actually working — the `exchange/...` paths above are the eventual
destination, not something created now. Milestone 1 will add new files alongside the
existing tree (e.g. `include/exchange/core/...`) without moving anything that already exists.

---

## 5. Why the two books cannot share a class

| | Trader-side `book::OrderBook` (existing) | Exchange matching book (new) |
|---|---|---|
| Input | Already-decided market-data facts (`AddOrder` etc., arriving as *events*) | Inbound *commands* requesting a decision (`NewOrderCommand` etc.) |
| Core operation | Insert/erase a resting order at a known price | Decide whether an incoming order crosses the book *before* it rests |
| Mutates on a fill | Never — no fill concept exists (`Trade` is informational-only, doesn't touch book depth, see `src/replay/replay_engine.cpp`) | Reduces a resting order's *remaining* quantity in place, or removes it entirely |
| Crossed state | Tolerated — a reconstructed book can show a crossed spread and this is documented as expected, not a bug | Never valid — a crossed authoritative book is a correctness bug |
| Source of truth | No — it is a *reconstruction*, allowed to be temporarily wrong/stale pending recovery | Yes — it is the one true record of what the exchange has done |
| Recovery story | Snapshot + resume (already built, Milestone 4 of the market-data roadmap) | Command journal replay (Milestone 3 of *this* roadmap) — a different mechanism for a different failure mode |

Reusing `RestingOrder`/`OrderBook` unchanged for matching would conflate "record of a fact"
with "adjudicator of a decision," and would make the crossed-spread tolerance of the
reconstruction path (correct for its purpose) silently apply to the matching book (where it
would be a bug). Milestone 2 will define a distinct exchange-side resting-order type and
book, per the working rules.

---

## 6. Verified baseline (this milestone)

See the accompanying report for exact commands and output. Summary: debug build,
ASan+UBSan build, and TSan build all succeed with zero warnings; all 103 existing tests
pass under all three configurations, with no source changes made. Breakdown by area
relevant to this document's "existing components" claim above:

- File replay (event-file I/O, replay determinism): 7 tests, all passing.
- UDP replay (socket, batched receive, packet framing/sequencing, end-to-end UDP replay): 16 tests, all passing.
- Snapshot / sequence-gap recovery: 9 tests, all passing.
- Local (reconstructed) order book: 18 tests, all passing.
- Remaining 53 tests (protocol codec, decode errors, sequence validation, SPSC/dropping queue, backpressure integration): all passing.

No exchange-side code exists to test yet — that begins at Milestone 1.

---

## 7. Verified baseline as of Milestone 9

Superseding section 6 above (kept for history): debug, ASan+UBSan, and TSan builds all
still succeed with zero warnings, and all **314** tests pass under all three
configurations. `docs/exchange_flow.md` has the full exchange-side test breakdown by
milestone; the highlights specific to Milestones 7–9's own concurrency are
`test_order_entry_gateway_e2e.cpp`, `test_oms_gateway_e2e.cpp`, and
`test_trader_risk_gated_oms_e2e.cpp`, all real multi-threaded TCP round trips (gateway
accept/reader/writer/matching threads on one side, an `OrderEntryClient` reader thread on
the other) that pass clean under TSan. The last of these is also the test proving
Milestone 9's central design claim: the trader-side risk check
(`TraderRiskEngine`/`PositionTracker`) and the exchange-side one (`RiskGatedEngine`/
`Ledger`) are genuinely independent, each capable of rejecting an order the other would
have allowed.

---

## 8. Verified baseline as of Milestone 11

Superseding section 7 above (kept for history): debug, ASan+UBSan, and TSan builds all
still succeed with zero warnings, and all **337** tests pass under all three
configurations. `docs/exchange_flow.md` has the full breakdown by milestone, including
its own "Milestones 10-11" section; the highlights specific to Milestones 10-11 are
`test_market_maker_strategy_e2e.cpp` (a real `MarketMakerStrategy`, trading through a
real `TraderRiskGatedOms` + `OrderEntryClient`, quoting/getting filled/requoting over a
real TCP connection to a real `OrderEntryGateway` — including a fix this test itself
caught: replacing a widening bid before its own stale ask had moved could momentarily
cross the strategy's own book, so `MarketMakerStrategy` now sequences which side it
replaces first) and `test_cross_venue_arbitrage_strategy_e2e.cpp` (two complete,
independent exchange stacks — two gateways, two matching engines, two ledgers — running
side by side in one test process, with a single `CrossVenueArbStrategy` trading both and
capturing a real, seeded price discrepancy between them via two independent IOC order
round trips). TSan also caught a second real, if narrow, bug during this milestone's own
development, in test infrastructure rather than production code: the `RiskGatedTrader`
test helper (shared by the Milestone 9 and Milestone 10-11 e2e tests) declared its
`OrderEntryClient` member before its `TraderRiskGatedOms` member, so the client's
background reader thread — which calls into the risk-gated OMS asynchronously — could
still be running while the OMS was being destroyed (C++ destroys members in reverse
declaration order); all three `RiskGatedTrader` copies now declare `TraderRiskGatedOms`
first so the client (and its thread) tears down before the OMS it calls into does.

---

## 9. Verified baseline as of Milestone 12

Superseding section 8 above (kept for history): debug, ASan+UBSan, and TSan builds all
still succeed with zero warnings, and all **351** C++ tests pass under all three
configurations, including the 13 new tests in `tests/test_ui_gateway.cpp`. This is also
the first milestone with a separate, non-C++ build: `ui/` (React + Vite + TypeScript)
type-checks (`tsc -b`) and builds (`npm run build`) cleanly, and its output was verified
serving from a real, running `trading_server` process in an actual browser (a headless
Chrome screenshot showing a live order book, positions, orders, and activity feed, all
updating from real SSE events).

Two real concurrency bugs surfaced and were fixed during this milestone's own manual
end-to-end smoke testing (not caught by the unit-level tests written first, which is
exactly why the manual smoke test — then turned into `tests/test_ui_gateway.cpp` — mattered):
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

`tests/test_ui_gateway.cpp` is this milestone's loop-closing test, same shape as every
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

---

## 10. Verified baseline as of Milestone 14

Superseding section 9 above (kept for history): debug, ASan+UBSan, and TSan builds all
still succeed with zero warnings, and all **362** C++ tests pass under all three
configurations, including the 11 new failure-injection tests
(`tests/test_failure_injection_gateway.cpp`, `tests/test_failure_injection_market_data.cpp`
-- 6 and 5 respectively) — see `docs/failure_injection.md` for the full fault matrix each
one covers and confirmation all 11 pass clean under TSan specifically, which matters here
because several of these faults (a slow non-reading client, a flood of random bytes,
duplicated/gapped UDP packets) are only actually testing anything real if the
reader/writer/matching/market-data threads they exercise are genuinely running
concurrently, not accidentally serialized by test timing.

This milestone also added a Release-build-only artifact class this document's own
"Verified baseline" sections hadn't needed before: **benchmarks**, not correctness tests.
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

Finally, this milestone ran the one demonstration every earlier milestone's own
end-to-end test proved the *pieces* for but never assembled into a single live scenario:
a real `MarketMakerStrategy` (`apps/live_strategy_demo/`, new) trading against a real,
running `trading_server`, a real second account crossing both sides of its quotes over
the same REST API a browser dashboard uses, both processes' independently-computed fill
accounting agreeing exactly, and a real headless-Chrome screenshot of the resulting live
dashboard state. `docs/live_demo.md` has the full run, every JSON response and log line
involved, and an explicit list of this demo's own deliberate scope limits (REST-polling
the book instead of a second UDP subscriber; a pre-existing, documented IOC/FOK
no-fill-remainder behavior from Milestones 3/4 that is not new to this milestone).
