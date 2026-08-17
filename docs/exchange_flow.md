# Exchange-Side Flow: `include/exchange/` and `src/exchange/`

This is a code-reading guide to the exchange side of `mdh` — the parts under
`exchange/` that implement an authoritative matching engine, as opposed to
the trader-side market-data handler the top-level `README.md` already
documents (UDP ingestion, sequence validation, book reconstruction). The two
sides share only style conventions (big-endian wire encoding, structured
error enums, `std::variant` command/event vocabularies) and one root
namespace; neither depends on the other's types. If you're looking for how
`market_data_replay` or `udp_sender` work, see the top-level `README.md`
instead — this document is scoped entirely to `mdh::exchange`.

It walks through the fourteen milestones built so far in the order they were
built, then traces one order through the whole stack end to end, then gives
a directory-by-directory component reference. (Milestones 10-11 live under
`trader/strategies/`, one level above everything else this document
otherwise covers — see the "Milestones 10-11" section right after the
numbered list below for those two specifically. Milestone 12 -- the UI
gateway -- lives under `ui_gateway/`, `apps/trading_server/`, and the
separate `ui/` React project; see its own section right after "Milestones
10-11" below. Milestones 13-14 -- benchmarks, failure injection, and the
final live demonstration -- live under `benchmarks/`, `tests/
test_failure_injection_*.cpp`, and `apps/live_strategy_demo/`; see their own
sections right after Milestone 12 below, and `docs/benchmarks.md`/
`docs/failure_injection.md`/`docs/live_demo.md` for the full detail this
document only summarizes.)

---

## Milestones, in one paragraph each

**Milestone 1 — Domain types & commands** (`exchange/core/types.hpp`,
`exchange/core/commands.hpp`). The exchange's own vocabulary: `AccountId`,
`ClientOrderId`, `ExchangeOrderId`, `CommandSequence`, `EventSequence`,
`OrderType`, `TimeInForce`, `RejectReason`, and the three inbound command
structs (`NewOrderCommand`, `CancelOrderCommand`, `ReplaceOrderCommand`)
bundled into `ExchangeCommand = std::variant<...>`. Transport-independent —
nothing here has been decoded from wire bytes, and a command carries no
timestamp, because the matching engine's determinism rule forbids one being
captured inside the matcher.

**Milestone 2 — The matching engine** (`exchange/matching/`). A
single-threaded, deterministic `MatchingEngine::process(command, sink)` that
turns one `ExchangeCommand` into zero or more `ExchangeEvent`s, delivered
synchronously to an `EventSink` callback. Owns a `MatchingBook` per
instrument (price-time-priority order book) and emits `OrderAccepted` /
`OrderRejected` / `OrderCancelled` / `OrderReplaced` / `TradeExecuted` /
`BookOrder{Added,Reduced,Removed}`.

**Milestone 3 — Command journal & deterministic replay**
(`exchange/persistence/`). A binary wire format for `ExchangeCommand`
(`command_encoder`/`command_decoder`, mirroring the trader-side protocol
codec's conventions but for a different vocabulary), a sequential
file writer/reader (`CommandJournalWriter`/`CommandJournalReader`), and
`run_command_replay()`, which feeds a journal file into a fresh
`MatchingEngine` and returns every event plus a canonical, order-independent
`EngineStateSnapshot`. Proves the matcher is genuinely deterministic:
replaying the same journal twice produces byte-for-byte identical event
streams and equal final states.

**Milestone 4 — Command sequencer & matching pipeline**
(`exchange/sequencing/`). `CommandSequencer` assigns the authoritative,
monotonically increasing `CommandSequence` to an inbound command — the one
place that decides matching order, so no upstream caller can pick its own
position in it. `MatchingPipeline` wraps a lock-free SPSC queue and a
dedicated matching thread around a `MatchingEngine`, so a producer thread
can call `submit()` without itself becoming the thread that runs matching
logic. A full queue is an explicit rejection (`submit()` returns `false`),
never a silent drop — unlike market-data's `DroppingQueue`, a dropped
*inbound order* is unacceptable: the client would believe their order was
seen when it never reached the matcher.

**Milestone 5 — Ledger & pre-trade risk** (`exchange/ledger/`,
`exchange/risk/`). `Ledger` tracks per-account cash and per-instrument
position balances by watching the matching engine's own event stream (same
`EventSink` pattern the engine itself uses), with reservation semantics for
resting GTC orders. `RiskEngine` performs pre-trade checks (order size, cash
for buys, position for sells) against the ledger's *available* (unreserved)
balances. `RiskGatedEngine` composes `RiskEngine` + `Ledger` +
`MatchingEngine` behind `MatchingEngine::process()`'s exact signature, so it
can drop in anywhere a bare engine is used.

**Milestone 6 — Market-data publisher** (`exchange/market_data/`).
`MarketDataPublisher` translates the four *public* `ExchangeEvent`s
(`BookOrderAdded`/`Reduced`/`Removed`, `TradeExecuted`) into the trader
side's own, pre-existing wire format (`protocol::AddOrder`/`ModifyOrder`/
`CancelOrder`/`Trade`) — the same format `feed_generator` has always
produced synthetically. The four private, account-addressed events never
cross this boundary. Owns its own `Sequence`/`Timestamp` stream (distinct
from `EventSequence`/`CommandSequence`) and captures wall-clock time at
publish time — a deliberate, narrow exception to the "no wall-clock time"
rule that protects the matcher itself, not the feed-publish boundary. This
is the milestone that closes the loop for real: an end-to-end test
(`test_market_data_e2e.cpp`) runs commands through a real `MatchingEngine`,
publishes the resulting events through `MarketDataPublisher`, writes them
with the trader side's *unmodified* `EventFileWriter`, and replays them
back through the trader side's *unmodified* `replay::run_replay()` —
asserting the reconstructed `book::BookManager` agrees with the
authoritative `MatchingEngine::snapshot()`.

**Milestone 7 — TCP order-entry gateway** (`exchange/gateway/`,
`protocol/order_entry/`). The first real network front door to the exchange:
`OrderEntryGateway` composes everything built so far (`RiskGatedEngine` wired
into `MatchingPipeline` via its `Processor` seam) behind a TCP listener built
on a new `net::TcpSocket` RAII wrapper (the connection-oriented counterpart
to the trader side's existing `net::UdpSocket`). `protocol::order_entry/`
defines a small, 3-byte-header, length-prefixed wire format distinct from
`protocol/messages.hpp` — no sequence number, since TCP already guarantees
ordered, lossless delivery (see that header's own comment for the full
reasoning) — carrying `NewOrder`/`CancelOrder`/`ReplaceOrder` one way and
`Accepted`/`Rejected`/`Cancelled`/`Replaced`/`TradeReport` the other. Each
accepted connection gets its own reader thread (decodes inbound frames,
translates them to `ExchangeCommand`s, and calls a mutex-serialized
`submit()`) and writer thread (drains a per-connection outbound `SpscQueue`
fed by `route_event()`, which runs *on the matching thread* as the
`Processor`'s `EventSink` and therefore must never block — a slow client's
socket cannot be allowed to stall matching for every other client).
Session-to-account binding is opportunistic: a connection is unbound until
its first valid request arrives, since every client message type already
carries `account_id`. That binding is then immutable — a later message
naming a different account is answered with `Rejected{AccountMismatch}` and
never reaches the pipeline — and an account may have several sessions at
once. Which of them a private report belongs to is decided by
`(account_id, client_order_id)`, the same key `MatchingEngine` already uses
for live orders: the gateway records the session that submitted each order
and routes its reports back there, falling back to another live session for
the account if that one has disconnected (a resting order outlives the session
that placed it) and, failing even that, retaining the report until a session
for the account reconnects. This retention is only a bounded, process-local
best effort: queue overflow, a slow connected client, or a gateway restart
can lose reports. Durable offline delivery and a full reconciliation protocol
remain out of scope. None of this reaches into `ExchangeCommand`/
`ExchangeEvent` — the exchange core has still never heard of a socket.
`test_order_entry_gateway_e2e.cpp` is the loop-closing test here — a
hand-rolled test client drives a real `OrderEntryGateway` over real loopback
TCP, including a two-connection crossing trade that exercises `TradeReport`
fan-out to both sides independently, and four session tests covering the
routing rules above.

**Milestone 8 — Trader-side OMS + order-entry client** (`trader/oms/`). The
client-side counterpart to Milestone 7, reusing the exact same
`protocol::order_entry/` wire format (the two milestones' only shared
contract). `OrderEntryClient` is the network transport — one background
reader thread decoding frames and delivering them to a `MessageSink`,
`send()` writing synchronously on the caller's thread since (unlike the
gateway) there is no shared matching thread here for a blocking write to
endanger. `OrderManagementSystem` is the actual client-side order state
machine (`PendingNew → Live → PartiallyFilled/Filled`, or `→ Rejected/
Cancelled/Replaced`), deliberately decoupled from `OrderEntryClient` itself
via two function-shaped seams (a `Sender` for outbound, `handle_message()`
for inbound) — the same `EventSink`-style pattern used throughout this
codebase, which is what makes its full state machine unit-testable
(`test_order_management_system.cpp`) with a fake sender and no socket at
all. The trickiest piece of that state machine: the wire's `Rejected`
message carries no "what kind of request failed" field, so a `Rejected`
referencing an order that isn't `PendingNew` must mean an in-flight
cancel/replace attempt failed (the order itself is untouched), not that the
order was rejected — `ClientOrder::pending_action` is what lets
`handle_message()` tell those apart. `test_oms_gateway_e2e.cpp` is the
loop-closing test: a real `OrderManagementSystem` + `OrderEntryClient`
against a real, unmodified `OrderEntryGateway`, covering the same scenarios
as Milestone 7's own e2e test but driven through the production client
instead of a hand-rolled one.

**Milestone 9 — Trader-side positions/P&L/risk** (`trader/positions/`,
`trader/risk/`). The trader-side mirror of Milestone 5's `Ledger` +
`RiskEngine` + `RiskGatedEngine` trio, one level up the stack:
`positions::PositionTracker` keeps per-account cash and per-instrument
holdings by watching fills, and `risk::TraderRiskEngine` checks a
prospective order against it (order-too-large, insufficient funds,
insufficient position -- the exact same three checks and `RejectReason`
values as the exchange-side `RiskEngine`, reused rather than duplicated as a
parallel enum). `risk::TraderRiskGatedOms` composes both together with a
real `OrderManagementSystem` behind (almost) `OrderManagementSystem`'s own
`submit_new_order()`/`cancel_order()`/`replace_order()` surface, exactly
mirroring how `RiskGatedEngine` composes its own trio behind
`MatchingEngine::process()`'s signature. The one real design problem this
milestone had to solve: `PositionTracker` needs a fill's *price* and
*incremental quantity* to update cash correctly, but
`protocol::order_entry::TradeReport` (the wire message `OrderManagementSystem`
already decodes) carries only a running `remaining_quantity`, not a delta,
and no `side` at all (see that struct's own comment on why) -- so
`OrderManagementSystem` gained a small, purely additive `Fill`/`FillSink`
(computed once, inside `on_trade_report()`, from data it already has:
`TradeReport`'s price/quantity plus the tracked order's own remembered
side) rather than making every downstream consumer re-derive that from raw
wire messages independently. The other explicit scope decision: unlike
`Ledger`, `PositionTracker` does **not** reserve against the trader's own
in-flight orders -- it is a second, best-effort, independent screen sitting
in front of the OMS, never the authoritative check (the exchange's own
`RiskGatedEngine` remains that); `test_trader_risk_gated_oms_e2e.cpp`
proves both layers really are independent by seeding the trader-side and
exchange-side ledgers differently and showing each layer can reject on its
own without the other's help.

---

## Milestones 10-11 -- the strategy layer (`trader/strategies/`)

**Milestone 10 -- Strategy runtime + market maker.**
`strategies::StrategyRuntime` is the thin dispatch piece named "Strategy
runtime" in `docs/end_to_end_architecture.md`'s system diagram:
`subscribe(instrument_id, sink)` registers a `BookUpdateSink`
(`std::function<void(InstrumentId, const book::OrderBook&)>` -- a plain
callable, not a virtual `Strategy` base class, matching this codebase's one,
uniform `EventSink`-style convention everywhere else), and `on_event(event,
books)` extracts `event`'s instrument id (every `protocol::Event` variant
carries one) and calls every sink subscribed to it, passing the book *as
the caller has already updated it* -- `on_event()` does not itself mutate
`books`, by design (see the header's own comment on why duplicating
`replay::apply_frame_result()`'s private `apply_event()` logic here would be
the wrong tradeoff). `strategies::MarketMakerStrategy` is a textbook
two-sided market maker built on top of it: on every book update for its own
instrument, it quotes a bid and an ask centered on the book's midpoint,
`half_spread` ticks apart, replacing a resting quote (via
`TraderRiskGatedOms::replace_order()`) only once the desired price has
drifted by at least `requote_threshold` ticks, and withdrawing the bid
entirely once held position reaches `max_position` (the ask side is already
self-limiting -- `TraderRiskEngine`, Milestone 9, refuses to oversell). Its
own e2e test (`test_market_maker_strategy_e2e.cpp`) caught a real bug during
development: replacing a widening bid *before* the still-stale ask on the
same book had moved could have the strategy's own new bid immediately cross
and self-fill against its own resting ask; `on_book_update()` now checks
which side's move would create that transient cross and sends that side's
replace first (see `market_maker_strategy.cpp`'s own comment for the full
reasoning) -- the exact "widen the far side before tightening the near side"
discipline a real market maker's quoting logic needs.

**Milestone 11 -- Additional strategies, two-venue simulation.**
`strategies::CrossVenueArbStrategy` is a second, independent strategy on the
exact same plumbing: it watches the same instrument's book on two separate
venues (two entirely independent `TraderRiskGatedOms` connections -- nothing
about `StrategyRuntime` or `TraderRiskGatedOms` has any single-venue
assumption baked in) and, whenever one venue's best ask sits at least
`min_edge` below the other venue's best bid, buys on the cheap venue and
sells on the rich one, both legs as IOC (an arbitrage order is only worth
sending if it can execute immediately against the edge just observed; GTC
would risk only one leg filling, leaving a naked position). The "two-venue
simulation" itself is `test_cross_venue_arbitrage_strategy_e2e.cpp`: two
complete, independent exchange stacks (each its own `OrderEntryGateway` in
front of its own matching engine and ledger, per Milestone 7's own
composition) running side by side in one test process on two different
ports, each with its own liquidity-provider account creating a genuine,
seeded price discrepancy, with the arbitrage strategy capturing it via two
real, independent TCP round trips and each venue's own `PositionTracker`
showing the resulting cash/position movement.

**Why both strategies' books are built by the test, not a live feed:** per
this document's own "Integration status" section, `MarketDataPublisher`
(Milestone 6) is not yet wired into a running gateway's matching thread --
there is no live UDP feed to listen to yet. Both e2e tests instead mirror
what such a feed would eventually report directly into a `book::OrderBook`,
using only information the test itself already knows (a liquidity
provider's own confirmed order prices), and drive the strategy's
`on_book_update()`/`on_venue_a_update()`/`on_venue_b_update()` exactly the
way a live `StrategyRuntime` call site would once that wiring exists. Order
flow itself -- every `NewOrder`/`Accepted`/`ReplaceOrder`/`TradeReport` in
both tests -- is fully real, over real sockets, against real gateways; only
"how does a book update arrive" is test-simulated. `StrategyRuntime` itself
is unit-tested (`test_strategy_runtime.cpp`) against synthetic
`protocol::Event` values the same way `replay::apply_frame_result()`'s own
tests are, so it is proven correct on its own even though nothing yet
drives it from a live socket.

---

## Milestone 12 -- the UI gateway (`ui_gateway/`, `apps/trading_server/`, `ui/`)

The first milestone to close two gaps this document's own "Integration
status" section (below) had documented as open since Milestone 6: there was
no long-running gateway process outside of tests, and `MarketDataPublisher`
was never wired into one. `apps/trading_server/main.cpp` is that process --
it constructs a real `OrderEntryGateway` with a new, purely additive
`OrderEntryGatewayOptions::extra_event_sink` hook (invoked synchronously from
`route_event()`, on the matching thread, alongside `RiskGatedEngine`'s own
`Ledger` wiring) that fans every event out to a real `MarketDataPublisher`,
publishing real UDP frames on a configurable port -- exactly the wiring
Milestone 6 itself proved correct in isolation (`test_market_data_e2e.cpp`)
but never connected to live traffic until now.

`ui_gateway::UiGateway` is the new piece consuming that feed: a persistent
background thread listens on that same UDP port and reconstructs a live
`book::BookManager`, reusing the trader side's own
`replay::apply_frame_result()` pipeline unchanged (the same one
`market_data_replay --listen` already exercises against synthetic UDP --
this is a second, independent, real caller of it, just one that never stops
on idle). Each pre-seeded demo account gets exactly the same
`TraderRiskGatedOms` + `OrderEntryClient` pairing a Milestone 10-11 strategy
uses, connected back to the gateway's own TCP port -- `UiGateway` adds zero
new trading logic; it is purely a protocol adapter. That adapter is REST +
Server-Sent Events, built on `cpp-httplib` (a small, vendored, header-only
library -- the first third-party dependency this project has taken on,
justified in `ui_gateway.hpp`'s own class comment: HTTP/1.1 framing and a
browser dashboard's transport are not this milestone's teaching point the way
every other hand-rolled wire format in this project was for *its* milestone).
SSE, not WebSocket, for the live push channel, for the same reason: cpp-httplib
has no WebSocket support, and every standalone C++ WebSocket library
evaluated was either unproven or a much larger dependency footprint than a
read-mostly dashboard warrants -- SSE needs nothing beyond chunked HTTP
responses cpp-httplib already provides, and the browser's native
`EventSource` needs zero client-side dependencies either. `SseHub` (a small,
mutex/condition-variable-guarded, per-topic "latest value wins" pub/sub, not
a guaranteed-delivery log -- see its own class comment for the tradeoff) is
what `/api/stream` actually reads from; `broadcast_book()`/`broadcast_order()`
are its only two publishers.

`ui/` is a separate React + Vite + TypeScript project (`npm create vite --
--template react-ts`) consuming that REST/SSE surface -- an order book view,
an order-entry form, a positions/cash panel, an orders table with
cancel/replace, and a live activity feed, all kept in sync by one shared
`EventSource` plus the REST calls a *newly selected* account/instrument needs
to hydrate (SSE only pushes deltas; it was never meant to replace an initial
fetch). `npm run build`'s output (`ui/dist`) is served directly by
`trading_server` itself via `UiGatewayOptions::static_files_dir`
(`--static-dir`), so the whole system -- exchange, gateway, market data, UI
API, and dashboard -- is one process a browser can point at with nothing
else running; `npm run dev`'s Vite dev server proxies `/api/*` to
`trading_server` instead, for a fast frontend iteration loop.

`tests/test_ui_gateway.cpp` is this milestone's loop-closing test -- a real
`OrderEntryGateway` + `MarketDataPublisher` + `UiGateway`, driven entirely
through `httplib::Client` against the actual REST/SSE surface, proving (among
other things) that a resting order becomes visible on `GET /api/book/:id`
only because a real UDP frame really was published and received, and that
`/api/stream` really does deliver a live event to a subscribed client after
an order is submitted. Two genuine concurrency bugs surfaced during this
milestone's own manual end-to-end testing before that test file existed --
see `docs/end_to_end_architecture.md`'s "Verified baseline as of Milestone
12" section for both (a `bind_to_any_port`/`bind_to_port` mixup, and a
`UiGateway::stop()` deadlock with two independent causes).

---

## Milestone 13 -- Benchmarks (`benchmarks/`)

The performance half of the milestone `docs/current_system_assessment.md`
originally scoped as "allocation profiling, decode throughput benchmarks,
end-to-end latency (p50/p99/p99.9), comparison of alternative book
representations." Five Google-Benchmark-based (plus one hand-rolled)
executables, added via a new `MDH_BUILD_BENCHMARKS` CMake option
(`FetchContent`, same pattern as `googletest`/`cpp-httplib`/`nlohmann-json`):
`bench_protocol_codec` (market-data and order-entry encode/decode
throughput), `bench_matching_engine` (`MatchingEngine::process()` for
resting/crossing/cancel/replace), `bench_order_book` (the trader-side
reconstructed `book::OrderBook`'s add/cancel/modify/query cost at varying
depth), `bench_spsc_queue` (single- and two-thread `SpscQueue` throughput),
and `bench_end_to_end_latency` (a real, hand-rolled loopback-TCP round trip
against a real `OrderEntryGateway`, since Google Benchmark's fixed-iteration
model can't express a latency *distribution* the way this one needed to).

The single most useful finding, identified from a real measurement plus a
source read rather than assumed: every hot-path component measured here
(codec, matching engine, trader-side book, SPSC queue) costs tens to low
thousands of nanoseconds per operation, while the real, live, real-TCP
order-entry round trip *initially* measured ~0.7-1.3 ms at p50/mean -- three
orders of magnitude larger, traced directly to `OrderEntryGateway::
connection_writer_loop()`'s deliberate 1 ms `sleep_for` poll interval, not
to anything slow inside matching/risk/ledger. That finding was then fixed and
re-measured, not left as a recorded limitation: the writer thread now blocks
on a condition variable notified directly by `route_event()` instead of
polling on a timer (plus `TCP_NODELAY` on every connected `TcpSocket`, a
second latency source the same investigation surfaced), dropping p50 to ~73
μs -- roughly a 17x reduction, confirmed with two independent runs. See
`docs/benchmarks.md` §7 for every number (both before and after), the
machine they were measured on, and the full reasoning behind each one
(including the real `PauseTiming()`/`ResumeTiming()` measurement-artifact
caveat several of the matching-engine/book benchmarks have to account for).

---

## Milestone 14 -- Failure injection & the live demonstration

The final milestone: hardening proof (failure injection against the two
live network-facing components built so far) plus a real, end-to-end,
running demonstration of the whole system.

**Failure injection** (`tests/test_failure_injection_gateway.cpp`,
`tests/test_failure_injection_market_data.cpp`) drives the *live*
`OrderEntryGateway` over real TCP sockets and the *live*
`UiGateway::market_data_loop()` over real UDP datagrams with deliberately
malformed/adversarial/abrupt input -- unrecognized type bytes, corrupted
payload fields under an otherwise-valid header, truncated frames/datagrams,
abrupt disconnects, a slow non-reading client overflowing its outbound
queue, a flood of non-protocol noise, corrupt magic numbers, sequence gaps,
and duplicated packets -- and asserts, for every one, that (1) the
component itself never crashes or hangs, and (2) a fault on one
connection/datagram never degrades or blocks any *other* connection/packet.
This is a strictly stronger claim than the existing unit-level decode-error
tests (`test_order_entry_decode_errors.cpp`, `test_decoder_errors.cpp`)
already made: those prove the codec classifies malformed bytes correctly in
isolation; these prove the live, threaded, buffering components built on
top of it behave correctly when that malformed input arrives over a real
transport, interleaved with real traffic. One real, previously undocumented
behavioral difference was found and is now recorded: the live UI gateway's
market-data path does not halt on a sequence gap the way offline
`replay::run_replay()` does, because `market_data_loop()` discards
`apply_frame_result()`'s stop-signal return value -- see
`docs/failure_injection.md` for the full fault matrix, the reasoning behind
each one, and confirmation that all 11 of these tests pass clean under
ThreadSanitizer too.

**The live demonstration** (`apps/live_strategy_demo/`, new) is what
finally closes the specific gap this document's own "Integration status"
section (below) had listed as still open since Milestone 10: a real
`MarketMakerStrategy`, trading through a real `TraderRiskGatedOms` +
`OrderEntryClient` over a real TCP connection to a real, running
`trading_server`, quoting off a book it gets by polling `UiGateway`'s own
`GET /api/book/:id` REST endpoint (a deliberate, documented simplification
for a demo binary -- see that app's own top comment for why this was chosen
over standing up a second UDP market-data subscriber, which would be new
production networking work, not a demo). It trades as its own dedicated
account (9001, pre-seeded by `trading_server` alongside its existing UI
demo accounts but deliberately never one of them -- see that app's own
comment on why a shared account would make the demonstration ambiguous), so its
resting quotes are visible to every dashboard viewer (the book is public)
while a human -- or, in this demonstration, `curl` exercising the exact same
REST surface a human clicking the dashboard would use -- trades against it
for real. `docs/live_demo.md` walks through one complete real run: seeding
an initial market, the strategy quoting inside it, a demo account crossing
both sides of the strategy's quotes, both processes' independently-computed
fill accounting agreeing exactly, and a real (headless Chrome) screenshot of
the live dashboard reflecting all of it.

---

## Architecture: the live command path

```
                       PRODUCER THREAD                              MATCHING THREAD
                       ────────────────                             ────────────────────
Test / future gateway
builds an ExchangeCommand
(NewOrder / Cancel / Replace)
        │
        ▼
CommandSequencer::sequence()                  <- Milestone 4
  overwrites command_sequence with the
  next value from a producer-owned,
  monotonic (non-atomic) counter
        │
        ▼
SpscQueue<ExchangeCommand>::try_push()  ── full? ──► submit() returns false
  (MatchingPipeline::submit())                        (reject, never silently drop)
        │
        │   lock-free ring buffer, cache-line-padded
        │   head_/tail_ (common/spsc_queue.hpp)
        ▼
                                              SpscQueue::try_pop()      <- MatchingPipeline's
                                                    │                     std::jthread, sole consumer
                                                    ▼
                                              MatchingEngine::process(cmd, sink)     <- Milestone 2
                                                    │
                                   ┌────────────────┼─────────────────────┐
                                   ▼                ▼                     ▼
                          MatchingBook          live_orders_       next_event_sequence_
                          (per instrument:      (AccountId,        (gapless, engine-owned;
                           std::map price       ClientOrderId)      also used by
                           levels, std::list     -> live order       reject_new_order(),
                           FIFO, unordered_map    lookup             see Milestone 5 below)
                           id index)
                                   │
                                   ▼
                     zero or more ExchangeEvent, in command order, delivered
                     synchronously to `sink` on the matching thread:

                       OrderAccepted · OrderRejected · OrderCancelled ·
                       OrderReplaced · TradeExecuted · BookOrderAdded ·
                       BookOrderReduced · BookOrderRemoved
```

`RiskGatedEngine::process()` (Milestone 5) has the exact same signature as
`MatchingEngine::process()` and is designed to substitute for the bare call
inside `MatchingPipeline`'s matching-thread loop — but as of this writing
that substitution has **not** been made by default; `MatchingPipeline` still
calls `engine_.process()` directly unless a caller supplies its own
`Processor` (see `src/exchange/sequencing/matching_pipeline.cpp`). Milestone
7's `OrderEntryGateway` is exactly that caller: it constructs its
`MatchingPipeline` with a `Processor` that forwards to
`risk_gated_engine_.process()`, so live traffic through the gateway *is*
risk-gated end to end — the diagram above (a bare `MatchingEngine`) still
describes `MatchingPipeline`'s own default/no-argument behavior, which
remains what every pre-Milestone-7 test in this document exercises.
`RiskGatedEngine` is fully implemented and tested standalone, single-threaded,
against a bare `MatchingEngine` + `Ledger`. See *Integration status* at the
end of this document.

### The offline path: journaling & replay (Milestone 3)

This is a second, currently-separate path — nothing above writes to a
journal automatically:

```
ExchangeCommand ──encode_command()──► CommandJournalWriter ──► journal file
                                                                (12-byte header +
                                                                 fixed payload per type)
journal file ──CommandJournalReader::next()──► decode_command() ──► fresh MatchingEngine
                                                                          │
                                                                          ▼
                                                        run_command_replay(): every emitted
                                                        event, in order, plus a canonical
                                                        EngineStateSnapshot for the final state
```

`run_command_replay()` builds its own fresh `MatchingEngine` — it does not
share state with whatever process originally produced the journal. Calling
it twice on the same file is the actual determinism proof: identical event
vectors and `EngineStateSnapshot::operator==` both hold.

### Closing the loop: publishing to the trader side (Milestone 6)

A third path, also not (yet) wired to the two above, but tied together by
`test_market_data_e2e.cpp`:

```
ExchangeEvent ──MarketDataPublisher::publish()──► protocol::Event   (only the
        │                                          four public event types;
        │                                          private ones produce
        │                                          nothing, see events.hpp)
        ▼
encode_event() ──► EventFileWriter ──► event file   <- trader side's OWN,
                                                        unmodified code
event file ──EventFileReader──► decode_event() ──► replay::run_replay()
                                                            │
                                                            ▼
                                          book::BookManager  <- trader side's
                                          OWN, unmodified reconstruction
```

Every box below `encode_event()` already existed before this milestone and
needed zero changes — `MarketDataPublisher` is a drop-in producer of
exactly the feed `feed_generator` has always produced synthetically. The
end-to-end test asserts the reconstructed `book::BookManager` (built purely
from wire bytes) agrees with the authoritative `MatchingEngine::snapshot()`
(e.g. a partially-filled order resting at the same reduced quantity on both
sides, or a book both sides agree is now empty after a cancel).

---

## Worked example: one order, end to end

Account `42` submits a GTC buy: 10 shares of instrument `1` at price `101`.
The book already has two resting sell orders on that instrument: 5 @ `100`
(account `7`) and 8 @ `101` (account `9`).

1. **`CommandSequencer::sequence()`** stamps `command_sequence = 57` onto
   the `NewOrderCommand`, discarding whatever placeholder value it arrived
   with.
2. **`MatchingPipeline::submit()`** pushes it onto the SPSC queue; the
   matching thread's `try_pop()` picks it up.
3. *(If risk-gated)* **`RiskEngine::check()`** confirms account `42` has at
   least `101 * 10 = 1010` in *available* (unreserved) cash and that `10` is
   under `RiskLimits::max_order_quantity`. Returns `RejectReason::None`.
4. **`MatchingEngine::process_new_order()`**: price and quantity are valid,
   no duplicate `(account_id, client_order_id)` is live, and it's not FOK
   (so no `crossable_quantity()` pre-check runs). A fresh
   `exchange_order_id = 901` is assigned and `OrderAccepted` is emitted
   (`event_sequence = 201`).
5. **`match_and_rest()`** walks the contra (ask) side:
   - `front_of_best(Sell)` returns the 5 @ `100` order. `101 >= 100` crosses.
     `trade_qty = min(10, 5) = 5`. **`TradeExecuted`** fires at price `100`
     (the *resting* order's price, never the aggressor's) for quantity `5`.
     The resting order's remainder is `0`, so it's removed: **`BookOrderRemoved`**.
   - `front_of_best(Sell)` now returns the 8 @ `101` order. `101 >= 101`
     crosses. `trade_qty = min(5, 8) = 5`. **`TradeExecuted`** fires at price
     `101` for quantity `5`. The resting order's remainder is `3`, so it's
     reduced in place: **`BookOrderReduced`**.
   - The incoming order's remaining quantity is now `0`; the loop ends.
6. **`rest_remainder_if_applicable()`**: nothing remains, so no
   `BookOrderAdded` fires — the order is fully filled, not resting.
7. **`Ledger::apply()`** (Milestone 5), watching this same event stream:
   - `OrderAccepted` (GTC, Buy) opens a hold reserving `101 * 10 = 1010`
     cash against account `42`.
   - Each `TradeExecuted` leg releases the filled slice of that hold at the
     *limit* price (`5 * 101` then `5 * 101` = `1010` released in total) and
     debits `cash_total` at the *actual* trade price (`5 * 100 + 5 * 101 =
     1005`). The `5`-tick difference between what was reserved and what was
     actually spent reappears in `available_cash` automatically — no
     separate refund bookkeeping needed, since `available = total -
     reserved` and both sides of that subtraction moved by the same amount.

Six events, one command, one pass through the book, and a ledger that ends
up consistent with the true (price-improved) cost — all derived from the
same event stream with no extra coordination between `MatchingEngine` and
`Ledger`.

---

## Component reference

### `exchange/core/` — domain vocabulary
- **`types.hpp`** — `AccountId`, `ClientOrderId`, `ExchangeOrderId`,
  `CommandSequence`, `EventSequence`; `OrderType` (currently `Limit` only);
  `TimeInForce` (`GTC`/`IOC`/`FOK`); `RejectReason`, kept to exactly the
  reasons the current milestones need (not an exhaustive real-venue list).
- **`commands.hpp`** — `NewOrderCommand`, `CancelOrderCommand`,
  `ReplaceOrderCommand`, and `ExchangeCommand = std::variant<...>`. Each
  struct has a defaulted `operator==` (used by journal round-trip tests).
- **`events.hpp`** — `OrderAccepted`, `OrderRejected`, `OrderCancelled`,
  `OrderReplaced`, `TradeExecuted` (carries both `TradeCounterparty` legs),
  `BookOrderAdded`/`Reduced`/`Removed`, and `ExchangeEvent = std::variant<...>`.
  The `Book*` events deliberately omit `account_id`/`client_order_id` — they
  describe visible book depth the way an L3 market observer would see it, so
  a future market-data publisher can forward exactly those three without a
  filtering step that could otherwise leak private data by omission.
- **`event_sink.hpp`** — `EventSink = std::function<void(const ExchangeEvent&)>`.
  The matching engine's only output boundary, called synchronously on
  whichever thread is running `process()`. A plain `std::function` rather
  than a virtual interface — nothing here needs a base class or a vtable.

### `exchange/matching/` — the matcher itself
- **`resting_order.hpp`** — `ExchangeRestingOrder`: a live book entry
  (account, client/exchange order ids, side, price, original/remaining
  quantity, TIF, an observability-only `order_sequence`). Deliberately not
  reused with the trader-side `book::RestingOrder` — this one has to decide
  whether to cross, whose order it is, and whether it may rest at all.
- **`matching_book.hpp`/`.cpp`** — `MatchingBook`: per-instrument order book.
  `std::map<Price, std::list<ExchangeRestingOrder>>` per side (`std::greater`
  for bids so `begin()` is best-bid, ascending for asks so `begin()` is
  best-ask), plus an `unordered_map<ExchangeOrderId, Location>` for O(1)
  `remove`/`reduce`/`find`. `front_of_best()`/`reduce_front()`/`remove_front()`
  are the three operations the matching walk actually needs.
- **`state_snapshot.hpp`** — `EngineStateSnapshot`/`InstrumentBookSnapshot`:
  a canonical (instruments sorted by id, each side already in
  price-priority-then-FIFO order) dump of every resting order, so two
  independently-built engines can be compared with `==` regardless of
  `std::unordered_map`'s undefined iteration order.
- **`matching_engine.hpp`/`.cpp`** — `MatchingEngine::process(command, sink)`.
  Dispatches via `std::visit` to `process_new_order`/`process_cancel`/
  `process_replace`. `match_and_rest()` is the core price-time-priority loop;
  `rest_remainder_if_applicable()` decides whether an unfilled remainder
  rests (GTC) or is silently discarded (IOC/FOK). `reject_new_order()`
  (Milestone 5) lets a caller outside `process()` — i.e. `RiskEngine` —
  emit an `OrderRejected` using the engine's own gapless `event_sequence`
  counter. Replace policy: same price and quantity same-or-decreased
  preserves FIFO priority (in-place mutation); a price change or quantity
  increase is cancel-plus-new (new `exchange_order_id`, re-enters matching
  fresh, so a repriced-to-cross replace trades immediately). Self-trade
  prevention is explicitly deferred — orders from the same account match
  each other normally.

### `exchange/persistence/` — journal codec, journal I/O, replay
- **`command_messages.hpp`** — wire format constants: 12-byte header
  (`type`, `reserved`, `payload_size`, `command_sequence`) and
  `payload_size_for(type)` giving each command type's fixed payload size.
  A deliberately separate format from `protocol/messages.hpp` (trader-side
  market-data wire format) — same *style* (big-endian via
  `common/byte_io.hpp`, fixed header + fixed payload), different vocabulary,
  never merged.
- **`command_errors.hpp`** — `CommandDecodeError` (truncated header/payload,
  bad reserved byte, invalid type/size/enum value) — structured returns
  instead of exceptions, decoded via `std::variant<T, CommandDecodeError>`.
- **`command_encoder.hpp`/`.cpp`**, **`command_decoder.hpp`/`.cpp`** —
  `encode_command()` appends one command's wire bytes to a caller-owned
  buffer; `decode_command_header()`/`decode_command()` decode them back,
  mirroring `protocol::decode_header`/`decode_event`'s two-step shape (peek
  the header to learn the payload size, then decode the full frame).
- **`command_journal_writer.hpp`/`.cpp`**, **`command_journal_reader.hpp`/`.cpp`** —
  `CommandJournalWriter::write()` appends encoded frames to a file with no
  extra framing beyond each frame's own header. `CommandJournalReader::next()`
  reads one frame at a time, returning `std::nullopt` at a clean EOF or a
  `CommandDecodeError` for a corrupt/truncated frame. Both mirror
  `replay::EventFileWriter`/`EventFileReader` exactly, applied to
  `ExchangeCommand` instead of `protocol::Event`.
- **`state_hash.hpp`/`.cpp`** — `hash_state_snapshot()`: a 64-bit FNV-1a
  fingerprint of an `EngineStateSnapshot`, for a one-line "did the final
  state match" assertion or log line on top of (not instead of) direct
  `EngineStateSnapshot` equality.
- **`command_replay.hpp`/`.cpp`** — `run_command_replay(journal_path,
  options)`: reads a journal end to end (or stops at the first decode error,
  per `CommandReplayOptions::stop_on_decode_error`), replaying into a fresh
  `MatchingEngine` and returning a `CommandReplayOutcome` (final engine,
  every event emitted, commands processed, stop reason if any). A distinct
  concept from `replay::run_replay()` — that one replays market-data events
  into the trader-side `book::BookManager`; this one replays exchange
  *commands* into the authoritative `MatchingEngine`. Neither reuses the
  other.

### `exchange/sequencing/` — ordering & the matching thread
- **`command_sequencer.hpp`/`.cpp`** — `CommandSequencer::sequence(command)`:
  overwrites whichever `ExchangeCommand` alternative's `command_sequence`
  field with the next value from a monotonic, non-atomic counter, and
  returns it. Producer-thread-only, by the same single-writer contract
  `SpscQueue`'s own `head_`/`tail_` split relies on.
- **`matching_pipeline.hpp`/`.cpp`** — `MatchingPipeline`: owns a
  `SpscQueue<ExchangeCommand>`, a `CommandSequencer`, a `MatchingEngine`, and
  a `std::jthread` that loops `try_pop()` → `engine_.process()` until told to
  stop (and drained). `submit()` (producer-only) checks queue occupancy
  *before* sequencing — a command that never enters the queue must not
  consume an authoritative `CommandSequence` value, or the sequence stream
  would show a permanent gap for a command the matcher never saw. `stop()`
  requests a stop and joins only after the queue has fully drained (never
  mid-drain); `snapshot()` is only safe to call after that join has
  happened. `MatchingPipelineOptions::matching_delay` lets a test
  deterministically simulate a slow matcher to exercise the backpressure
  path on demand, rather than depending on incidental scheduling.

### `exchange/ledger/` — account balances
- **`ledger.hpp`/`.cpp`** — `Ledger`: per-account `cash_total`/`cash_reserved`
  and per-`(account, instrument)` `position_total`/`position_reserved`.
  `deposit_cash()`/`deposit_position()` are the only way balances start
  non-zero (there is no `DepositCommand` in this project's command set —
  funding is out of scope, seeding is test/admin-only). `apply()` is an
  `EventSink`-shaped callable (`sink()` returns one bound to `this`) that
  watches `OrderAccepted`/`OrderReplaced`/`OrderCancelled`/`TradeExecuted`;
  `OrderRejected` and every `Book*` event are no-ops here (no account info
  to act on). Reservations exist **only** for resting GTC orders, keyed by
  `(account_id, client_order_id)` — the same key `MatchingEngine` uses for
  `live_orders_` — specifically so a replace's cancel-plus-new path (which
  can assign a brand-new `exchange_order_id`) still resolves correctly via
  `OrderReplaced`'s `original_client_order_id`/`new_client_order_id` pair.
  IOC/FOK orders never reserve at all: they resolve entirely within one
  `process()` call with nothing else interleaved, so there's no window for a
  double-spend to exploit, and reserving for them would create an
  unreleasable hold (IOC's discarded remainder fires no event to release it
  against).

### `exchange/risk/` — pre-trade checks
- **`risk_engine.hpp`/`.cpp`** — `RiskEngine::check(...)`: read-only
  pre-trade checks against a `NewOrderCommand` or `ReplaceOrderCommand` —
  order size vs `RiskLimits::max_order_quantity`, then available cash /
  position. For a replace, only the *extra* exposure beyond the original
  order's existing reservation must fit in available resources
  (`max(0, new_required - old_required)`); a shrink always passes the
  balance check. Returns `RejectReason::None` or a specific reason; never
  mutates the ledger (reservation happens later, via `Ledger::apply()`
  watching the resulting `OrderAccepted` / `OrderReplaced`).
  `CancelOrderCommand` is not checked — it cannot increase exposure.
- **`risk_gated_engine.hpp`/`.cpp`** — `RiskGatedEngine::process(command,
  sink)`: same signature as `MatchingEngine::process()`. Runs
  `RiskEngine::check()` first for a `NewOrderCommand` or
  `ReplaceOrderCommand`; on reject, calls
  `MatchingEngine::reject_new_order()` /
  `reject_replace_order()` (so the rejection still consumes a
  real, gapless `event_sequence`) and stops — the command never reaches
  `MatchingEngine::process()` at all, and a rejected replace leaves the
  resting order and its hold untouched. On pass (or for Cancel, which
  skips the check), delegates to the engine and feeds every emitted
  event to `Ledger::apply()` *before* forwarding it to the caller's `sink` —
  so by the time a caller observes an event, the ledger is already
  consistent with it. Deliberately keeps risk-check-then-reserve
  single-threaded, in strict command order, on whatever one thread drives
  it — splitting "check" and "reserve" across two different points in time
  (as the architecture diagram's box ordering alone would suggest) would
  reopen the exact double-spend race reservations exist to prevent.

### `exchange/market_data/` — translating to the trader-side wire format
- **`market_data_publisher.hpp`/`.cpp`** — `MarketDataPublisher::publish(event,
  sink)`: `std::visit`-dispatches to one helper per public event type
  (`BookOrderAdded` → `protocol::AddOrder`, `BookOrderReduced` →
  `protocol::ModifyOrder`, `BookOrderRemoved` → `protocol::CancelOrder`,
  `TradeExecuted` → `protocol::Trade`, stripping both legs'
  account/order-id fields); the four private event types match no branch
  and produce nothing. `sink(downstream)` binds a `MarketDataSink` into an
  `EventSink`-shaped callable, the same composable shape `Ledger::sink()`
  uses, so a publisher can be fanned out alongside one. Owns its own
  monotonic `Sequence` counter, incremented only when a wire message is
  actually emitted (a private event must not consume a sequence number,
  the same reasoning `MatchingPipeline::submit()` applies to
  `CommandSequence`) — and an injectable `clock` (defaults to real
  `std::chrono::system_clock`) for `timestamp_ns`, so tests aren't at the
  mercy of wall-clock time despite this being the one place in `exchange/`
  that's allowed to read it.

---

## Design decisions worth knowing before reading the code

- **No wall-clock time, thread scheduling, or randomness anywhere inside
  `MatchingEngine::process()`.** Every decision depends only on the
  command's own fields and current book state — this is what makes replay
  determinism (Milestone 3) provable at all, not just probable.
- **`std::variant` + `std::visit`, not class hierarchies**, for both
  commands and events. No virtual dispatch, no heap-allocated polymorphic
  event objects, and exhaustiveness is checked by the compiler at every
  `std::visit` call site.
- **`EventSink` (a plain `std::function`) is the one, uniform way every
  consumer — the engine's own callers, `Ledger`, tests — observes what
  happened.** No separate observer interface, no event bus.
- **Structured `RejectReason`/`CommandDecodeError` enums, not exceptions**,
  for every "this didn't work" outcome, matching this codebase's existing
  convention on the trader side (`protocol::DecodeError`, `book::BookError`).
- **Reject, never silently drop, at the command-submission boundary.**
  `DroppingQueue` (market data) and `MatchingPipeline`'s SPSC queue (inbound
  commands) make opposite backpressure choices on purpose: a dropped
  market-data frame is a detectable, recoverable sequence gap downstream; a
  silently dropped *order* leaves a client with no way to know their request
  was never seen.
- **Risk-check-then-reserve stays single-threaded and in strict command
  order** (`RiskGatedEngine`), even though the architecture doc's box
  diagram draws risk *before* sequencing and ledger updates *after*
  matching. Splitting them across two different points in wall-clock time
  would reopen a double-spend race; keeping both immediately around the
  matching call, on one thread, closes it.
- **A resting order's *limit* price is reserved, not the eventual trade
  price.** Price improvement on the aggressor side of a trade reappears in
  `available_cash`/`available_position` automatically, because `available =
  total - reserved` and both terms move together — no separate refund
  bookkeeping.

---

## Building & testing

Same build as the rest of `mdh` — there is no separate build target for the
exchange side; `exchange/*.cpp` are compiled into `mdh_core` alongside the
trader-side sources (see the top-level `CMakeLists.txt`), and its tests are
part of the single `mdh_tests` binary.

```bash
# Configure + build (Debug, from the mdh/ directory)
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build

# Run everything (trader-side + exchange-side tests together)
ctest --test-dir build --output-on-failure

# Run just the exchange-side tests
ctest --test-dir build --output-on-failure -R "Exchange|Matching|Command|Ledger|Risk"

# Sanitizer builds (separate build directories, per CMakeLists.txt)
cmake -S . -B build-asan -DMDH_ENABLE_ASAN=ON -DMDH_ENABLE_UBSAN=ON && cmake --build build-asan && ctest --test-dir build-asan
cmake -S . -B build-tsan -DMDH_ENABLE_TSAN=ON && cmake --build build-tsan && ctest --test-dir build-tsan
```

Exchange-side test files, by milestone:

| Milestone | Test files |
|---|---|
| 1 — core types/commands/events | `test_exchange_commands.cpp`, `test_exchange_events.cpp` |
| 2 — matching engine | `test_matching_book.cpp`, `test_matching_engine.cpp` |
| 3 — journal & replay | `test_command_codec.cpp`, `test_command_decode_errors.cpp`, `test_command_journal.cpp`, `test_exchange_replay.cpp` |
| 4 — sequencer & pipeline | `test_command_sequencer.cpp`, `test_matching_pipeline.cpp` |
| 5 — ledger & risk | `test_ledger.cpp`, `test_risk_engine.cpp`, `test_risk_gated_engine.cpp` |
| 6 — market-data publisher | `test_market_data_publisher.cpp` (unit-level translation), `test_market_data_e2e.cpp` (loop-closing round-trip through the trader side's own replay pipeline) |
| 7 — TCP order-entry gateway | `test_tcp_socket.cpp` (RAII socket wrapper), `test_order_entry_codec.cpp`, `test_order_entry_decode_errors.cpp` (wire codec), `test_order_entry_gateway_e2e.cpp` (loop-closing, real TCP client against a real gateway) |
| 8 — trader-side OMS + client | `test_order_management_system.cpp` (pure state-machine logic, fake sender), `test_order_entry_client.cpp` (transport, real socket against a raw peer), `test_oms_gateway_e2e.cpp` (loop-closing, production OMS + client against a real Milestone 7 gateway) |
| 9 — trader-side positions/risk | `test_position_tracker.cpp`, `test_trader_risk_engine.cpp` (pure logic, synthetic fills/checks), `test_trader_risk_gated_oms.cpp` (composition, fake sender), `test_trader_risk_gated_oms_e2e.cpp` (loop-closing, production risk-gated OMS against a real Milestone 7 gateway, including both independent risk layers rejecting on their own) |
| 10 — strategy runtime + market maker | `test_strategy_runtime.cpp` (pure dispatch logic, synthetic events), `test_market_maker_strategy.cpp` (composition, fake sender), `test_market_maker_strategy_e2e.cpp` (loop-closing, a real market maker quoting/getting filled/requoting over a real Milestone 7 gateway) |
| 11 — additional strategies, two-venue simulation | `test_cross_venue_arbitrage_strategy.cpp` (composition, two fake senders), `test_cross_venue_arbitrage_strategy_e2e.cpp` (the "two-venue simulation" -- two complete, independent exchange stacks, one arbitrage strategy trading both over real TCP) |
| 12 — UI gateway | `test_order_entry_gateway_e2e.cpp` (new cases covering `extra_event_sink`), `test_ui_gateway.cpp` (loop-closing, a real gateway + `MarketDataPublisher` + `UiGateway`, driven through `httplib::Client` against the real REST/SSE surface) |

`test_matching_pipeline.cpp`, `test_risk_gated_engine.cpp`,
`test_order_entry_gateway_e2e.cpp`, `test_oms_gateway_e2e.cpp`,
`test_trader_risk_gated_oms_e2e.cpp`, `test_market_maker_strategy_e2e.cpp`, and
`test_cross_venue_arbitrage_strategy_e2e.cpp` are the ones that most benefit
from running under TSan — all exercise real, multi-threaded traffic
(`MatchingPipeline`'s producer/matching-thread split, one or more gateways'
per-connection reader/writer threads plus their matching threads, and one or
more OMS clients' own reader threads). All 351 tests as of Milestone 12 pass
clean under a debug build, an ASan+UBSan build, and a TSan build alike (this
includes the 13 new tests in `test_ui_gateway.cpp`, themselves genuinely
multi-threaded: a gateway's accept/reader/writer/matching threads, a
`UiGateway`'s own market-data thread and per-session `OrderEntryClient`
reader threads, and cpp-httplib's own HTTP worker threads, all running
concurrently). TSan has now caught two real, if narrow, bugs across this
project's development, both in test infrastructure rather than production
code (a third, real but non-TSan-detected concurrency bug from Milestone 12 —
a `std::jthread`/`std::stop_token` mixup causing `UiGateway::stop()` to hang —
is documented in `docs/end_to_end_architecture.md`'s Milestone 12 section
instead, since it was a deadlock, not a data race, and TSan does not detect
deadlocks):
- During Milestone 8: both gateway e2e tests were calling
  `OrderEntryGateway::snapshot()` — which, like `MatchingPipeline::snapshot()`,
  is only safe once the matching thread has been joined — while the gateway
  was still running; both now call `stop()` first.
- During Milestone 10-11's own development: the `RiskGatedTrader` test
  helper (shared by the Milestone 9 and Milestone 10-11 e2e tests) declared
  its `OrderEntryClient` member *before* its `TraderRiskGatedOms` member.
  `OrderEntryClient`'s background reader thread calls into the risk-gated
  OMS asynchronously, and C++ destroys members in reverse declaration order
  — so the OMS could start being destroyed while that thread was still
  calling into it. All three `RiskGatedTrader` copies now declare
  `TraderRiskGatedOms` first (see any of their own comments for the full
  reasoning), so the client — and the thread it owns — finishes tearing down
  before the OMS it calls into does.

---

## Integration status: what's wired together, and what isn't yet

As of Milestone 12, real client traffic can flow all the way from a TCP
socket through risk-gated matching and back out to five different kinds of
client (a hand-rolled test client, the production `OrderEntryClient` +
`OrderManagementSystem`, that same pair further wrapped in
`TraderRiskGatedOms` for a second, independent trader-side risk check and
live position/P&L tracking, that `TraderRiskGatedOms` driven by a
`strategies::MarketMakerStrategy`/`CrossVenueArbStrategy` instead of a test
calling `submit_new_order()` directly, or that same `TraderRiskGatedOms`
driven by an HTTP request through the new `ui_gateway::UiGateway`) — see
Milestones 7–12 above and their respective e2e tests. As of this same
milestone, there is also a real long-running process outside of tests --
`apps/trading_server/main.cpp` — and `MarketDataPublisher` is wired into its
matching thread for real, closing the two items this section had previously
listed as the most-open gaps. As of Milestone 14, a sixth kind of client
exists too: `apps/live_strategy_demo/`'s real `MarketMakerStrategy`, trading
against a real, running `trading_server` -- see that milestone's own section
above and `docs/live_demo.md` for a full real run. What's still **not**
assembled into one live call path or app:

- `MatchingPipeline` (Milestone 4) itself still drives a bare
  `MatchingEngine` directly *by default* — `RiskGatedEngine` (Milestone 5)
  is only substituted in by a caller that supplies its own `Processor`, which
  `OrderEntryGateway` (Milestone 7) does. Nothing about `MatchingPipeline`
  changed to make that possible; it was designed for exactly this
  substitution from Milestone 4 onward.
- Nothing currently calls `CommandJournalWriter::write()` from the
  gateway's matching thread — journaling (Milestone 3) is exercised by its
  own tests and by `run_command_replay()` against journal files built
  directly via `encode_command`/`CommandJournalWriter`, not against live
  gateway traffic. `trading_server` (Milestone 12) fans matching-thread
  events out to `Ledger::sink()` (via `RiskGatedEngine`) *and*
  `MarketDataPublisher::sink()` (via the new `extra_event_sink` hook) but
  still not to a `CommandJournalWriter` -- a live gateway process still
  keeps no durable command journal of its own traffic.
- `strategies::StrategyRuntime` (Milestone 10) itself -- the dispatch class,
  literally -- is still proven correct only in isolation, against synthetic
  `protocol::Event` values feeding a `book::BookManager`; nothing calls
  `StrategyRuntime::on_event()` from a live UDP feed. What Milestone 14's
  `apps/live_strategy_demo` *does* close is the narrower, more concretely
  useful gap: a real `MarketMakerStrategy` (the strategy `StrategyRuntime`
  would otherwise dispatch to) now does trade, live, against a real running
  `trading_server`, driven by book snapshots it polls from `UiGateway`'s own
  REST API rather than by `StrategyRuntime` reacting to raw UDP frames — see
  that milestone's own section above for exactly why that specific,
  narrower substitution was the right scope for a demo binary. Binding
  `StrategyRuntime` itself directly to the raw UDP feed (so a strategy could
  react to a book update within microseconds of the packet arriving, not
  within one REST-poll interval) remains open, real, future work.

This is intentional, incremental milestone scope, not an oversight — each
piece is built and proven correct on its own (and, as of Milestone 6, proven
correct in combination with the trader side's replay pipeline; as of
Milestones 7–11, proven correct end to end over real TCP connections between
real gateways and real clients, including two independent gateways at once
as of Milestone 11; as of Milestone 12, proven correct end to end over a
real long-running process, real UDP market data, and a real browser
dashboard; as of Milestone 14, proven resilient to a real, documented matrix
of network-level faults on both live components, and demonstrated once, for
real, end to end, with a real strategy, a real counterparty, and a real
browser screenshot) before being composed further. See
`docs/end_to_end_architecture.md` for the full milestone table,
`docs/benchmarks.md` for performance, `docs/failure_injection.md` for the
fault matrix, and `docs/live_demo.md` for the final run. This document
should be updated as any further wiring happens.
