# End-to-End Architecture — Trader Side (existing) and Exchange Side (planned)

**Status:** originally a Milestone 0 deliverable, describing a system where no
exchange-side production code existed yet. As of Milestone 11, Milestones 1–11 below are
implemented and tested (see `docs/exchange_flow.md` for the exchange-side walkthrough);
Milestone 12 onward remain a map for work not yet started, still clearly labeled as such
in the sections below.

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

                     Later, outside the latency-sensitive path on both halves:
                     UI gateway (REST/WebSocket) + React dashboard  (Milestone 12)
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
is not yet fed by a live UDP feed against a running gateway, since that
wiring (`MarketDataPublisher` into a live gateway's matching thread) remains
the same not-yet-done integration item described below. Milestone 12 onward
(the UI gateway, benchmarks, and failure injection) remain not yet started.

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
| UI gateway + React dashboard | 12 | Not started | `ui_gateway/`, `ui/` |
| Benchmarks | 13 | Not started | `benchmarks/` |
| Failure injection + final demonstration | 14 | Not started | across the above |

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
