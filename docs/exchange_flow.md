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

It walks through the five milestones built so far in the order they were
built, then traces one order through the whole stack end to end, then gives
a directory-by-directory component reference.

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
that substitution has **not** been made; `MatchingPipeline` still calls
`engine_.process()` directly (see `src/exchange/sequencing/matching_pipeline.cpp`).
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
- **`risk_engine.hpp`/`.cpp`** — `RiskEngine::check(command, ledger)`:
  read-only pre-trade checks against a `NewOrderCommand` — order size vs
  `RiskLimits::max_order_quantity`, available cash for a buy, available
  position for a sell. Returns `RejectReason::None` or a specific reason;
  never mutates the ledger (reservation happens later, via `Ledger::apply()`
  watching the resulting `OrderAccepted`). Only `NewOrderCommand` is
  checked — `CancelOrderCommand` can't increase exposure, and
  `ReplaceOrderCommand` is a documented, deliberate gap (see the class
  comment in `risk_engine.hpp` for why: the priority-preserving replace path
  never increases exposure, and the cancel-plus-new path is allowed to
  overdraw rather than being rejected, for now).
- **`risk_gated_engine.hpp`/`.cpp`** — `RiskGatedEngine::process(command,
  sink)`: same signature as `MatchingEngine::process()`. Runs
  `RiskEngine::check()` first for a `NewOrderCommand`; on reject, calls
  `MatchingEngine::reject_new_order()` (so the rejection still consumes a
  real, gapless `event_sequence`) and stops — the command never reaches
  `MatchingEngine::process()` at all. On pass (or for Cancel/Replace, which
  skip the check entirely), delegates to the engine and feeds every emitted
  event to `Ledger::apply()` *before* forwarding it to the caller's `sink` —
  so by the time a caller observes an event, the ledger is already
  consistent with it. Deliberately keeps risk-check-then-reserve
  single-threaded, in strict command order, on whatever one thread drives
  it — splitting "check" and "reserve" across two different points in time
  (as the architecture diagram's box ordering alone would suggest) would
  reopen the exact double-spend race reservations exist to prevent.

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

`test_matching_pipeline.cpp` and `test_risk_gated_engine.cpp` are the two
that most benefit from running under TSan — both exercise real threading
(`MatchingPipeline`'s producer/matching-thread split) or state a future
threaded caller must get right (`RiskGatedEngine`'s single-thread
requirement).

---

## Integration status: what's wired together, and what isn't yet

Every component above is implemented and independently tested, but as of
this writing they have **not** all been assembled into one live call path
or app:

- `MatchingPipeline` (Milestone 4) drives a bare `MatchingEngine` directly —
  `RiskGatedEngine` (Milestone 5) is not substituted in.
- Nothing currently calls `CommandJournalWriter::write()` from
  `MatchingPipeline`'s matching thread — journaling (Milestone 3) is
  exercised by its own tests and by `run_command_replay()` against
  journal files built directly via `encode_command`/`CommandJournalWriter`,
  not against live pipeline traffic.
- There is no exchange-side equivalent yet of `market_data_replay`'s
  `main.cpp` — no app wires a `CommandSequencer` + `MatchingPipeline` +
  `RiskGatedEngine` + `CommandJournalWriter` together and runs it against
  real input (a file, a socket, or a TCP order-entry session).

This is intentional, incremental milestone scope, not an oversight — each
piece is built and proven correct on its own before being composed. See
`docs/end_to_end_architecture.md` for the target composition and the
still-to-come milestones (a wire-level order-entry protocol, a live gateway,
market-data publication of the `Book*` events). This document should be
updated once that wiring happens.
