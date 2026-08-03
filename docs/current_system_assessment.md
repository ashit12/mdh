# Current System Assessment — `mdh` Repository

**Assessed:** 2026-08-04
**Repo:** `~/Personal/CPP-Projects/mdh`, branch `main`, HEAD `5a42c61` ("add SPSc queue and dropping logic with tests"), plus uncommitted working-tree changes implementing milestone 4 (see §3).
**Scope of this document:** repository assessment only. No production source was modified. Two scratch build directories (`/tmp/mdh-build-assess`, `/tmp/mdh-build-asan`, `/tmp/mdh-build-tsan`) and one scratch event file were created outside the repo for verification and can be discarded.

---

## 1. Executive summary

**Headline finding, stated plainly because it changes everything downstream in this report: there is no matching engine in this repository, at any milestone.** The task brief that requested this assessment describes a "low-latency order book and matching engine... completed through approximately milestone 5," listing price-time-priority matching, partial/complete fills, and an SPSC queue "connecting producer and matching threads." None of that exists in `~/Personal/CPP-Projects/mdh` as it stands today. What exists instead is a **market-data feed handler and order-book *reconstruction* system**, explicitly scoped and documented as such:

> "This is a simulated, ITCH/OUCH-inspired protocol... It has no matching engine and no persistence yet." — [README.md:8-10](../README.md)
> "No matching engine: since nothing here matches a crossing bid against a resting ask, a reconstructed book *can* show a crossed spread." — [README.md:215-219](../README.md)

The project is at **milestone 4 of its own 6-milestone roadmap** (README.md:15), not milestone 5 of a matching-engine roadmap. Its own milestone 5 is "allocation profiling, decode throughput benchmarks, end-to-end latency... comparison of alternative book representations" (README.md:626-627) — benchmarking, which the brief's milestone list also treats as a milestone-5-adjacent concern, but for a fundamentally different subsystem.

Git history confirms this isn't a naming coincidence or partial migration: an earlier, unrelated exercise repo *did* contain a `LimitOrderBook/` directory with `matching_engine.hpp`/`matching_engine.cpp` (commit `f1f58bf`, "add uts and matching engine logic"), but that entire directory tree is absent from every commit reachable from the current `mdh` project, which begins fresh at commit `3543670` ("initial project structure for mdh"). `git branch -a` shows only `main` (local) and `origin/main` — no other branch holds the missing matching engine. It is simply not part of this codebase.

What *is* here, and what it's actually good for, is genuinely solid: a hand-rolled big-endian binary wire protocol with structured (non-exception) error handling, a file- and UDP-based replay pipeline, a lock-free SPSC ring buffer connecting a UDP-receive thread to a book-reconstruction thread with a tested drop-newest backpressure policy, sequence-gap detection with snapshot-based recovery, and 103 GoogleTest tests that pass cleanly under plain debug, ASan+UBSan, and ThreadSanitizer builds (verified in this assessment, see §3). This maps naturally onto the **trading-firm side** of the target platform — specifically the market-data feed handler and locally reconstructed order book — not onto the exchange's matching core. Building the actual matching engine, the exchange command/event model, the gateway, risk, ledger, and OMS is new work, not extraction.

The rest of this report evaluates the codebase honestly against that reality: what's here, how good it is at what it actually does, and how far away the target platform's exchange-side matching core is (very far — it doesn't exist yet) versus its trading-firm-side feed handler (comparatively close).

---

## 2. Current repository map

Only architecturally meaningful paths shown. Line counts from `wc -l` (verified 2026-08-04).

```
mdh/
├── CMakeLists.txt                        (102 lines — see §3)
├── README.md                             (631 lines — architecture, design rationale, milestones)
├── docs/protocol.md                      (315 lines — wire-format spec)
├── include/
│   ├── common/
│   │   ├── types.hpp          (27)   Price/Quantity/OrderId/InstrumentId/Side — shared primitives
│   │   ├── byte_io.hpp        (114)  explicit-shift big-endian encode/decode, bounds-checked ByteReader
│   │   ├── sequence_validator.hpp (63)  InOrder/Duplicate/OutOfOrder/Missing classifier
│   │   ├── spsc_queue.hpp     (136)  lock-free bounded SPSC ring buffer
│   │   └── dropping_queue.hpp (52)   wraps SpscQueue with drop-newest + counter policy
│   ├── protocol/
│   │   ├── messages.hpp       (96)   MessageType, Header, AddOrder/CancelOrder/ModifyOrder/Trade/ClearBook, Event variant
│   │   ├── decoder.hpp        (27)   decode_header/decode_event signatures
│   │   ├── encoder.hpp        (15)
│   │   └── errors.hpp         (28)   DecodeError enum
│   ├── replay/
│   │   ├── event_file_reader.hpp (34)
│   │   ├── event_file_writer.hpp (29)
│   │   ├── replay_engine.hpp  (90)   ReplayOptions/ReplayOutcome, run_replay(), apply_frame_result() — the shared core
│   │   ├── replay_stats.hpp   (35)   ReplayStats counters
│   │   └── snapshot.hpp       (56)   book-state snapshot format (milestone 4)
│   ├── book/
│   │   ├── book_errors.hpp    (24)   BookError enum
│   │   ├── price_level.hpp    (48)   PriceLevel: std::list<RestingOrder> + aggregate qty
│   │   ├── order_book.hpp     (82)   OrderBook: bids_/asks_ std::map + order_index_ unordered_map
│   │   └── book_manager.hpp   (40)   BookManager: OrderBook + InstrumentStats per InstrumentId
│   └── net/
│       ├── udp_socket.hpp     (65)   RAII POSIX UDP socket
│       ├── udp_receiver.hpp   (55)   batched non-blocking receive
│       ├── packet.hpp         (88)   UDP packet framing (batches event frames per datagram)
│       ├── packet_sequence_tracker.hpp (61)  observational packet-level seq tracking
│       └── udp_listener.hpp   (64)   run_udp_listen() — the two-thread producer/consumer pipeline
├── src/                                    (mirrors include/, implementations)
│   ├── book/order_book.cpp    (143)
│   ├── book/book_manager.cpp  (35)
│   ├── protocol/decoder.cpp   (166), encoder.cpp (65)
│   ├── replay/replay_engine.cpp (140), snapshot.cpp (130), event_file_reader.cpp (47), event_file_writer.cpp (16)
│   ├── common/sequence_validator.cpp (36)
│   └── net/udp_listener.cpp (114), udp_receiver.cpp (41), udp_socket.cpp (94), packet.cpp (95)
├── apps/
│   ├── feed_generator/main.cpp     (219)  deterministic seeded synthetic feed generator
│   ├── market_data_replay/main.cpp (236)  file OR --listen UDP replay + stats + snapshot in/out
│   └── udp_sender/main.cpp         (155)  streams an event file over UDP, batched
└── tests/                                 16 files, 103 TEST() cases total (see §3)
```

**Total handwritten C++ (excluding tests, `.cache/`, build dirs):** ~3,061 lines across headers/src/apps. Tests: 2,122 lines. `.cache/clangd/` is clangd's index cache (generated, ignorable). `.vscode/` is editor config. No CI configuration file (no `.github/workflows/`, no `.gitlab-ci.yml`) exists in the repo.

**Main executables:** `feed_generator`, `market_data_replay`, `udp_sender` — all thin CLI wrappers over `mdh_core`. **Library:** `mdh_core` (static), containing everything under `src/`. **Test binary:** `mdh_tests` (GoogleTest, discovered via `gtest_discover_tests`).

**Entry points / hot vs. cold path:** The single meaningfully "hot" path is `net::run_udp_listen()`'s two `jthread`s ([udp_listener.cpp:37-98](../src/net/udp_listener.cpp)) — a receive/decode producer and a validate/apply consumer. Everything else (`feed_generator`, `udp_sender`, file-mode `market_data_replay`, snapshot I/O) is single-threaded, cold-path CLI-tool code with no latency sensitivity designed in or claimed.

**Existing network boundary:** POSIX UDP sockets only (`net/udp_socket.hpp`), unicast, IPv4-literal-only, no TLS/auth — appropriate for a portfolio project, not a boundary a real exchange gateway would reuse as-is.

**Existing persistence boundary:** none in the durable-storage sense. `EventFileWriter`/`EventFileReader` read/write flat binary files for replay and snapshotting; there is no database, no WAL beyond the replay file itself, and no crash-recovery story beyond "replay the file from the start" or "load the one snapshot named on the command line."

---

## 3. Build and test status

All commands below were actually executed during this assessment (not assumed from README claims).

### 3.1 Debug configure + build

```bash
cmake -S . -B /tmp/mdh-build-assess
cmake --build /tmp/mdh-build-assess -j
```
**Result: succeeded, zero warnings.** GoogleTest fetched live via `FetchContent` (network access required — this will fail offline or in a CI sandbox with no network egress, which is worth flagging for a future CI setup). Compiled with `-Wall -Wextra -Wpedantic -Wshadow -Wconversion` ([CMakeLists.txt:17](../CMakeLists.txt)) and genuinely warning-clean, matching the README's claim.

### 3.2 Test run (debug)

```bash
ctest --test-dir /tmp/mdh-build-assess --output-on-failure
```
**Result: 103/103 tests passed**, 4.05s wall. Matches README's stated "103 tests as of milestone 4."

### 3.3 Sanitizer builds

```bash
cmake -S . -B /tmp/mdh-build-asan -DMDH_ENABLE_ASAN=ON -DMDH_ENABLE_UBSAN=ON && cmake --build /tmp/mdh-build-asan -j
cmake -S . -B /tmp/mdh-build-tsan -DMDH_ENABLE_TSAN=ON && cmake --build /tmp/mdh-build-tsan -j
```
Both **configured and built successfully**, no additional warnings.

### 3.4 Test run under sanitizers

- **ASan + UBSan:** 103/103 passed, 11.14s. No leaks, no UB, no address errors reported.
- **ThreadSanitizer:** 103/103 passed, 41.71s. No data races reported — meaningful here specifically because `test_spsc_queue.cpp` includes a genuine multi-threaded producer/consumer stress test (`SpscQueue.ConcurrentProducerConsumerPreservesFifoOrderAndCount`) and `test_udp_replay_e2e.cpp`/`test_backpressure_integration.cpp` exercise the real two-thread `run_udp_listen()` pipeline over real loopback sockets, not mocks. A clean TSan run on the *actual* concurrency primitive (not a simplified stand-in) is a genuine, verified correctness signal, not just a checkbox.

### 3.5 Smoke test of the actual applications

```bash
feed_generator --output /tmp/mdh-smoke-events.bin --orders 2000 --seed 7
market_data_replay --input /tmp/mdh-smoke-events.bin --top-levels 2
```
Ran cleanly: 2,830 events generated (2,000 adds + derived cancels/modifies/trades/clears), replayed with 0 decode failures, 0 sequence failures, 0 book errors, correct-looking top-of-book output. Confirms the documented pipeline actually works end-to-end, not just under test mocks.

### 3.6 What was **not** run, and why

- **Benchmarks:** none exist to run. No `benchmarks/` directory, no Google Benchmark dependency, no `--benchmark` mode in any app, and no `rdtsc`/`__rdtsc`/CPU-cycle-counter code anywhere in the tree (verified via `grep -rniE "rdtsc|benchmark|perf_event|affinity"` across `include/`, `src/`, `apps/`, `tests/`, `CMakeLists.txt` — the only hit is a comment in `replay_stats.hpp` explicitly disclaiming any benchmark claim). This is fully consistent with the README's own roadmap, which places "allocation profiling, decode throughput benchmarks, end-to-end latency (p50/p99/p99.9)" at **its** milestone 5 (README.md:626), not yet reached.
- **CI:** no CI configuration exists to check.
- **Static analysis (clang-tidy, cppcheck, etc.):** none configured in the build; not run separately for this assessment beyond the sanitizers above, which is where the highest-value signal already was.

### 3.7 Working-tree state (uncommitted changes)

`git status` shows **modifications to 13 tracked files and 4 untracked new files** (`include/replay/snapshot.hpp`, `src/replay/snapshot.cpp`, `tests/test_sequence_recovery.cpp`, `tests/test_snapshot.cpp`) relative to HEAD (`5a42c61`, milestone 3). This is milestone 4 (snapshot/recovery) work, fully implemented and tested (as verified above) but **not committed**. Anyone resetting or cleaning this working tree would lose it. Flagged here rather than acted on, per this assessment's read-only scope — see the immediate next tasks in §16.

---

## 4. Existing system architecture

The system is one pipeline with two frame sources feeding the same core logic (see the ASCII diagram already in [README.md:52-105](../README.md), which was verified against the code and found accurate):

```
file:  EventFileReader.next() ──┐
                                  ├──► apply_frame_result() ──► BookManager (+ ReplayStats)
UDP:   producer thread:          │        (SequenceValidator classifies;
       receive_batch()→          │         snapshot recovery on Missing)
       unpack_frames()→          │
       DroppingQueue.push() ─────┘
       consumer thread: pop() ───┘
```

Both transports funnel through the single function `replay::apply_frame_result()` ([replay_engine.cpp:68-110](../src/replay/replay_engine.cpp)) — the one place sequence classification, stats, and book mutation happen. This is a genuinely good architectural decision, not just a documentation claim: it means the file-replay path (single-threaded) and the UDP path (two `jthread`s) cannot silently diverge in *what a message means*, only in *how a message arrives*. It is the single most reusable piece of design discipline in this codebase (see §7 target-boundary discussion).

**Important terminology correction for the rest of this report:** the brief's "new-order, cancel, replace" flows described below are **book-reconstruction operations on a passively-received feed message**, not an exchange accepting a live inbound order for matching. `AddOrder`/`CancelOrder`/`ModifyOrder` here are wire messages describing something that (per the simulated feed) already happened; `OrderBook::add_order()` etc. only ever *records* that fact into local state. There is no request/response, no acceptance/rejection sent back to a submitter, no matching against a resting order, because there is no submitter and no matching engine — only a generator (`feed_generator`) and a receiver (`market_data_replay`).

---

## 5. New-order, cancel, replace (modify), and event flows

### 5.1 "New order" (AddOrder) flow

1. **Entry point (file):** `EventFileReader::next()` ([event_file_reader.cpp:9-45](../src/replay/event_file_reader.cpp)) reads the 20-byte header, then `header.payload_size` more bytes, reusing `frame_buf_` (no per-call heap allocation once warm).
   **Entry point (UDP):** `net::run_udp_listen()`'s producer `jthread` ([udp_listener.cpp:37-75](../src/net/udp_listener.cpp)) calls `UdpReceiver::receive_batch()` → `net::unpack_frames()` ([packet.cpp:26-93](../src/net/packet.cpp)), which walks a datagram's packet header then each contained event frame.
2. **Parsing/decoding:** `protocol::decode_event()` ([decoder.cpp:58-96](../src/protocol/decoder.cpp)) reads the fixed header via `decode_header()`, cross-checks `payload_size` against `payload_size_for(type)` ([messages.hpp:85-94](../include/protocol/messages.hpp)), then decodes the 29-byte `AddOrder` payload via `io::ByteReader` — every field read is bounds-checked (`std::optional`, never UB on truncation).
3. **Validation (decode-level):** non-zero reserved byte → `InvalidReserved`; unknown type → `InvalidMessageType`; size mismatch → `InvalidMessageSize`; invalid `side` byte → `InvalidSide` (decoder.cpp:85-87). All returned as `DecodeError` via `std::variant`, never thrown.
4. **Queueing (UDP only):** the decoded `std::variant<Event, DecodeError>` is pushed onto a `DroppingQueue<FrameResult>` ([udp_listener.cpp:71](../src/net/udp_listener.cpp)) — drop-newest on full, counted. File replay has no queue; it's called directly in `run_replay()`'s loop ([replay_engine.cpp:125-133](../src/replay/replay_engine.cpp)).
5. **Sequencing:** `apply_frame_result()` calls `SequenceValidator::check()` ([sequence_validator.hpp:47](../include/common/sequence_validator.hpp)) on `AddOrder::sequence_number`. `InOrder` proceeds; anything else increments `sequence_failures` and either stops replay, triggers snapshot recovery (Missing only), or is a no-op continuation, per `ReplayOptions`.
6. **Book insertion ("matching," such as it is):** `apply_event()`'s `AddOrder` branch ([replay_engine.cpp:24-27](../src/replay/replay_engine.cpp)) calls `BookManager::book_for(instrument_id).add_order(order_id, price, quantity, side)`. `OrderBook::add_order()` ([order_book.cpp:35-47](../src/book/order_book.cpp)) validates `price > 0`, `qty != 0`, and `!order_index_.contains(id)` (duplicate rejection), then calls `insert_at()` ([order_book.cpp:7-17](../src/book/order_book.cpp)), which `try_emplace`s a `PriceLevel` into the appropriate `std::map` and appends to that level's `std::list<RestingOrder>` — **there is no crossing check against the opposite side at any point in this call chain.** A buy at 105 and a resting sell at 100 simply both end up resting; nothing detects or resolves the cross.
7. **"Fill" generation:** does not exist for `AddOrder`. `Trade` is a separate, independent wire message type that is purely informational (updates `InstrumentStats` only, does not touch book depth — [replay_engine.cpp:36-38](../src/replay/replay_engine.cpp), documented at [README.md:205-209](../README.md)) — it is not derived from or linked to any `AddOrder`.
8. **Result handling:** an `add_order()` failure (`BookError`) increments `stats.book_errors` ([replay_engine.cpp:27](../src/replay/replay_engine.cpp)) — a silent counter, not surfaced per-event, not sent anywhere, since there is no "sender" to acknowledge.

### 5.2 Cancel flow

`CancelOrder::order_id` → `BookManager::book_for(instrument_id).cancel_order(order_id)` → `OrderBook::cancel_order()` ([order_book.cpp:49-58](../src/book/order_book.cpp)): O(1) `order_index_.find()`, then `erase_at()` ([order_book.cpp:19-33](../src/book/order_book.cpp)) which finds the price level (`O(log P)` map lookup), calls `PriceLevel::remove(iterator)` (O(1) `std::list::erase`), and **erases the price-level map entry if the level is now empty** (order_book.cpp:23-25/29-30) — level cleanup is handled, verified by `OrderBook.CancelRemovesOrderAndEmptiesLevel` (tests/test_order_book.cpp:65). Unknown order id returns `BookError::UnknownOrderId` (order_book.cpp:52) without touching any state.

### 5.3 "Replace" (ModifyOrder) flow

`OrderBook::modify_order()` ([order_book.cpp:60-81](../src/book/order_book.cpp)) validates the new price/qty, looks up the order, **unconditionally does `erase_at()` then `insert_at()` at the new price/qty — i.e. cancel + re-add, always at the back of the (possibly new) list.** This is explicit, not accidental: the code comment at order_book.cpp:75-78 states "modify always loses time priority in this milestone, even for a quantity-only decrease where a real exchange would preserve it," and the README repeats this at README.md:199-203 as a named, deliberate simplification, not a bug.

Per the brief's specific replace-semantics questions:
- **Preserves priority when quantity decreases?** No — verified by code and design-doc, not merely assumed. Always cancel+re-add.
- **Loses priority when price changes?** Yes, and also when price is unchanged.
- **Implemented as cancel+new?** Yes, literally (`erase_at` + `insert_at`).
- **Is it "replace" in the order-entry-protocol sense (client requests to change their own resting order)?** No — `ModifyOrder` is a feed message describing that a modification already happened upstream (in the simulated exchange feed), not a request this system evaluates or could reject for referencing someone else's order.

### 5.4 Concurrency flow

- **Threads:** exactly two, both only inside `net::run_udp_listen()` ([udp_listener.cpp:18-112](../src/net/udp_listener.cpp)) — a producer (`receive_batch` → `unpack_frames` → `queue.push`) and a consumer (`queue.try_pop` → `apply_frame_result`). File-replay and `feed_generator`/`udp_sender` are single-threaded.
- **Producer/consumer boundary:** genuinely SPSC — one producer thread, one consumer thread, enforced by `SpscQueue`'s deleted copy/move constructors and the fact that only `run_udp_listen()` constructs and uses one (`DroppingQueue<FrameResult> queue` at udp_listener.cpp:29, captured by reference in exactly two lambdas).
- **Shared mutable state across the boundary:** the `DroppingQueue<FrameResult>` itself (the only state genuinely shared with a happens-before relationship — see below), plus `stop_source`/`result.outcome` (written by consumer, read by nothing else concurrently until `.join()`), and a handful of plain (non-atomic) locals (`packets_received`, `packet_errors`, `packet_tracker`) that are **producer-thread-only** and read only after `producer.join()` — not concurrent, so no atomics are needed there and none are used, correctly.
- **Atomics and memory orders:** `SpscQueue::head_`/`tail_` — producer does `head_.load(relaxed)` (own write, no need to synchronize with self) then `tail_.load(acquire)` (must see consumer's prior `destroy_at`), then `head_.store(release)` (publishes the constructed slot); consumer is the mirror image ([spsc_queue.hpp:67-96](../include/common/spsc_queue.hpp)). This is the textbook-correct SPSC acquire/release pairing (as the code's own doc comment states, citing the Rigtorp design) — no CAS loop, correctly, since neither atomic ever has concurrent writers. `high_water_mark_` is written only by the producer (relaxed store; no other writer, so no ordering is needed — correct as documented at spsc_queue.hpp:129-130) but *read* via `.load(acquire)` from `high_water_mark()` (spsc_queue.hpp:114), callable from any thread as a best-effort snapshot — correctly documented as such, not treated as a synchronization point. `DroppingQueue::dropped_count_` is a `fetch_add(relaxed)` from the producer only (dropping_queue.hpp:36,49) — correct, single-writer.
- **Locks:** none, anywhere in the hot path. `std::mutex` does not appear in `include/` or `src/`.
- **Busy-wait loops:** the producer sleeps 5ms when nothing is pending (udp_listener.cpp:49); the consumer does `std::this_thread::yield()` on an empty queue while not stopped (udp_listener.cpp:87) — a real (if coarse) busy-wait, acceptable for this project's scale but not something a genuinely latency-sensitive matching core would want on its own critical path (though note: this loop is on the *feed-handler* consumer thread, not inside anything resembling a matching engine, so its cost profile is about UDP/book-reconstruction throughput, not order-matching latency).
- **Shutdown coordination:** one shared `std::stop_source` ([udp_listener.cpp:30](../src/net/udp_listener.cpp)), `jthread` (auto-joining, no `std::terminate` risk from a missed `.join()`). Producer requests stop on idle timeout; consumer requests stop on a stop-worthy `apply_frame_result()` result; either way the consumer keeps draining (`while(true) { try_pop... if (!item) { if (stop_requested) break } }`, udp_listener.cpp:81-97) until the queue is empty *and* stop was requested — verified correct behavior, not just documented, by `test_udp_replay_e2e.cpp` and `test_backpressure_integration.cpp` passing under TSan.
- **Backpressure:** drop-newest, counted (`DroppingQueue::push`, dropping_queue.hpp:32-38) — a real, tested policy (`UdpReplayE2E.SlowConsumerForcesDrops`), not just described in prose.
- **False-sharing protection:** `head_`/`tail_` are each `alignas(64)` (spsc_queue.hpp:132-133), explicitly to prevent producer writes to `head_` from bouncing the consumer's cache line holding `tail_` — a real, correctly-reasoned optimization (not premature: it costs nothing and the queue is the one place with genuine cross-core contention).
- **CPU affinity:** none. No `sched_setaffinity`/`pthread_setaffinity_np`/thread-pinning code anywhere (confirmed via grep). Consistent with there being no benchmark suite yet to justify or measure the effect of pinning — appropriately not present rather than prematurely added.

### 5.5 Memory flow

- **Heap allocations in the "hot" path (the two-thread UDP pipeline):** meaningful and not hidden — every `receive_batch()` call heap-allocates a `std::vector<std::byte> buf(max_datagram_size_)` scratch buffer *per call* (udp_receiver.cpp:26, inside the loop body, reused only within one call not across calls) and then **copies** each datagram into a fresh `ReceivedDatagram::bytes` vector (udp_receiver.cpp:33-34, `.assign()`), i.e. one allocation+copy per received datagram, not a zero-copy design. `unpack_frames()` returns a `std::vector<std::variant<Event, DecodeError>>` (heap-allocated per packet, packet.hpp:78). Every `AddOrder` that lands in the book allocates a `std::list` node (`PriceLevel::add`, price_level.hpp:25-29) and, on a new price, a `std::map` node (`try_emplace`, order_book.cpp:9/13). This is a book-reconstruction/throughput profile, not a design that has been through a latency-allocation audit — which is expected and honest, since that audit is explicitly *this project's own* milestone 5, not yet done.
- **Object pools:** none exist anywhere in the codebase.
- **Fixed-capacity / preallocated structures:** exactly one — `SpscQueue<T>`'s ring buffer (`alloc_.allocate(capacity_)` once at construction, spsc_queue.hpp:51). Nothing else is fixed-capacity; `std::map`/`std::list`/`std::unordered_map`/`std::vector` are used throughout for the book and for per-call buffers.
- **Intrusive structures:** none. `PriceLevel::Iterator` (a `std::list<RestingOrder>::iterator`) is stored per-order in `OrderBook::order_index_` (order_book.hpp:65-69) to get O(1) list-erase-by-iterator, which is a reasonable non-intrusive substitute, but it is not an intrusive list in the low-latency-engineering sense (no orders-are-their-own-list-nodes design).
- **Pointer/reference stability assumptions:** `PriceLevel::Iterator` values stored in `order_index_` remain valid because `std::list` guarantees iterator stability across insertion/erasure of *other* elements — this is used correctly (verified: `OrderBook::cancel_order`/`modify_order` only ever erase the exact iterator looked up, never invalidate a sibling's). `std::map` iterators (`bids_`/`asks_`) are looked up fresh via `.find(price)` every time (order_book.cpp:21,27), never cached across calls, which sidesteps any risk of a stale map iterator after a level is erased. No dangling-pointer or use-after-free issue was found in this codebase, and ASan confirms this at runtime (§3.4).
- **Capacity limits / overflow behaviour:** `SpscQueue` capacity is fixed at construction with no resize (documented limitation, README.md:595-599); overflow (full queue) is drop-newest + counted, not UB, not blocking. No other component has an explicit capacity limit; `OrderBook`/`BookManager` grow unbounded with the feed (fine for a portfolio-scale synthetic feed, would need real capacity planning for a "credible platform" scale target).
- **Cache-line alignment:** only `SpscQueue::head_`/`tail_` (spsc_queue.hpp:132-133). `RestingOrder` (16 bytes: `OrderId` u64 + `Quantity` u64, price_level.hpp:9-12) and `PriceLevel` (Price i64 + `std::list` + Quantity u64 = 24 bytes of state plus `std::list`'s own overhead, price_level.hpp:43-45) are compact but not cache-line-sized or padded — reasonable for their current role (they're not the object being bounced between cores; only the SPSC queue indices are).

### 5.6 Event flow

What `apply_frame_result()` actually emits/returns, precisely:

- A `bool` — "should the caller stop" — is the *only* return value (replay_engine.hpp:85-88). There is no `EventSink`, no list of emitted events, no per-message result object handed back to a caller.
- Everything else is a **side effect**: mutation of `outcome.stats` (a plain counters struct, incremented in place) and mutation of `outcome.books` (the `BookManager`, mutated in place). One incoming frame can indeed produce multiple observable effects — e.g. a `Missing` classification with recovery configured increments `sequence_failures` *and* `recoveries` *and* replaces the entire `outcome.books` *and* resets the validator baseline *and* then still applies the triggering event on top (replay_engine.cpp:86-108) — but none of this is modeled as a list of discrete "events" a downstream consumer could iterate; it's several independent counters and one large mutable-state object, observed only via the final `ReplayStats`/console dump after the whole run ends (`market_data_replay/main.cpp:172-233`).
- There is no `Accept`/`Reject` concept at all (no submitter to accept or reject a request from). `Trade` messages are the closest thing to an "event" in the exchange sense, and they are wire input (already-happened facts from the simulated feed), not something this system generates.

**Implication for the target architecture (elaborated in §7):** none of this event-flow shape is reusable as the target's `EventSink`/execution-report/market-data-publisher fan-out. It would need to be built essentially from scratch, though the *discipline* of "one function is the single place state changes happen" (`apply_frame_result`) is a good pattern to carry forward.

---

## 6. Threading and ownership model

Fully covered in §5.4. Summary of ownership: `run_udp_listen()` is a free function that owns everything for the duration of one call — the `UdpReceiver`, the `DroppingQueue`, the `stop_source`, both `jthread`s, and the `UdpListenResult` being built up — and returns a single owned value. No global/static mutable state exists anywhere in the codebase (verified: no `static` non-const data members or file-scope mutable globals in any header/source file reviewed). This is a clean, easy-to-reason-about ownership model, and one of the more directly reusable structural patterns for a future exchange gateway (bind session lifetime to a stack-owned object, own your threads, join before returning).

---

## 7. Memory and allocation model

Covered in detail in §5.5. One-line summary for the matrix in §10: **throughput/correctness-oriented, not latency-oriented** — real heap traffic per message on the hot path (datagram copy, decode-result vector, list/map nodes on book mutation), zero object pools, one genuinely fixed-capacity structure (the SPSC queue). This is appropriate for what the project set out to be and is honestly documented as not yet optimized (its own milestone 5), but it means **none of the book/queue code should be assumed "already low-latency"** when reused in a matching-engine context — it would need the same allocation audit the README itself proposes doing for book reconstruction.

---

## 8. Correctness assessment

Evaluated by reading the actual implementation and the actual tests exercising it, not by test/function naming alone. "N/A" = the brief's item describes matching-engine behavior that has no corresponding concept in this codebase at all (there is no matching, so there is nothing to verify or refute).

| Item | Status | Evidence |
|---|---|---|
| Price-time priority (matching) | **N/A** | No matching engine exists; nothing crosses a bid against an ask. README explicitly disclaims this (README.md:215-219). |
| FIFO preservation (within a reconstructed level) | **Verified** | `PriceLevel::add()` always `push_back`s (price_level.hpp:25-29); `all_bids()`/`all_asks()` iterate `orders()` in list order. Test: `OrderBook.AllBidsAndAllAsksEnumerateEveryOrderNotJustTopLevels` (test_order_book.cpp:142). This is FIFO of *insertion into the reconstructed book*, not FIFO of *matching priority* (there is no matching). |
| Partial fills | **N/A** | No fill concept exists; `Trade` doesn't reduce any resting order's quantity (replay_engine.cpp:36-38; README.md:205-209). |
| Multi-level matching | **N/A** | No matching engine. |
| Self-crossing orders | **Missing (by design)** | A crossed book (best bid > best ask) can and does occur and is left as-is — explicitly documented, not a bug (README.md:215-219; order_book.cpp has no cross-check anywhere in `add_order`/`insert_at`). |
| Duplicate order IDs | **Verified** | `add_order()` returns `BookError::DuplicateOrderId` if `order_index_.contains(id)` (order_book.cpp:42-44). Test: `OrderBook.DuplicateOrderIdIsRejected` (test_order_book.cpp:110). |
| Unknown cancellation | **Verified** | `cancel_order()` returns `UnknownOrderId` if not found (order_book.cpp:51-53). Test: `OrderBook.CancelUnknownOrderIsRejected` (test_order_book.cpp:85). |
| Repeated cancellation | **Verified** | The first cancel erases the entry from `order_index_` (order_book.cpp:56); a second cancel of the same id necessarily falls into the "unknown" branch above. Not a dedicated named test, but directly implied by the erase-then-lookup structure, and safe under ASan (no double-free/use-after-free). |
| Zero or negative quantity/price | **Verified** | `price <= 0` → `InvalidPrice`, `qty == 0` → `InvalidQuantity` in both `add_order` and `modify_order` (order_book.cpp:36-41, 61-66). Tests: `OrderBook.ZeroOrNegativePriceIsRejected`, `ZeroQuantityIsRejected` (test_order_book.cpp:118,124). |
| Integer overflow | **Unable to determine** | No test constructs a `Price`/`Quantity` near `INT64_MAX`/`UINT64_MAX` and checks for wraparound in `aggregate_qty_ += qty` (price_level.hpp:27) or elsewhere. UBSan (which does catch signed overflow) passed cleanly, but only against inputs `feed_generator`'s RNG actually produces, which are not adversarial. Not verified either way for adversarial/boundary input. |
| Order-ID reuse | **Verified** | After `clear()`, a previously-used id is accepted again — test `OrderBook.ClearBookRemovesAllOrdersAndAllowsIdReuse` (test_order_book.cpp:129) — but reuse *without* an intervening clear/cancel correctly hits `DuplicateOrderId` (see above), which is the correct behavior for reconstruction of a real feed where ids are exchange-assigned and unique-while-live. |
| Marketable limit orders / IOC / FOK / GTC | **Missing** | No order-type or time-in-force concept exists anywhere in `messages.hpp`/`order_book.hpp` — `AddOrder` has no `order_type`/`tif` field on the wire at all (messages.hpp:38-46; docs/protocol.md:67). This is a wire-format/domain-model gap, not a bug — the concept was never in scope for a feed-reconstruction project. |
| Replace semantics | **Partially implemented (as designed)** | See §5.3 — implemented, but always cancel+re-add, always loses priority; documented as a deliberate simplification, not incomplete/incorrect relative to its own stated scope. |
| Empty-level removal | **Verified** | `erase_at()` erases the map entry when the level becomes empty (order_book.cpp:23-25, 29-30). Test: `OrderBook.CancelRemovesOrderAndEmptiesLevel` (test_order_book.cpp:65). |
| Iterator/pointer invalidation | **Verified (safe)** | See §5.5 — `std::list` iterator stability is relied on correctly and is safe by the standard's own guarantees; `std::map` iterators are never cached across calls. ASan/TSan runs found nothing. |
| Shutdown with commands still queued | **Verified** | Consumer drains the queue fully after a stop signal before exiting (udp_listener.cpp:81-97); verified under TSan, and by `UdpReplayE2E`/`BackpressureIntegration` tests. |
| Queue-full behaviour | **Verified** | Drop-newest + counted (`DroppingQueue::push`, dropping_queue.hpp:32-38); tested deterministically via `--consumer-delay-us`/small `--queue-capacity` (`UdpReplayE2E.SlowConsumerForcesDrops`, README.md:347-382 shows an actual captured run, not a hypothetical). |
| Determinism | **Partially verified** | File-replay path is fully deterministic given a fixed input file (single-threaded, no wall-clock dependence in book state) — `feed_generator --seed` produces byte-identical output, stated and consistent with its RNG-only source of nondeterminism (feed_generator/main.cpp:9-10 comment). The **UDP path is not, and cannot be, deterministic** in the strong sense: real sockets, real OS scheduling of two threads, and a drop-newest queue under real timing mean two runs of the same feed over `udp_sender`→`--listen` can legitimately produce different `queue high water`/`dropped` counts and even different final book states if backpressure triggers differently. This is inherent to the design (documented at README.md:271-280 re: the flaky-test lesson), not a defect, but it does mean "determinism" as a property holds for the file/replay core only, not for the live UDP pipeline. |
| Replayability | **Verified (file), Not applicable as "replay" (UDP)** | `run_replay()` deterministically reproduces book state from a fixed file. There is no capture-and-replay-exact-UDP-timing feature, nor is one claimed. |

---

## 9. Performance assessment

There is, at present, **nothing to assess empirically** — no benchmark harness, no measured throughput/latency numbers beyond the illustrative (explicitly-disclaimed-as-non-benchmark) console output shown in the README (README.md:423-425: "not invented figures... not a benchmark claim"). This section is therefore a structural review, not a numbers review.

- **Algorithmic complexity:** `add_order` — `O(log P)` (map insert) + O(1) list append + O(1) hash insert. `cancel_order`/`modify_order` — O(1) hash lookup + `O(log P)` map lookup + O(1) list erase (+ `O(log P)` map erase on last-order-in-level). This matches the README's own stated complexity analysis (README.md:189-197) and is verified correct by reading the code, not just quoted from docs. All complexities are appropriate for a reconstruction workload; none of this has been measured against a real distinct-price-level count.
- **Allocation behaviour:** per-message heap traffic on the UDP hot path (§5.5) — a genuine latency concern *if* this code were repurposed as-is inside a matching engine's critical section, not yet a proven problem for its actual current job (book reconstruction for a synthetic, moderate-rate feed).
- **Cache locality:** `std::map`/`std::list`-based book is pointer-chasing by construction — not cache-friendly, acknowledged in the README's own design-decision writeup (README.md:190-197) as a known, deliberate simplicity-over-locality tradeoff, with the flat-array alternative already named and rejected for now. This is an honest, pre-considered tradeoff, not an oversight.
- **Branching, data layout:** nothing branch-heavy or layout-pathological stood out in the reviewed code; `decode_event()`'s type switch (decoder.cpp:75-159) is a small, bounded switch over 5 cases, fine as-is.
- **Container choices:** `std::map` for price levels (log-time, ordered iteration for top-of-book/top-N — a reasonable choice for the current requirement to enumerate ordered levels); `std::unordered_map` for the order index (correct — no ordering needed there); `std::list` for FIFO-within-level (correct structural choice for O(1) erase-by-iterator, at the cost of a heap node per order).
- **Atomics/memory ordering:** covered in §5.4 — correctly minimal (no unnecessary `seq_cst`), no CAS where none is needed. This is genuinely good, non-premature use of relaxed/acquire/release.
- **False sharing:** correctly addressed in the one place it matters (`SpscQueue::head_`/`tail_` alignas(64)); not an issue elsewhere since nothing else is contended across threads.
- **Queue design/batching:** `receive_batch(64)` batches datagram draining per syscall-loop iteration (udp_receiver.cpp:22-39), a real (if modest) batching win; there's no batching of *book application* (each popped item is applied to the book individually, which is fine since book application is the actual unit of work here).
- **Logging in the hot path:** none — a genuine positive; no `std::cout`/logging calls exist inside `run_udp_listen()`'s thread bodies or inside `OrderBook`/`PriceLevel`.
- **Timestamping overhead:** `UdpReceiver` calls `std::chrono::steady_clock::now()` once per received datagram (udp_receiver.cpp:9-11, 35) — a `ReceivedDatagram::receive_timestamp_ns` is captured but **not currently used anywhere downstream** (confirmed via grep — no reader of `receive_timestamp_ns` outside the struct definition and its assignment). This is scaffolding for the project's own future milestone-5 latency work, not yet wired to anything, and not itself a hot-path cost concern (one `steady_clock::now()` per datagram is cheap).
- **Benchmark methodology / warm-up / debug-vs-release / p50/p99/p99.9 / trustworthiness:** **not applicable — none of this exists yet.** The only timing in the codebase is `std::chrono::steady_clock` wall-clock duration around an entire `run_replay()`/`run_udp_listen()` call (replay_engine.cpp:123,135-136; udp_listener.cpp:35,103-105), reported as a single aggregate `messages/sec` figure. This measures the whole pipeline (decode + validate + book-apply + queue overhead for UDP), not the book/matching logic in isolation — worth stating plainly since the target platform's benchmark work will need to build this from zero, including deciding what "the matching core alone" even means to measure once one exists.
- **Premature vs. useful optimizations, called out separately, per the brief's request:**
  - *Useful, not premature:* the SPSC queue's lock-free acquire/release design and cache-line padding — genuinely load-bearing for the one place with real cross-thread contention, low complexity cost, already tested under TSan.
  - *Correctly deferred, not "missing":* no CPU affinity, no `rdtsc`, no kernel-bypass/huge-pages/NUMA anything — appropriately absent given there's no benchmark suite yet to justify them, exactly per this assessment's own instruction not to suggest these prematurely.
  - *Worth reconsidering only once repurposed as a matching core:* `std::map`/`std::list` for price levels and per-message heap allocation on the hot path are fine for book *reconstruction* at today's scale, but would be the first things to profile-and-likely-replace if this code's structure were reused as the actual matching engine's book (see §10, §13).

---

## 10. KEEP / WRAP / REFACTOR / REPLACE / ADD matrix

Classified against the **target end-to-end platform** described in the brief. Where a component's fate differs depending on whether it lands on the **exchange (matching) side** vs. the **trading-firm (feed handler / local book) side**, both are given — this split is itself one of the report's key findings, since the current code is *the trading-firm side's raw material*, not the exchange's.

| Component | Classification | Existing files/classes | Evidence | Required action | Priority |
|---|---|---|---|---|---|
| Order representation | REFACTOR (exchange) / KEEP (trader local book) | `RestingOrder` (price_level.hpp:9-12) | 16 bytes: id+qty only, no owner/session/TIF/order-type/original-vs-remaining-qty | For exchange: add owner/session, TIF, order type, original vs. remaining qty (for partial fills). For trader's local book: usable as-is. | High (exchange), Low (trader) |
| Price representation | KEEP | `Price = int64_t` (types.hpp:19) | Scaled-integer ticks, explicitly chosen over floating point, consistent everywhere | None | — |
| Quantity representation | KEEP | `Quantity = uint64_t` (types.hpp:20) | Consistent everywhere | None | — |
| Order book | WRAP (trader local book) / REPLACE (exchange matching book) | `OrderBook` (order_book.hpp/.cpp) | Has no crossing/fill logic at all (§5.1 step 6); is *reconstruction*, not matching | Trader side: wrap with gap-recovery hookup (already half-done via `apply_frame_result`) and expose as the local book. Exchange side: needs a new book that peels/reduces resting orders on a crossing incoming order — current class cannot do this without new methods entirely. | High (exchange) |
| Price levels | KEEP (trader) / REFACTOR (exchange) | `PriceLevel` (price_level.hpp) | FIFO via `std::list`, O(1) add/remove-by-iterator | Exchange matching needs in-place quantity reduction on partial match without removing/reinserting the order (currently only supports remove-then-reinsert via `modify_order`, which is the wrong operation for a fill) | High (exchange) |
| FIFO queues | KEEP (trader) / REFACTOR (exchange, for allocation profile) | `std::list<RestingOrder>` | Heap node per order; correct semantics, not latency-optimized | Fine to keep for trader-side reconstruction; profile before reusing as-is in a matching core | Medium |
| Matching engine | **ADD** | *(none exist)* | Confirmed absent from working tree and all reachable git history (§1) | Design and implement from scratch: `ExchangeCommand` → crossing algorithm → `MatchingEvent`s | **Highest** |
| Order-ID index | KEEP | `unordered_map<OrderId, OrderLocation>` (order_book.hpp:79) | O(1) lookup, correctly used, no invalidation bugs found | None structurally; would carry over to an exchange book unchanged | — |
| New-order handling | WRAP (as a validation-only precedent) | `OrderBook::add_order()` (order_book.cpp:35-47) | Validates price/qty/duplicate id only; no matching | The validation shape (reject invalid input up front, then mutate) is reusable; the "then mutate" step needs to become "then attempt to match, then rest the remainder" for an exchange | High |
| Cancel handling | KEEP (pattern directly transferable) | `OrderBook::cancel_order()` (order_book.cpp:49-58) | O(1)+O(log P)+O(1), correct empty-level cleanup, tested | Directly reusable shape for an exchange's cancel path | Low |
| Replace handling | REFACTOR | `OrderBook::modify_order()` (order_book.cpp:60-81) | Always cancel+re-add, loses priority unconditionally (documented, deliberate) | A real exchange replace needs quantity-decrease-preserves-priority; needs new logic, not just relocation | Medium |
| SPSC ring buffer | **KEEP** | `SpscQueue<T>` (spsc_queue.hpp) | Lock-free, correct acquire/release, cache-line padded, TSan-clean including a genuine concurrent stress test | None — directly reusable for gateway→sequencer, sequencer→matching, or matching→market-data-publisher boundaries as-is | — |
| Matching thread | **ADD** | *(none — current consumer thread does book reconstruction, not matching)* | udp_listener.cpp:77-98 consumer calls `apply_frame_result`, never anything matching-shaped | Build a new single-threaded deterministic matching-core thread/loop | **Highest** |
| Input/replay parser | WRAP | `EventFileReader`/`EventFileWriter`, `run_replay()` | Tightly coupled to market-data message types, not exchange commands | Reusable *pattern* (streaming frame reader, reused buffer) for a future exchange command/event journal; needs a parallel type set | Medium |
| Protocol encoder/decoder | KEEP (pattern + market-data reuse) / REFACTOR (order-entry) | `protocol::encode_event`/`decode_event`, `io::ByteReader` | Explicit-shift big-endian, `std::variant<T,DecodeError>`, bounds-checked reader — genuinely solid pattern | Reuse the *codec discipline* wholesale for a new order-entry protocol; the concrete message set (`AddOrder`/`Cancel`/`Modify`/`Trade`/`ClearBook`) is market-data-shaped and can stay as-is for L3 market data | Medium |
| Benchmark harness | **ADD** | *(none)* | Confirmed absent (§3.6, §9) | Build from scratch — this project's own milestone 5 and the target platform's need overlap here, worth doing once for both | High |
| Unit tests | **KEEP** | 103 tests across 16 files | All pass, incl. under ASan/UBSan/TSan (§3) | Preserve and extend; a genuine asset | — |
| Integration tests | **KEEP** | `test_udp_replay_e2e.cpp`, `test_backpressure_integration.cpp`, `test_sequence_recovery.cpp` | Real loopback sockets, real threads, deterministic where it matters | Preserve; template for future exchange-side integration tests | — |
| Logging | **ADD** | *(none — only final console summary)* | No structured/leveled logging anywhere | Needs a real logging story for a multi-process platform; currently nothing to reuse | Medium |
| Metrics | WRAP | `ReplayStats`, `PacketSequenceStats`, `DroppingQueue` counters | Real, atomically-safe counters, but only dumped once at the end of a run, not exported live | Reusable counter shapes; needs a live-export layer (not present) | Medium |
| Configuration | WRAP | Hand-rolled `argv` parsing per app (e.g. market_data_replay/main.cpp:58-111) | Simple, adequate for one CLI tool each; not a shared config layer | Fine per-binary; a multi-process platform will want a shared config approach eventually | Low |
| Shutdown handling | WRAP | Shared `stop_source` + idle-timeout (udp_listener.cpp) | Clean pattern, but no signal-based (SIGINT) graceful shutdown anywhere (documented gap, README.md:480-484) | Reusable pattern; add real signal handling for a long-running gateway/service | Medium |
| Error model | **KEEP** | `std::variant<T, DecodeError>`, `std::optional<BookError>` throughout | No exceptions anywhere in the reviewed hot-path code; every fallible call returns a value | Directly reusable discipline for the exchange kernel's deterministic error handling | — |
| Build system | **KEEP** | `CMakeLists.txt` (102 lines) | Clean, warning-strict, 3 sanitizer targets, `FetchContent` GoogleTest, all verified working (§3) | Extend with new targets as new components are added; no rework needed | — |
| Common type definitions | KEEP / WRAP | `common/types.hpp` (27 lines) | `Price`/`Quantity`/`OrderId`/`InstrumentId`/`Side` already match what a matching engine needs almost verbatim | Add `OrderType`, `TimeInForce`, session/client-id types alongside the existing ones | Low |

---

## 11. Target architecture fit

The brief's target boundary is:

```cpp
class MatchingEngine {
public:
    void process(const ExchangeCommand& command, EventSink& sink);
};
```

**There is no matching engine to shape into this interface — it must be written.** What's worth assessing instead is: does the *surrounding* code demonstrate the separation discipline this interface requires, such that building the real thing inside this codebase's conventions would go well? The answer is a qualified yes:

- `apply_frame_result()` ([replay_engine.hpp:85-88](../include/replay/replay_engine.hpp)) already has almost exactly the requested shape: a pure function taking an already-decoded input and a mutable output-accumulator (`ReplayOutcome&`, playing the `EventSink&` role), called identically from two different transports. This is direct evidence the author already thinks in terms of "decode is not the same concern as application logic" and "one function, multiple entry points" — the core discipline the target interface wants. **This is a transferable habit, not a transferable component** — `apply_frame_result` itself operates on market-data `Event`s and mutates a `BookManager`, and cannot become the matching engine without new crossing logic; but the *shape* of "single deterministic apply function, EventSink-like output parameter, called from file/replay/network entry points alike" is exactly right and worth deliberately preserving in the new matching-engine code.
- The constraints the brief lists are already respected by analogous code today, which is a good sign for the same constraints holding once a real matching engine is built in this style: the decoder never touches the book (`decoder.cpp` has no dependency on `book/`); the book never touches the network (`order_book.cpp`/`price_level.cpp` have zero includes of `net/`); nothing here parses JSON or touches a database; nothing blocks on a socket inside the book-mutation call path (blocking only happens in `UdpReceiver`/`UdpSocket`, strictly upstream of `apply_frame_result`).
- **What's missing, concretely, to reach the target shape:** (1) an `ExchangeCommand` type — does not exist; `protocol::Event` is a market-data message, not an inbound trading command (no client id, no "new/cancel/replace" as a *request* rather than an already-happened *fact*); (2) a `MatchingEvent`/`EventSink` type — does not exist; `ReplayOutcome`/`ReplayStats` are aggregate counters plus final book state, not a stream of discrete accept/reject/fill/cancel events a downstream consumer (execution reports, market-data publisher, ledger) could each independently observe; (3) the crossing algorithm itself — does not exist anywhere; (4) a sequencer — the closest analog is `SequenceValidator`, which only *validates* a sequence number already assigned upstream (by the simulated feed / `feed_generator`), it does not *assign* sequence numbers to arriving commands, which is what a real sequencer must do.

**Bottom line:** the repository is close, in *style*, to being able to host a `MatchingEngine::process()` built the right way — clean layering, no accidental coupling to network/DB/UI, deterministic single-threaded core pattern already modeled by `apply_frame_result`/`run_replay`. It is **not close at all in substance** — there is no order-acceptance model, no crossing algorithm, no event-emission model, and no sequencer that assigns (rather than merely checks) sequence numbers. Treat this repo's actual matching-engine readiness as **0%**, and its architectural-style readiness (how well a new matching engine would fit alongside the existing code) as **good**.

---

## 12. End-to-end platform gap analysis

For each group: what exists, what's reusable, what's missing, dependencies, main risks, recommended order. Groups where nothing at all exists in this repo are marked **(clean slate)**.

**1. Matching-core preparation** — *Exists:* nothing matching-specific; only the layering discipline discussed in §11 and the `common/types.hpp` primitives. *Reusable:* `Price`/`Quantity`/`OrderId`/`InstrumentId`/`Side`, the `std::optional`/`std::variant` error-handling style, the build system. *Missing:* everything else — order representation with TIF/type, the crossing algorithm, fill generation. *Dependencies:* none blocking — can start immediately. *Risks:* underestimating this as "just reuse `OrderBook`" — it cannot be reused for matching without new mutation semantics (in-place partial-fill quantity reduction, not remove/reinsert). *Order:* first.

**2. Exchange command model** — **(clean slate).** *Missing:* `ExchangeCommand` (NewOrder/Cancel/Replace requests, each carrying client/session identity). *Dependencies:* needed before the sequencer or matching core can be meaningfully typed. *Risks:* conflating this with `protocol::messages.hpp`'s existing types, which model already-happened market-data facts, not inbound requests — a genuinely different shape (needs request/response correlation, reject reasons). *Order:* early, alongside #1.

**3. Event model** — **(clean slate).** *Missing:* a `MatchingEvent`/`EventSink` type distinguishing Accept/Reject/Trade/PartialFill/Cancel/BookChange, each independently deliverable to multiple downstream consumers (execution reports, market data, ledger, journal). *Reusable pattern:* `ReplayStats`'s counters and `apply_frame_result`'s "one function, one place things happen" discipline, as a style reference only. *Risks:* under-designing this as "just more `ReplayStats` fields" — the target needs discrete, independently-routable events, not aggregate counters. *Order:* alongside #2, before #1's matching core is finalized (the two co-design).

**4. Sequencing** — *Exists:* `SequenceValidator` — but it **classifies an already-assigned sequence number**, it does not assign one. *Reusable:* the classification logic (InOrder/Duplicate/OutOfOrder/Missing) is directly useful for detecting gaps in anything sequenced, including a new command-sequencer's output stream if downstream consumers need gap detection too. *Missing:* the actual sequencer that assigns a monotonic sequence to incoming validated commands before they reach matching. *Dependencies:* needs #2 (command model) first. *Risks:* low — this is a small, well-bounded new component. *Order:* after #1/#2, before the gateway is wired up end-to-end.

**5. Order-entry protocol** — *Exists:* a complete, working codec *pattern* (`protocol::encode_event`/`decode_event`, `io::ByteReader`, big-endian byte-shift approach, `std::variant<T, DecodeError>`) but for the wrong message set (market data, not order entry — no NewOrder/CancelRequest/ReplaceRequest with client-assigned IDs). *Reusable:* the entire codec discipline and its test-writing style (`tests/test_protocol_roundtrip.cpp`, `test_decoder_errors.cpp` are excellent templates). *Missing:* the actual order-entry message set and its round-trip tests. *Risks:* low — this is mechanical, well-precedented work given the existing pattern. *Order:* can proceed in parallel with #1-#4.

**6. Exchange gateway and sessions** — **(clean slate)** for sessions/heartbeats/client identity. *Reusable:* `UdpSocket`/`UdpReceiver` demonstrate clean POSIX-socket RAII and non-blocking batched receive, directly relevant groundwork for a TCP order-entry gateway (different transport, same "own your fd, RAII, non-blocking, batch-drain" shape); `run_udp_listen()`'s two-thread producer/consumer + shared `stop_source` pattern is a strong template for a gateway's per-session or accept-loop threading model. *Missing:* TCP listener, session/heartbeat/sequence-number-per-client tracking, request validation before entering the sequencer. *Risks:* medium — session lifecycle and heartbeat timeout logic is new complexity, though the `jthread`+`stop_source` shutdown pattern already proven here reduces some of that risk. *Order:* after #2, #4, #5 exist to give it something to decode into and hand off to.

**7. Exchange-side risk** — **(clean slate).** No risk-check concept anywhere in this repo. *Risks:* main risk is scope creep — brief only needs "small but credible" pre-trade checks (e.g. max order size, max position). *Order:* after #2 (needs `ExchangeCommand` to check against) and #8 (needs balances to check against).

**8. Balances, reservations, ledger** — **(clean slate).** *Risks:* the biggest design risk in this whole group is deciding reservation semantics (when does a balance get reserved vs. debited) — genuinely new design work, no precedent in this repo to build on. *Order:* needed before risk (#7) can check real balances, and before execution reports (#9) can be meaningfully tied to settlement.

**9. Execution reports** — **(clean slate)**, but the *pattern* to reuse is again `apply_frame_result`'s "one function, multiple observers" style, plus the codec discipline from #5 to encode reports on the wire. *Order:* after #1/#3 (needs matching events to report on).

**10. Market-data publisher** — **Partially reusable in an unusual direction:** this repository is *already* a consumer of a market-data feed (the trading-firm side of this exact box), not a publisher of one — but `net::pack_frames()`/`protocol::encode_event()` (the `udp_sender` app's job today, sending a *replayed* feed) is structurally identical to what an exchange-side publisher needs to do with *live* matching events instead of *replayed* history. *Missing:* deriving L3/L2 messages from live matching events (today `udp_sender` only re-streams a pre-existing file). *Risks:* low — `udp_sender`/`packet.cpp` already prove the batching-into-UDP-datagrams mechanics work and are tested. *Order:* after #3 (event model) exists to derive market data from.

**11. Snapshots and gap recovery** — **Substantially reusable, arguably this project's strongest asset for the target platform.** `replay::apply_frame_result()`'s Missing-triggers-recovery logic and `replay/snapshot.hpp`/`snapshot.cpp`'s wire-reusing snapshot format (§ new milestone-4 work, verified working end-to-end in §3.2-3.4) is a complete, tested reference implementation of exactly the "snapshot and sequence-gap recovery" box on the trading-firm side of the target diagram. *Missing:* nothing structural for the trader-side use of this box; the exchange side would need its own snapshot-serving capability (a live "give me a current snapshot" service), which does not exist (the current design reloads one static file per recovery, a documented, deliberate simplification — README.md:613-617). *Risks:* low for trader-side reuse; medium for exchange-side (needs a live snapshot-serving component, new work). *Order:* trader-side: reusable now. Exchange-side: after #1/#3.

**12. Journal and deterministic replay** — *Exists:* `EventFileReader`/`EventFileWriter`/`run_replay()` are a complete, tested, append-only-in-spirit (sequential write, sequential read) journal-and-replay mechanism for *market-data* events. *Reusable:* the streaming-reader pattern (reused buffer, no per-message allocation once warm — event_file_reader.cpp:14,31) directly. *Missing:* an equivalent journal for **exchange commands** (inbound requests) and **matching events** (outbound), which is what the target's "deterministic replay of the matching core" needs — today's journal replays market-data facts, not exchange decisions. *Risks:* low — same pattern, new message types. *Order:* alongside #1-#3.

**13. Trading-side feed handler** — **This is essentially what `mdh` already is.** `market_data_replay --listen`, `net::run_udp_listen()`, the decode/validate/apply pipeline — this entire repository's core competence. *Missing:* nothing structural, assuming the exchange eventually emits a wire-compatible (or adaptable) feed. *Risks:* the only real risk is a protocol mismatch if the new exchange's market-data output doesn't resemble this wire format — worth deciding explicitly (see §17 open questions) whether to reuse this exact protocol or design a new one. *Order:* can be nearly last, since it's closest to done; revisit once the exchange's actual market-data output format is fixed.

**14. Local reconstructed order book** — **`book::OrderBook`/`BookManager`, as-is, largely fit for purpose here** (as distinct from its unfitness for the *exchange's* matching book, see §10). This is the "trader's reconstructed local book" the brief explicitly asks to be architecturally distinguished from the authoritative exchange book (per the brief's reporting standards) — and this codebase's `OrderBook` is a reasonable implementation of exactly that reconstructed-book role, not the authoritative-matching-book role. *Missing:* nothing major; would benefit from exposing more read-only views if a strategy runtime needs richer queries than `top_bids`/`top_asks`/`best_bid`/`best_ask`. *Risks:* low. *Order:* near-last, reuse as-is.

**15. OMS** — **(clean slate).** No order-management, no client-order-id-to-exchange-order-id mapping, no order-state-machine anywhere in this repo. *Order:* after #5/#6 (needs an order-entry client to send through) and #14 (needs a local book concept, already available, to reconcile against).

**16. Trader-side risk** — **(clean slate)**, conceptually simpler than exchange-side risk (#7) since it's single-account. *Order:* after #15.

**17. Positions and P&L** — **(clean slate).** *Risks:* realized/unrealized P&L computation is genuinely new math/design work with no precedent here. *Order:* after #15/#9 (needs fills to compute against).

**18. Strategy runtime** — **(clean slate).** *Order:* after #14/#15/#16/#17 exist to give a strategy something to observe and act through.

**19. Example strategies** — **(clean slate).** *Order:* last among trading-firm-side work, needs #18.

**20. UI gateway** — **(clean slate).** No REST/WebSocket server anywhere in this repo (only raw UDP sockets for market data). *Order:* can start in parallel with backend work once there's *something* to expose (even just replay stats).

**21. React dashboard** — **(clean slate)** from this repo's perspective — note this assessment's own harness is a React/TS codebase in a *different* repository (`spotnana-frontend`), unrelated to `mdh`; no dashboard code exists in `mdh` itself. *Order:* last, needs #20.

**22. Metrics and observability** — *Exists:* real, tested counters (`ReplayStats`, `PacketSequenceStats`, `DroppingQueue::dropped_count`/`high_water_mark`) — but only dumped once, at the end of a run, to stdout (`market_data_replay/main.cpp:172-233`). *Missing:* any live/streaming export (Prometheus, a metrics socket, anything polled continuously). *Risks:* low, mechanical work. *Order:* can proceed incrementally alongside everything else.

**23. Failure injection** — *Exists:* nothing yet — this is explicitly this project's **own** milestone 6 ("fault injection: corrupt messages, truncated packets, duplication, packet loss, delayed consumer, recovery validation," README.md:630-631), not yet started. `--consumer-delay-us` (a deliberate, already-built slow-consumer injector, udp_listener.hpp:22-27) is the one piece of failure-injection-adjacent tooling that already exists and works. *Order:* useful throughout, not just at the end — the target platform's own risk/recovery testing will want this earlier than "last."

**24. Performance engineering** — Covered in §9; nothing to reuse but the (currently unused) `receive_timestamp_ns` scaffolding and the general absence of premature optimization to first undo. *Order:* only after there's a real matching core to measure — measuring today's book-reconstruction throughput in isolation would not answer the target platform's actual performance questions.

**25. Documentation and demonstrations** — *Exists:* genuinely strong precedent — `README.md` and `docs/protocol.md` are thorough, honest about limitations, and kept in sync with the code (verified throughout this assessment: every claim checked against source was accurate). *Recommendation:* carry this documentation discipline forward into the new exchange-side work; it is a real, demonstrated strength of this author's process, not just of this one repo.

---

## 13. Recommended migration milestones

Given §11's finding (matching engine must be built from zero) and §12's finding (the trading-firm-side feed handler is comparatively far along), the brief's suggested first-five-milestones template is adjusted below to reflect that the "stabilize the existing matcher" step has no matcher to stabilize — instead, the earliest milestones focus on (a) not losing the trading-firm-side asset that does exist, and (b) starting the matching core with the same discipline this codebase already demonstrates.

**Milestone A — Commit and lock down what already exists**
- *Goal:* stop the current uncommitted milestone-4 work (§3.7) from being at risk of loss; establish a clean, tagged baseline to branch the new exchange-side work from.
- *Files/components:* the 4 new + 13 modified files currently uncommitted (snapshot/recovery).
- *Expected new interfaces:* none.
- *Existing code reused:* all of it, unchanged.
- *Tests required:* none new — the existing 103 already pass; just commit them.
- *Completion criteria:* `git status` clean; `main` (or a new branch) has milestone 4 committed; a tag or branch point exists to migrate from.
- *Risks:* none technical; purely a "don't lose work" housekeeping step. Can be done independently of everything else, immediately.

**Milestone B — Design and implement the `ExchangeCommand` / `MatchingEvent` type pair**
- *Goal:* create the two new type sets §12 groups 2 and 3 need, following this codebase's existing `std::variant`/`std::optional` error-handling style (§10 "Error model: KEEP").
- *Files/components:* new `include/exchange/command.hpp`, `include/exchange/event.hpp` (or similar new top-level area, see §14).
- *Expected new interfaces:* `ExchangeCommand` variant (NewOrder/Cancel/Replace, each with client/session identity), `MatchingEvent` variant (Accepted/Rejected/Trade/PartialFill/Cancelled/BookChanged).
- *Existing code reused:* `common/types.hpp` primitives extended (add `OrderType`, `TimeInForce`); the `std::variant`-based modeling style from `protocol/messages.hpp`.
- *Tests required:* round-trip/construction tests analogous to `test_protocol_roundtrip.cpp`.
- *Completion criteria:* types compile, have unit tests, are reviewed against §11's target-boundary constraints (no network/DB/UI dependency baked into the types themselves).
- *Risks:* under-scoping the event model as "just more counters" (§12 group 3's stated risk). Depends on Milestone A only (needs a stable base to branch from); independent of C.

**Milestone C — Build the matching core behind `MatchingEngine::process()`**
- *Goal:* the actual crossing algorithm — the single biggest gap identified in this whole assessment.
- *Files/components:* new `include/exchange/matching_engine.hpp`/`.cpp`; a new book type (not a reuse of `book::OrderBook` as-is — see §10) supporting in-place partial-fill quantity reduction.
- *Expected new interfaces:* `MatchingEngine::process(const ExchangeCommand&, EventSink&)` per the brief's target shape.
- *Existing code reused:* `common/types.hpp`, the `PriceLevel`/`std::map`-per-side *pattern* (not the class itself, since it needs new mutation semantics), the "single deterministic function, called from multiple entry points" discipline modeled by `apply_frame_result`.
- *Tests required:* new — price-time priority, partial fills, multi-level matching, IOC/FOK/GTC, self-cross handling (a policy decision — reject vs. allow vs. cancel-both, currently undecided, see §17), all analogous in rigor to `test_order_book.cpp`'s 18 tests.
- *Completion criteria:* a deterministic, single-threaded matching core with test coverage at least as thorough as the existing `OrderBook` suite, passing under ASan/UBSan/TSan the same way the rest of this repo already does.
- *Risks:* **highest in the whole plan** — this is genuinely new design and implementation work, not extraction; self-crossing policy, TIF semantics, and partial-fill bookkeeping all need real decisions (§17). Depends on Milestone B (needs the command/event types first); independent of D.

**Milestone D — Preserve existing tests/benchmarks while adding a deterministic replay for the new matching core**
- *Goal:* extend the existing `EventFileReader`/`EventFileWriter`/`run_replay()` pattern (§12 group 12) to journal `ExchangeCommand`s and replay them deterministically through the Milestone C matching core.
- *Files/components:* new journal writer/reader for exchange commands, reusing `event_file_reader.cpp`/`event_file_writer.cpp`'s streaming-buffer pattern.
- *Existing code reused:* the entire file-I/O pattern, essentially unchanged in shape.
- *Tests required:* a determinism test analogous to the existing replay determinism check (README.md:255 mentions one already exists for market-data replay — mirror it for command replay).
- *Completion criteria:* the same input command journal always produces the same matching events and final book state.
- *Risks:* low — mechanical, well-precedented by existing code. Depends on Milestone C.

**Milestone E — Only then: networking, risk, ledger, market data, OMS, strategies, UI**
- Per the brief's own guidance and §12's per-group ordering above. Each of these should be its own milestone with its own acceptance criteria once reached; not detailed further here since none of them can be meaningfully scoped until Milestones B/C establish the command/event vocabulary they all consume.

**Independence:** Milestone A is independent of everything and should happen first regardless. B and the codec work in §12 group 5 (order-entry protocol) can proceed in parallel. C strictly depends on B. D strictly depends on C. Everything in "E" depends on C at minimum, and mostly on B+C+D together.

---

## 14. Recommended repository evolution

The brief's suggested structure fits reasonably well, with one adjustment: `mdh`'s existing code maps almost entirely under `/trader/feed_handler` and `/trader/local_book`, plus reusable pieces under `/common` and `/protocol/market_data` — **not** under `/exchange/*`, which is close to entirely new.

```
/common
    /types         <- include/common/types.hpp (extend with OrderType/TimeInForce)
    /concurrency   <- include/common/spsc_queue.hpp, dropping_queue.hpp (unchanged)
    /io            <- include/common/byte_io.hpp (unchanged; shared codec primitive)
    /sequencing    <- include/common/sequence_validator.hpp (reused for both event- and packet-level
                      gap detection today; reusable for exchange-side sequencer's own gap detection too)

/protocol
    /market_data   <- include/protocol/{messages,decoder,encoder,errors}.hpp, net/packet.hpp
                      (current wire format — reused as-is for trader-side feed handling;
                       decide per §17 whether the new exchange emits this same format)
    /order_entry   <- NEW: mirrors market_data/'s codec pattern for ExchangeCommand wire messages

/exchange
    /core          <- NEW: ExchangeCommand, MatchingEvent (Milestone B)
    /matching      <- NEW: the matching engine itself (Milestone C) — NOT include/book/* reused as-is
    /gateway       <- NEW: TCP order-entry sessions (can borrow udp_listener.cpp's
                      jthread+stop_source+queue shape, different transport)
    /sequencer     <- NEW: assigns sequence numbers to validated commands (SequenceValidator's
                      classification logic is reusable for downstream gap-checking, not for assignment)
    /risk          <- NEW
    /ledger        <- NEW
    /market_data   <- NEW: derives live market data from MatchingEvents (net::pack_frames() pattern
                      reusable for the actual wire-batching mechanics)
    /persistence   <- NEW: exchange command/event journal (Milestone D, pattern borrowed from
                      replay/event_file_{reader,writer}.cpp)

/trader
    /feed_handler  <- net/{udp_socket,udp_receiver,udp_listener,packet,packet_sequence_tracker}.hpp,
                      replay/{replay_engine,snapshot}.hpp — this repository's core competence, reused
                      close to as-is
    /local_book    <- book/{order_book,price_level,book_manager,book_errors}.hpp — reused as-is
                      as the RECONSTRUCTED book (explicitly not the exchange's authoritative book)
    /oms           <- NEW
    /risk          <- NEW
    /positions     <- NEW
    /strategies    <- NEW

/simulation        <- apps/feed_generator/ pattern reused/extended for exchange-command generation
/replay            <- replay/{event_file_reader,event_file_writer}.hpp pattern, extended for
                      exchange command/event journals (Milestone D)
/benchmarks        <- NEW (this project's own deferred milestone 5, and the target platform's
                      performance-engineering group — worth building once for both)
/tests             <- tests/*.cpp — preserved wholesale; extend with new test files per new component,
                      following the existing per-component-one-file convention
/ui_gateway        <- NEW
/ui                <- NEW (a separate repo/toolchain, not part of this C++ codebase)
/docs              <- docs/protocol.md pattern extended for the order-entry protocol; README.md's
                      documentation discipline (accurate, limitation-honest) is worth explicitly
                      carrying forward as a norm, not just a file
```

**Old-path → new-path mapping for the files that move with the least friction (i.e., near-verbatim reuse, not rewrite):**

| Old path | New path | Change needed |
|---|---|---|
| `include/common/types.hpp` | `common/types/` | Add `OrderType`, `TimeInForce`, session/client-id types |
| `include/common/spsc_queue.hpp` | `common/concurrency/` | None |
| `include/common/dropping_queue.hpp` | `common/concurrency/` | None |
| `include/common/byte_io.hpp` | `common/io/` | None |
| `include/common/sequence_validator.hpp` | `common/sequencing/` | None (reused for classification; a new component does assignment) |
| `include/protocol/*`, `src/protocol/*` | `protocol/market_data/` | None, if the exchange reuses this wire format for its own market-data output (open question, §17) |
| `include/net/*`, `src/net/*` | `trader/feed_handler/` | None structurally; UDP-specific parts stay UDP-specific unless the exchange's transport differs |
| `include/replay/*`, `src/replay/*` | `trader/feed_handler/` (market-data replay) + `replay/` (pattern reused for command/event journal) | Split: existing files stay for market-data replay; new parallel files for exchange command/event replay |
| `include/book/*`, `src/book/*` | `trader/local_book/` | None — reused as the reconstructed book, explicitly not the exchange's matching book |
| `apps/feed_generator/` | `simulation/` | None initially; extend later for exchange-command generation |
| `apps/market_data_replay/`, `apps/udp_sender/` | `trader/feed_handler/` (as example drivers) | None |
| `tests/*` | `tests/` (unchanged location) | None — preserved wholesale |

---

## 15. Top technical risks

1. **The brief's premise mismatch is the single largest risk to the whole migration plan.** If the expectation going in was "extract and harden an existing matching engine," every subsequent estimate (time, complexity, milestone count) built on that premise needs to be redone treating the matching core as 100% new work, not a refactor. This is the most consequential finding in this report and should be resolved with the user before any implementation planning proceeds (see §17, Q1).
2. **`OrderBook`/`PriceLevel` cannot be reused unchanged as the exchange's matching book** — they support remove-then-reinsert (`modify_order`) but not in-place partial-fill quantity reduction, which real matching needs. Treating them as "close enough" risks baking wrong mutation semantics into the new matching core (§10).
3. **Determinism holds for file-replay but not for the live UDP pipeline** (§8) — if the target platform's "deterministic replay" requirement is meant to extend to live-traffic runs (not just journaled-command replay), the current SPSC/drop-newest design's inherent timing-dependent behavior needs to be explicitly scoped out of "must be deterministic," or redesigned (e.g., a blocking/reordering-buffer queue instead of drop-newest) — a real tension between the existing backpressure design and a stronger determinism requirement, not yet reconciled anywhere in this repo or the brief.
4. **No CI exists.** `FetchContent`-based GoogleTest requires live network access to configure — this will silently fail in an offline or network-restricted CI environment and needs a vendoring or network-allowlist decision before CI is set up.
5. **Uncommitted milestone-4 work (§3.7)** is a small but real risk of accidental loss until committed — flagged, not fixed, per this assessment's read-only scope.
6. **Self-crossing policy for the new matching engine is an open design question** (§12 group 1, §17) with no precedent anywhere in this codebase to lean on, since the existing book explicitly tolerates crossed books rather than resolving them.

---

## 16. Immediate next three engineering tasks

1. **Commit the uncommitted milestone-4 snapshot/recovery work** (§3.7) to establish a clean, safe baseline before any new (exchange-side) work begins branching off this repository.
2. **Resolve the scope mismatch with the user** (§17, Q1) before writing a single line of matching-engine code — the answer changes whether "Milestone C" in §13 is scoped as a small addition or, correctly per this assessment's findings, as the single largest, highest-risk body of new work in the entire migration.
3. **Design `ExchangeCommand`/`MatchingEvent` (Milestone B, §13)** — the one piece of new type design that everything else (sequencer, gateway, risk, ledger, market-data publisher, OMS) depends on being fixed first, and the natural first concrete deliverable once #1 and #2 are settled.

---

## 17. Questions that genuinely require a human decision

1. **Scope correction:** the brief describes a matching engine "completed through approximately milestone 5." This repository contains no matching engine at any milestone, and has its own, different 6-milestone roadmap for a market-data feed handler (currently at milestone 4). Is there a *different* repository that holds the matching engine described in the brief, or was the brief written from an inaccurate premise about this repository's contents? This must be resolved before any milestone planning is treated as final, since it changes Milestone C in §13 from "maybe partially reusable" to "100% new work."
2. **Given finding #1, does the user want:** (a) this `mdh` repository repurposed as the target platform's **trading-firm-side feed handler and local book** (where it fits naturally, per §12 groups 13-14), with a **brand-new** matching engine written from scratch for the exchange side; or (b) something else entirely (e.g., start a new repository for the exchange side, or locate/re-derive the missing matching engine first)?
3. **Wire-protocol reuse:** should the new exchange's live market-data output reuse this project's exact wire format (`protocol::messages.hpp`'s `AddOrder`/`CancelOrder`/`ModifyOrder`/`Trade`/`ClearBook`, big-endian byte-shift codec), maximizing reuse of `mdh`'s feed-handler code as-is, or does the target platform need a materially different/more realistic L2/L3 format that this repo's codec would need to be re-derived (not just relocated) for?
4. **Latency ambition:** is genuine low-latency engineering (object pools, intrusive structures, cache-optimized layouts, CPU affinity) actually a requirement for "small but credible," or is `std::map`/`std::list`-based book logic (this repo's existing style) acceptable for the matching core too, given the brief's own instruction not to reach for kernel-bypass/NUMA/affinity prematurely? This materially changes Milestone C's design and risk profile.
5. **Determinism vs. backpressure tension** (§15, risk 3): should the target platform's "deterministic command sequencer" and "deterministic replay" extend to live-traffic runs, or only to offline replay of a journaled command log? This decides whether the existing drop-newest SPSC design is reusable as-is for the exchange side or needs a different (e.g., blocking, back-pressuring-the-sender) policy.
6. **Self-crossing policy:** when the new matching engine receives an order that would cross the resting book in a way existing test data/generators don't currently anticipate, should it match, reject, or apply some other exchange-specific rule? No precedent exists in this codebase (which explicitly tolerates crossed books rather than resolving them) to default to.

---

*Verification note: every file path, class name, function name, and line number cited above was confirmed against the actual repository state on 2026-08-04 via direct reads of the source files and live execution of the build/test commands listed in §3 — not inferred from README/docs claims alone. Where README/docs claims were checked against code and matched, this is stated explicitly; no claim in this report rests solely on documentation or naming.*
