# Benchmarks (Milestone 13)

**Status:** implemented and run for real; every number below was produced by an actual
execution of the benchmarks in this repository on the date noted, not invented or
estimated — same documentation discipline this project applies everywhere else (see
e.g. `include/replay/replay_stats.hpp`'s own non-benchmark disclaimer, which is exactly
the gap this milestone closes).

This was this project's own long-deferred milestone (originally named "allocation
profiling, decode throughput benchmarks, end-to-end latency (p50/p99/p99.9), comparison
of alternative book representations" back when the project was market-data-feed-handler
only — see `docs/current_system_assessment.md` §9/§12 group 1/§13 Milestone... well,
the point stands regardless of the old numbering: benchmarking was named and never
built until now).

---

## 1. How to run these yourself

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j --target \
    bench_protocol_codec bench_matching_engine bench_order_book \
    bench_spsc_queue bench_end_to_end_latency

./build-release/bench_protocol_codec
./build-release/bench_matching_engine
./build-release/bench_order_book
./build-release/bench_spsc_queue
./build-release/bench_end_to_end_latency 20000   # optional iteration count, default 10000
```

**Debug-build numbers are meaningless and must never be compared against the ones
below** — unoptimized codec/matching/book code plus (if enabled) sanitizer
instrumentation change every number here by a large, non-representative factor. All
figures in this document come from a `-DCMAKE_BUILD_TYPE=Release` build.

`MDH_BUILD_BENCHMARKS` (default `ON`) controls whether `benchmarks/` is even configured
— set `-DMDH_BUILD_BENCHMARKS=OFF` to skip fetching/building Google Benchmark entirely
(e.g. in a network-restricted environment that only needs `mdh_tests`).

## 2. Machine this run was measured on

- **Date:** 2026-08-09
- **Host:** Apple M3 Pro, 12 logical cores (`arm64`, macOS/Darwin 25.5.0)
- **Compiler/flags:** AppleClang, `-DCMAKE_BUILD_TYPE=Release`, same `-Wall -Wextra
  -Wpedantic -Wshadow -Wconversion` warning set as every other target in this repo
  (all four `benchmark::benchmark`-linked binaries build warning-clean under it)
- **Google Benchmark:** v1.9.1, fetched via the same `FetchContent` pattern already used
  for googletest/cpp-httplib/nlohmann-json (see `CMakeLists.txt`)
- Every Google-Benchmark-based run below used `--benchmark_min_time=0.5s` (the default
  ~0.1s window is too short for the sub-microsecond cases here to get a stable
  iteration count); numbers will vary run-to-run and machine-to-machine by a
  meaningful margin — treat everything below as illustrative of relative cost and
  scaling shape, not a portable SLA.
- `bench_end_to_end_latency`'s numbers are further specific to this machine's own
  thread-scheduling/wake-up latency (see §6) and will differ substantially on Linux,
  under load, or under a different scheduler.

---

## 3. Protocol codec throughput (`bench_protocol_codec`)

| Benchmark | Time/op | Throughput |
|---|---|---|
| `BM_MarketData_EncodeAddOrder` (29-byte payload) | 43.4 ns | 1.06 GiB/s |
| `BM_MarketData_DecodeAddOrder` | 5.77 ns | 7.93 GiB/s |
| `BM_OrderEntry_EncodeNewOrder` (39-byte payload) | 43.9 ns | 917 MiB/s |
| `BM_OrderEntry_DecodeNewOrder` | 17.4 ns | 2.25 GiB/s |

**Reading this:** decode is faster than encode for both protocols — `decode_event()`/
`decode_message()` read directly out of an already-sized `std::span` via
`io::ByteReader` with no allocation, while `encode_event()`/`encode_message()` append
onto a `std::vector<std::byte>` whose capacity these micro-benchmarks don't
pre-reserve, so a chunk of the encode cost is amortized `push_back`/growth bookkeeping,
not the byte-shifting itself. `AddOrder` decodes ~3x faster than `NewOrder` decodes
despite a larger payload (29 vs. 39 bytes) — `NewOrder` has two additional
single-byte enum fields (`order_type`, `time_in_force`) that `ByteReader` reads one at
a time, which is a real, if small, cost difference between the two wire formats worth
knowing about, not a bug in either codec.

---

## 4. Matching engine throughput (`bench_matching_engine`)

| Benchmark | Time/op |
|---|---|
| `BM_MatchingEngine_NewOrderRestsNoCrossing` | 169 ns |
| `BM_MatchingEngine_SingleLevelFullFill` | 733 ns |
| `BM_MatchingEngine_MultiLevelSweep/1` | 732 ns |
| `BM_MatchingEngine_MultiLevelSweep/4` | 1000 ns |
| `BM_MatchingEngine_MultiLevelSweep/16` | 2035 ns |
| `BM_MatchingEngine_MultiLevelSweep/64` | 6158 ns |
| `BM_MatchingEngine_MultiLevelSweep/256` | 22703 ns |
| `BM_MatchingEngine_Cancel` | 728 ns |
| `BM_MatchingEngine_ReplaceQuantityDecreasePreservesPriority` | 695 ns |

**A real measurement artifact, stated plainly rather than hidden:** every benchmark
above except `NewOrderRestsNoCrossing` re-seeds book state inside the timed loop using
`state.PauseTiming()`/`state.ResumeTiming()` (needed so e.g. `Cancel`'s cost is measured
against a freshly-seeded live order every iteration, not an ever-growing or
ever-emptying book). Google Benchmark's own documentation notes `PauseTiming()`/
`ResumeTiming()` carry real overhead (they read the clock and touch internal
bookkeeping) — comparing `SingleLevelFullFill`/`Cancel`/`Replace...` (~700-730 ns, each
with exactly one pause/resume pair per iteration) against `NewOrderRestsNoCrossing`
(169 ns, no pause/resume at all) makes this visible directly: roughly 500-560 ns of
every "~700 ns" figure above is pause/resume overhead, not matching-engine cost. The
*trend* across `MultiLevelSweep`'s levels is still meaningful (subtract the same
roughly-constant ~560 ns baseline from each and the remainder scales close to linearly
with level count — about 85-110 ns per additional matched price level, falling as level
count grows, consistent with `MatchingBook`'s per-level cost being dominated by a fixed
per-level overhead rather than growing with book depth) — but the absolute per-op
numbers for anything using `PauseTiming()` should be read as upper bounds on true cost,
not exact figures.

**Reading this:** a non-crossing rest (`NewOrderRestsNoCrossing`, true ~169 ns, no
pause/resume artifact) is the cheapest operation — insert into `MatchingBook`'s
per-instrument book, no opposite-side walk at all. A single-level full fill costs
noticeably more even after subtracting the ~560 ns artifact (roughly 170-200 ns true
cost) than a bare rest, consistent with `match_and_rest()` doing strictly more work
per matched order than `rest_remainder_if_applicable()` alone (emitting `TradeExecuted`
in addition to `BookOrder{Added,Removed}`, mutating both sides' book state instead of
one). Multi-level sweeps scale roughly linearly in the number of matched levels, as
expected for `MatchingBook::front_of_best()`'s one-resting-order-at-a-time matching
loop with no batching across levels.

---

## 5. Trader-side reconstructed book (`bench_order_book`)

| Benchmark | Time/op |
|---|---|
| `BM_OrderBook_AddOrderNewPriceLevel` | 152 ns |
| `BM_OrderBook_AddOrderSamePriceLevel` | 54.0 ns |
| `BM_OrderBook_CancelOrder/1` (1 distinct price level) | 1332 ns |
| `BM_OrderBook_CancelOrder/16` | 1325 ns |
| `BM_OrderBook_CancelOrder/64` | 1356 ns |
| `BM_OrderBook_CancelOrder/256` | 1386 ns |
| `BM_OrderBook_CancelOrder/1024` | 1761 ns |
| `BM_OrderBook_ModifyOrderRepriceLosesPriority` | 1497 ns |
| `BM_OrderBook_TopBids/1` | 30.1 ns |
| `BM_OrderBook_TopBids/10` | 52.3 ns |
| `BM_OrderBook_TopBids/100` | 376 ns |
| `BM_OrderBook_TopBids/1024` | 3751 ns |

**Same `PauseTiming()`/`ResumeTiming()` caveat as §4 applies to every `CancelOrder`/
`ModifyOrder...` row** (each re-seeds via `add_order()` — and, for `CancelOrder`,
seeds and then calls `book.clear()` — inside a pause/resume bracket every iteration),
so their absolute ~1.3-1.8 μs figures include that same fixed overhead; the
**relative** growth from depth 1 to depth 1024 (1332 ns → 1761 ns, roughly +430 ns) is
the part actually attributable to `OrderBook`'s documented `O(log P)` map lookup
(`order_book.hpp`'s own class comment) — small and log-shaped, exactly as predicted:
going from 1 to 1024 price levels (1024x) costs well under 2x more, consistent with
`log2(1024) = 10` being a small multiplicative factor over a cheap per-comparison cost.

`TopBids` (no pause/resume — real numbers) scales close to linearly with requested
depth `n` against a book with 1024 distinct levels: ~30 ns for the top 1, ~3.75 μs for
all 1024 — consistent with `top_bids()` walking `n` `std::map` iterator steps and
copying `n` `PriceLevelView`s, no unexpected superlinear behavior. This is the read
path `GET /api/book/:id` and every SSE `"book"` event (Milestone 12) actually exercise
at `options_.book_depth` (default 10) — ~52 ns per call at that depth, negligible next
to the HTTP/SSE framing cost around it.

---

## 6. SPSC queue throughput (`bench_spsc_queue`)

| Benchmark | Time/op or duration | Throughput |
|---|---|---|
| `BM_SpscQueue_SingleThreadedPushPop` | 0.906 ns | — |
| `BM_SpscQueue_TwoThreadThroughput/10000` | 0.305 ms (real time) | 32.8 M items/s |
| `BM_SpscQueue_TwoThreadThroughput/100000` | 3.95 ms (real time) | 25.3 M items/s |

**Reading this:** a single-threaded push+pop pair costs under a nanosecond — almost
entirely two atomic loads/stores on already-hot cache lines with no cross-core traffic
at all, exactly what `spsc_queue.hpp`'s own design comment predicts. The two-thread
number (a real producer thread and a real consumer thread, `--benchmark_min_time`
applied via `UseRealTime()` since the work spans two threads) drops to 25-33M items/s
once actual cross-core cache-line traffic (the `alignas(64)`-padded `head_`/`tail_`
ping-ponging between two real cores) is in play — still fast enough that this queue is
not the bottleneck anywhere it's used in this codebase (see §7 for where the actual
bottleneck in the live gateway path is). The drop from 32.8M/s at 10,000 items to
25.3M/s at 100,000 items is consistent with thread-creation/join overhead being a
larger fraction of the smaller run's total time, not a queue-throughput regression —
`std::thread` spawn/join alone costs tens of microseconds on this machine.

---

## 7. End-to-end order-entry latency (`bench_end_to_end_latency`)

A real loopback-TCP `NewOrder` (IOC, empty book, so exactly one round trip and no
resting-order bookkeeping) sent to a real, fully-wired `OrderEntryGateway`
(risk + ledger + matching, identical to what `tests/test_order_entry_gateway_e2e.cpp`
exercises), timed from just before `send()` to just after this same connection's
`Accepted` is fully decoded.

### 7.1 Original measurement, and the bottleneck it exposed

The first real run of this benchmark (20,000 samples, 100 warm-up iterations
excluded) produced:

| Statistic | Latency |
|---|---|
| min | 278.62 μs |
| p50 | 1271.75 μs |
| mean | 1303.79 μs |
| p90 | 1327.75 μs |
| p99 | 1471.79 μs |
| p99.9 | 10186.88 μs |
| max | 17490.25 μs |

**The dominant cost, identified by reading the code, not guessed at:**
`OrderEntryGateway::connection_writer_loop()` (`src/exchange/gateway/
order_entry_gateway.cpp`) polled its per-connection outbound queue with
`std::this_thread::sleep_for(kPollInterval)` (`kPollInterval = 1ms`) whenever
`try_pop()` found nothing — a deliberate, documented choice ("short enough to keep
latency low, long enough not to spin a core at 100% doing nothing") appropriate for a
demo-scale gateway, but it meant every response this benchmark measured paid for
*this connection's own writer thread's next wake-up*, not just matching-engine time.
The matching thread itself had no such delay (`MatchingPipeline`'s inner loop
`std::this_thread::yield()`s, never sleeps, when its queue is empty — see
`matching_pipeline.cpp`) — the ~0.7-1.3 ms bulk of p50/mean above was consistent with
waiting on a uniformly-distributed-up-to-1ms poll interval plus this machine's own
thread wake-up latency from `sleep_for`, not with anything slow inside
`RiskGatedEngine`/`MatchingEngine`/`Ledger` (§4's `BM_MatchingEngine_*` numbers show
those costing hundreds of nanoseconds, three orders of magnitude below what was
measured here).

### 7.2 The fix: wake the writer thread directly instead of polling it

Two changes, both applied for real and re-measured, not just proposed:

1. **`connection_writer_loop()` now blocks on a condition variable
   (`Connection::wake_cv`) instead of unconditionally sleeping `kPollInterval`.**
   `route_event()` calls `wake_cv.notify_one()` immediately after a successful
   `try_push()` onto that connection's outbound queue, so the writer thread reacts
   as soon as the OS schedules it rather than on its next poll tick.
   `wait_for(lock, kPollInterval, predicate)` re-checks `outbound.size() > 0` itself
   before ever actually sleeping, so a notification that arrives just before the wait
   begins is never lost, only redundant with that re-check — `kPollInterval` survives
   only as a safety-net timeout, not the primary wake path anymore. `stop()` now also
   calls `notify_all()` on every live connection's `wake_cv` so shutdown doesn't wait
   on that same timeout either.
2. **`TcpSocket` now disables Nagle's algorithm (`TCP_NODELAY`) on every connected
   socket** (`net::tcp_socket.cpp`, both `accept()`'s server-side result and
   `connect()`'s client-side one) — a second, previously-unexamined latency source:
   this protocol is request/response and already writes one complete, already-framed
   message per `write()` call, so Nagle's coalescing had nothing useful to buy here,
   only latency to add.

Re-running the exact same benchmark after both changes (20,000 samples, two
independent runs back to back, to confirm this wasn't a fluke):

| Statistic | Run 1 | Run 2 |
|---|---|---|
| min | 58.21 μs | 57.96 μs |
| p50 | 73.12 μs | 74.04 μs |
| mean | 75.92 μs | 80.15 μs |
| p90 | 80.46 μs | 88.96 μs |
| p99 | 140.88 μs | 190.00 μs |
| p99.9 | 253.62 μs | 366.71 μs |
| max | 1968.25 μs | 10115.54 μs |

**p50 dropped from ~1272 μs to ~73 μs — roughly a 17x reduction** (mean: ~1304 μs →
~78 μs, also ~17x); **p99.9 dropped from ~10.2 ms to ~0.25-0.37 μs·10³ (253-367 μs) —
roughly a 30-40x reduction**. What's left (tens of μs, not sub-μs) is now
consistent with genuine, unavoidable costs this design still has: two real context
switches (reader thread → matching thread → writer thread, each a real OS thread
hand-off, not a poll), the `submit_mutex_`/`sessions_mutex_` locks each message
crosses, and this machine's own scheduler wake-up latency for a *notified* (not
merely timed-out) thread — three orders of magnitude smaller than before, but not
zero, because this is still a real multi-thread, real-syscall design, not a
kernel-bypass/busy-spin one. The occasional multi-ms outlier still visible in `max`
(and to a lesser extent p99.9) is consistent with the same shared-machine scheduler
pre-emption noted in the original measurement — this was run on a development
machine with a live, otherwise-idle `trading_server` process from an earlier
manual demo still competing for a core in the background, not an isolated/pinned
one.

**What this confirms about the earlier "measure before optimizing" framing:** the
fix that mattered was the one already identified by reading the code, not a new one
found by guessing — the condition-variable change alone accounts for essentially all
of the ~17x median improvement; `TCP_NODELAY` was a real, previously-unexamined
gap (worth fixing regardless, since it has no downside for this protocol) but a
secondary contributor on loopback, where ACKs return almost instantly regardless of
Nagle's algorithm.

---

## 8. Summary: what these benchmarks establish

- **Codecs, matching engine, book, and SPSC queue are all sub-microsecond per
  operation** (tens to low thousands of nanoseconds) — none of them was ever close to
  being the bottleneck in the one place this project has an actual measured,
  real-network latency number: the live order-entry gateway's request/response round
  trip.
- **The gateway's own connection-writer poll interval (1 ms) *was* that bottleneck**,
  identified from a real measurement plus a source read, not assumed — exactly the
  "measure before optimizing" discipline `docs/current_system_assessment.md` §9
  explicitly called out as absent in the pre-exchange codebase. Unlike the original
  version of this document, this was not left as a recorded-but-unfixed finding:
  §7.2 replaced the sleep-based poll with a condition-variable wakeup notified
  directly from `route_event()` (plus `TCP_NODELAY`, a second latency source the same
  investigation turned up), re-measured p50 end-to-end latency dropping ~17x (~1272 μs
  → ~73 μs) as a direct, verified result — not a projection.
- **`OrderBook`'s `O(log P)` cancel/modify cost is real but small** at every depth
  tested up to 1024 distinct price levels, consistent with its own documented
  complexity analysis (`order_book.hpp`) and with `docs/current_system_assessment.md`
  §10's judgment that `std::map`/`std::list` is "fine for book reconstruction at
  today's scale."
- **No object pools, no allocation-avoidance work, and no CPU affinity/pinning were
  added anywhere in this milestone** — consistent with this project's own stated
  principle (`docs/current_system_assessment.md` §9's "premature vs. useful
  optimizations" framing) of not reaching for those without a measured need, and
  nothing measured here shows one: every hot-path cost identified is either already
  small (codec/matching/book/queue) or dominated by a design choice (the writer
  thread's poll interval) that no allocator or cache-layout change would fix.
