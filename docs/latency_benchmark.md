# End-to-end order-path latency

**Status:** measured on a real `OrderEntryClient` ↔ TCP ↔ `OrderEntryGateway` path.
Numbers below are from a Release build on the date noted, not estimates.

This is a different question from [`docs/benchmarks.md`](benchmarks.md): that document
times codecs, the matching engine in isolation, and a client-side round-trip with no
stage breakdown. This document times the **live order path**, splits it into stages,
and records how the distribution moves under load.

---

## 1. Measurement methodology

The primary harness is `bench_order_path_latency`. It:

1. Times `MatchingEngine::process()` on the same IOC empty-book command, in
   process, with no sockets. That number is the matcher ceiling.
2. Starts a real `OrderEntryGateway` (the same TCP order-entry object
   `trading_server` uses; matching, risk, and ledger are behind it).
3. Connects with the production `OrderEntryClient`.
4. Submits `NewOrder` (IOC, empty book → one `Accepted` per command, no book
   growth) over loopback TCP.
5. Stores **raw per-order tick samples**, then computes percentiles after the
   timed region. Statistics are not updated on the hot path.

These two surfaces print on every TCP run. They are different ceilings.
Matcher ops/s is not the exchange's throughput.

**Matching-thread ceiling and the other capacity questions** live in
`bench_capacity`, not here. Empty-book IOC `MatchingEngine::process()` is a
microbenchmark of the matcher in isolation; it is **not** the headline
matching-thread number. See [§ Capacity program](#capacity-program) below.

`trading_server` is not spawned as a second process: its order-entry surface **is**
`OrderEntryGateway` over TCP. Spawning the UI/UDP process would confound the
measurement with HTTP and market-data publish. Cross-process tracing would also need
shared memory; the timestamps here are correlated in one process on a shared
monotonic counter.

Isolated matching-engine benches remain in `benchmarks/bench_matching_engine.cpp`
(GTC rest, fills, sweeps). Those are a different command shape than the IOC
empty-book used here. Neither is a TCP number.

How to run:

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j --target bench_order_path_latency
./build-release/bench_order_path_latency
```

Optional flags: `--workload sequential|sustained|multi|burst|soak|idle|core|all`,
`--sequential-samples N`, `--sustained-samples N`, `--max-clients N`,
`--multi-samples N`, `--idle-clients N`, `--idle-samples N`,
`--writer-batch N`, `--soak-orders N`, `--soak-seconds S`, `--market-data`,
`--matching-cpu N` (Linux only; pin the matching thread — see §19),
`--mlockall` (Linux only, off by default; `mlockall(MCL_CURRENT | MCL_FUTURE)`
before any timed work, so paging can be A/B’d against the unlocked run).
`mlockall` with `MCL_FUTURE` fails if `RLIMIT_MEMLOCK` is too small
(`ulimit -l`); the bench prints `strerror(errno)` and exits rather than
continuing unlocked.

`--workload soak` and `--workload idle` are **not** part of `all`. Soak: see
§16. Idle: many long-lived sessions, one active sender; see §18. `--workload
multi --max-clients N` sweeps 1, 2, 4, … N flood clients (default N=16).

Production is matcher `yield()` plus **one** `IoPoller` I/O thread
(`kqueue` on macOS/BSD, `epoll` on Linux) with opportunistic outbound
batching (default `--writer-batch 4`; never waits to fill). Matching never
writes a socket; it enqueues onto a per-connection SPSC and wakes the poller
via a dirty list. `--writer-batch` remains so the §14.3 tables can be
reproduced. Wait-policy and shared-writer switches, and later the
per-connection reader/writer threads, were measurement tools or superseded
architecture; they are documented in §13–§15 and §20.

Debug and sanitizer builds are not comparable to the tables below.

---

## 2. Clock / timestamp source

The same tick source as `include/exchange/testing/hr_timer.hpp`, extracted to
`include/common/monotonic_ticks.hpp` so production stamps and the harness convert
the same counter.

On this machine: **AArch64 `CNTVCT_EL0`**, `isb`-serialised.

| Property | Value |
|---|---|
| Declared rate | 1_000_000_000 Hz |
| Measured rate | ~1_000_000_000 Hz |
| Effective resolution | **42 ns** (hardware steps ~41–42 ticks) |
| Cost of one read | ~11.5 ns (a sampled interval pays this twice) |

Individual sub-microsecond samples are quantised. End-to-end order latency here is
tens of microseconds and up, so the resolution is not the limiting factor.
Percentiles are taken over 2_000–32_000 raw samples per workload.

When tracing is **disabled** (the default), each stamp is a relaxed atomic load and
a return. The slot table is allocated only in `Tracer::enable()`. Stamps are
lock-free stores; the matching thread never takes a mutex for metrics.

---

## 3. Timestamp boundaries

Correlation key: `(account_id, client_order_id)` from the inbound command (replace
uses `original_client_order_id`). Multiple execution reports for one command update
the same slot (`t5` first and last, report counts).

| Stamp | Where | Meaning |
|---|---|---|
| **T0** | `OrderEntryClient::send()`, before encode | Client submit |
| **T1** | Gateway I/O thread, after a complete frame decodes | Server has the order |
| **T2** | Matching thread, before `RiskGatedEngine::process` | Exchange processing begins |
| **T3a** | `deliver()`, first private report queued | First execution event |
| **T3** | Matching thread, after `process` returns | Exchange processing complete (includes routing onto the outbound SPSC queue) |
| **T4a** | Same `deliver()` | Handed to the connection outbound queue (dirty-list wake of the I/O thread) |
| **T4** | I/O thread, after `write()` completes | Response left the server socket |
| **T5** | Client reader, after decode, before the user sink | Client has the first execution report |

Derived intervals:

| Interval | Formula | What it includes |
|---|---|---|
| `client_to_server` | T1 − T0 | Client encode + `write()`, loopback TCP, server `read()` + decode |
| `server_pre_match` | T2 − T1 | Ownership claim, lock-free MPSC `submit()`, matching-thread wakeup, then `CommandSequencer` immediately before `process` |
| `exchange_processing` | T3 − T2 | Risk + ledger + matching + `route_event()` (no writer `write()`) |
| `writer_handoff` | T4 − T4a | I/O-thread wake (dirty list / poller), encode, `write()` |
| `server_to_client` | T5 − T4 | Loopback TCP + client `read()` + decode |
| `end_to_end` | T5 − T0 | First execution report back to the client |

T4 is stored *after* `write()` returns. The client's T5 can therefore land a few
ticks earlier on another core; inverted `server_to_client` samples are dropped
rather than wrapping a `uint64`.

Rejected, resting (`Accepted` only), and crossing (Accepted + TradeReport) orders
are covered by `tests/test_latency_tracer.cpp`. Tracing off leaves no slot and does
not change wire contents.

---

## 4. Benchmark environment

- **Date:** mutex+SPSC tables 2026-08-27; MPSC re-run 2026-08-28
- **Host:** Apple M3 Pro, `arm64`, macOS 26.5.2 (Darwin 25.5.0)
- **Build:** AppleClang, `-DCMAKE_BUILD_TYPE=Release`, same warning set as the rest of the repo
- **Load:** development laptop, not an isolated/pinned machine. `max` and sometimes
  p99.9 include scheduler preemption.
- **Later Linux isolation numbers** (GCP Xeon, pinned matcher, `isolcpus=1`) are
  §19. They do not replace the laptop tables above; they answer a different
  question (what `server_pre_match` looks like when the matcher actually owns a
  core).

---

## 5. Workloads

| Workload | What it does |
|---|---|
| **Sequential** | One client; wait for T5 before the next send. Minimal queueing. |
| **Sustained** | One client; offered 1k / 10k / 50k / 100k orders/s. If send falls behind the schedule, the harness **does not** accumulate debt into a catch-up burst; achieved rate is reported. |
| **Multi-client** | Powers of two up to `--max-clients` (default 16). Each client blasts its sample count then waits. This is an in-flight flood, not a paced fair-share test, and not a model of member-firm sessions. |
| **Idle population** | Powers of two up to `--idle-clients`. All sessions stay connected; **one** sends sequential IOCs. Historically the 2N+2 thread-per-connection tax (§18); now fd/poller cost plus N client readers (§20). Not part of `all`. |
| **Burst** | 8 clients; 40 rounds of 100 orders then 10 ms idle. |
| **Matching-core** | Same IOC empty-book command, `MatchingEngine::process()` only. Always printed with TCP runs. |

Gateway queues in the harness are 8192 (inbound and outbound) so the measurement is
not an artificial 1024-slot cliff.

---

## 6–8. Results

Same harness (`bench_order_path_latency`, Release, Apple M3 Pro). All times are
**microseconds**. Throughput is achieved orders/s over the timed window.

### Mutex + SPSC (after encode-buffer reuse and notify-outside-lock)

This is the last measured curve with `submit_mutex_` serialising every reader onto
an SPSC matching queue. It is the baseline for the ingest redesign in §10.

| Workload | Clients | Achieved throughput | p50 | p90 | p99 | p99.9 | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Sequential | 1 | 11_572 | 74.74 | 79.93 | 127.61 | 212.96 | 1_058 |
| Sustained 1k/s | 1 | 1_000 | 216.33 | 225.33 | 283.47 | 6_125 | 10_025 |
| Sustained 10k/s | 1 | 10_001 | 67.49 | 73.74 | 173.61 | 326.90 | 738 |
| Sustained 50k/s | 1 | **35_202** | 24_588 | 42_282 | 43_818 | 43_982 | 44_041 |
| Sustained 100k/s | 1 | **34_789** | 33_207 | 52_804 | 55_255 | 55_416 | 55_452 |
| Multi (flood) | 1 | 36_906 | 8_507 | 12_750 | 13_155 | 13_206 | 13_219 |
| Multi (flood) | 2 | 40_397 | 51_836 | 62_190 | 64_361 | 64_570 | 64_613 |
| Multi (flood) | 4 | 16_183 | 380_550 | 449_141 | 456_014 | 457_543 | 457_627 |
| Multi (flood) | 8 | 14_304 | 573_368 | 1_038_418 | 1_079_421 | 1_086_026 | 1_086_762 |
| Multi (flood) | 16 | 23_268 | 979_873 | 1_172_099 | 1_265_675 | 1_276_286 | 1_282_295 |
| Burst | 8 | 21_858 | 741_026 | 964_176 | 1_089_225 | 1_111_003 | 1_113_483 |

### Lock-free MPSC (no `submit_mutex_`)

Re-run of the **same** workloads after gateway readers call `MatchingPipeline::submit()`
concurrently. Date: 2026-08-28.

| Workload | Clients | Achieved throughput | p50 | p90 | p99 | p99.9 | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Sequential | 1 | 10_804 | 75.47 | 85.30 | 147.92 | 210.37 | 1_160 |
| Sustained 1k/s | 1 | 1_000 | 214.29 | 243.06 | 3_545 | 9_482 | 12_127 |
| Sustained 10k/s | 1 | 9_997 | 68.83 | 113.32 | 214.50 | 1_008 | 1_485 |
| Sustained 50k/s | 1 | **34_132** | 26_234 | 43_784 | 44_780 | 44_862 | 44_874 |
| Sustained 100k/s | 1 | **33_966** | 35_211 | 54_620 | 56_176 | 56_421 | 56_443 |
| Multi (flood) | 1 | 35_371 | 9_470 | 13_698 | 14_130 | 14_194 | 14_210 |
| Multi (flood) | 2 | 38_417 | 55_072 | 64_962 | 66_280 | 66_360 | 66_404 |
| Multi (flood) | 4 | 16_654 | 317_510 | 431_310 | 437_010 | 437_881 | 437_947 |
| Multi (flood) | 8 | 13_393 | 614_574 | 1_128_066 | 1_166_181 | 1_172_265 | 1_172_931 |
| Multi (flood) | 16 | 18_984 | 854_899 | 1_140_509 | 1_582_653 | 1_597_577 | 1_598_401 |
| Burst | 8 | 20_776 | 867_886 | 1_000_172 | 1_133_082 | 1_174_930 | 1_175_495 |

50k and 100k **offered** rates are still not sustained. The pipelined ceiling is
about **34k orders/s** on this machine for this IOC/empty-book shape — the same
knee as the mutex path within run noise.

### Saturation curve (mutex+SPSC vs MPSC)

| Workload | Mutex achieved /s | MPSC achieved /s | Mutex p50 e2e µs | MPSC p50 e2e µs |
|---|---:|---:|---:|---:|
| Sequential | 11_572 | 10_804 | 75 | 75 |
| Sustained 10k | 10_001 | 9_997 | 67 | 69 |
| Sustained 50k | **35_202** | **34_132** | 24_588 | 26_234 |
| Sustained 100k | **34_789** | **33_966** | 33_207 | 35_211 |
| Multi 1 | 36_906 | 35_371 | 8_507 | 9_470 |
| Multi 2 | 40_397 | 38_417 | 51_836 | 55_072 |
| Multi 4 | 16_183 | 16_654 | 380_550 | 317_510 |
| Multi 8 | 14_304 | 13_393 | 573_368 | 614_574 |
| Multi 16 | 23_268 | 18_984 | 979_873 | 854_899 |
| Burst 8 | 21_858 | 20_776 | 741_026 | 867_886 |

The offered-rate knee does not move. Removing the mutex did not raise the
pipelined ceiling; extra load is still in-flight TCP, not a contended
`submit_mutex_`. Multi-client p50 is run-to-run noisy (four clients improved;
eight and burst did not). That is consistent with floods into socket buffers,
not with ingest lock contention.

### Sequential breakdown (p50, µs) — MPSC

| Interval | p50 | Share of e2e p50 |
|---|---:|---:|
| `client_to_server` | 32.77 | 43% |
| `server_pre_match` | 4.42 | 6% |
| `exchange_processing` | 1.88 | 2% |
| `writer_handoff` | 15.63 | 21% |
| `server_to_client` | 22.13 | 29% |
| **end_to_end** | **75.47** | 100% |

Matching/risk/ledger (`exchange_processing`) is still about **2 µs**.
`server_pre_match` did not shrink in a way that matters: it was never the
mutex sitting in the sequential RTT.

### Load breakdown (p50, µs) — MPSC

| Workload | client_to_server | pre_match | exchange | writer | server_to_client | e2e |
|---|---:|---:|---:|---:|---:|---:|
| Sustained 10k/s | 30.85 | 3.88 | 1.71 | 13.92 | 19.64 | 68.83 |
| Sustained 50k/s | 12_277 | 4.63 | 0.92 | 30.64 | 13_939 | 26_234 |
| Burst 8 clients | 418_153 | 11.48 | 0.29 | 402 | 86_041 | 867_886 |

At 10k/s the machine is busy enough that writer/matching threads stay warm, so p50
is **better** than sequential. At 1k/s those threads sleep; p50 jumps above 200 µs,
and `server_pre_match` p99.9 is several milliseconds (`yield()` on an empty matching
queue).

Once offered rate exceeds ~34k/s, **T0→T1 and T4→T5 explode together** while
exchange stays ~1–2 µs and writer handoff stays tens of µs. That is socket and
in-flight queueing, not a slow matcher and not the ingest queue's CAS.

---

## 9. Identified bottleneck

**Not the matching engine.** Isolated `bench_matching_engine` already showed
non-crossing rest in the low hundreds of nanoseconds; the live path confirms
risk+match+route at ~2 µs even under load.

**Sequential / 10k/s:** three OS thread hops (client send → gateway reader →
matching → writer → client reader) plus loopback TCP. `writer_handoff` (~16 µs)
is the largest *server-side* stage; `client_to_server` (~32 µs) is the largest
overall, and most of that is syscalls and the NIC/loopback, not encode.

**Idle (1k/s, this laptop):** matching `yield()` and writer `wait_for` put cores
to sleep. Wakeup, not work, dominates p50. That reading does **not** survive a
genuinely isolated matching core — see §19.3. Hundreds of microseconds of
`server_pre_match` on Linux were an affinity bug, not `yield()`.

**Above ~34–35k/s offered, and any flood of concurrent clients:** the process
cannot drain commands as fast as they are written. Matching is still one thread;
each response still wakes a per-connection writer; in-flight builds in TCP
buffers. Gateway ingest is no longer a mutex in front of an SPSC queue — readers
race a lock-free MPSC ring — and that change did **not** move the knee. The
limiter was never `submit_mutex_`.

`read_buffer.erase(...)` and per-event `to_execution_reports` vectors are real
costs but sit inside the 2 µs exchange window — they are not where milliseconds
appear.

---

## 10. Changes on this path

**Outbound (earlier, still in the mutex-era table).** Encode-buffer reuse and
notifying writers outside `sessions_mutex_`. Those did not move the pipelined
ceiling.

**Inbound ingest (this milestone).** `OrderEntryGateway` no longer takes
`submit_mutex_` around `MatchingPipeline::submit()`. The pipeline queue is
`MpscQueue`: producers CAS-reserve a slot; a per-slot publish ticket keeps FIFO
as reservation order even if stores complete out of reservation order. The
matching thread is the only consumer. `command_sequence` is assigned there,
after `try_pop()`, immediately before `process()`. A full-queue `submit()`
returns `false` and consumes no sequence number.

Explicit rules:

1. **Per-producer FIFO.** One reader thread that submits A then B has A matched
   before B. A session is one producer.
2. **Cross-session order** is the lock-free admission race, not TCP arrival time
   and not mutex luck.
3. **Matching stays single-threaded.** Concurrent `submit()` is not concurrent
   `process()`.

---

## 11. Before / after

**Encode / writer notify** (mutex still present): sequential p50 stayed ~75 µs
(run noise). 10k/s p50 and p99 tightened vs the first staged run; burst drain
rose. The ~35k/s ceiling did not move.

**MPSC ingest:** sequential p50 is still ~75 µs. Sustained 50k/100k achieved
rate is ~34k/s vs ~35k/s before — same saturation, slightly lower this run
(CAS on the ring vs a short mutex, plus laptop noise). The point of the change
is sequencing that can be stated as rules, not a higher throughput number we
did not measure.

No correctness was traded for these numbers. Pipeline concurrent-producer tests
and the MPSC queue tests cover FIFO and gapless sequences.

---

## 12. Remaining limitations

- Loopback TCP on macOS, not kernel-bypass, not a real NIC, not Linux.
- One matching thread by design. Busy-spin on matcher+writer raised sequential
  p50 (~71 → ~57 µs) and the pipelined knee only modestly (~34.5k → ~37–40k/s).
  The leftover ceiling is loopback TCP and in-flight, not another `yield()`.
  Production matcher and writers sleep; they do not busy-spin.
- Multi-client rows are floods. A paced N-client test would look more like
  sequential × N until the 35k/s ceiling.
- T4-after-`write()` can invert vs T5 by a few ticks; those interval samples are
  omitted.
- Slot table is a power-of-two hash; the harness uses 2^20 slots so 32k in-flight
  ids do not collide. `disable()` only clears a flag (it does not free the table)
  so a stamp that passed the enabled check cannot use-after-free.
- `max` is not a reliable SLA on a laptop.
- ASan/UBSan/TSan cover functional tests, not these Release timings.

Related: [`docs/benchmarks.md`](benchmarks.md) §7 (earlier writer-poll fix),
[`docs/matching_engine_optimisation.md`](matching_engine_optimisation.md).

---

## 13. Thread-handoff experiment (yield vs busy-spin)

Laptop, unpinned, matcher **and** writers. A later Linux experiment that pinned
only the matcher (§19.4) is the one that decides production idle policy. Do not
read the hundreds-of-microseconds `server_pre_match` numbers in this section, or
the “`yield()` is the wakeup tax” story, as applying to an isolated matching
CPU: that conclusion was wrong when the matcher had not actually been pinned.

**Question:** is the ~35k/s knee scheduler wakeup (reader→matcher `yield()`,
matcher→writer `wait_for`), or the TCP path?

These runs used an opt-in `WaitPolicy` on matcher and writers. That switch
was removed after the experiment (production always yields/sleeps). The
tables stay as the record.

At the time of measurement, production default was `WaitPolicy::Yield`. The
bench could opt into `BusySpin` (and optional `writer_batch`) without turning
the live server into a core-burner.

Same harness, Release, Apple M3 Pro, 2026-08-28. Yield numbers below are a
fresh run of this binary (occupancy + `getrusage` probes added; they sit off
the encode path except a `matching_queue_size()` load after each send).

### Sequential stages (p50, µs)

| Interval | yield | busy (batch=1) | busy + batch=32 |
|---|---:|---:|---:|
| `server_pre_match` | 3.63 | **0.42** | 0.50 |
| `exchange_processing` | 1.42 | **0.29** | 0.29 |
| `writer_handoff` | 15.31 | **11.38** | 8.88 |
| `end_to_end` | 71.20 | **57.39** | 58.58 |
| switches/order (`nvcsw+nivcsw`) | 11.07 | **3.16** | 2.85 |

Matcher idle `yield()` was several microseconds of `server_pre_match`. Writer
CV wakeup is a few microseconds of `writer_handoff`. Busy-spin removes those.
Sequential RTT still has ~28 µs `client_to_server` + ~18 µs `server_to_client`.

Idle 1k/s e2e p50 stays ~200 µs even when spinning: the client is pacing, and
loopback/client-reader sleep still dominate. Spinning does not fix a quiet
path.

### Saturation curve

| Workload | yield /s | busy /s | busy+batch32 /s | yield p50 µs | busy p50 µs | busy+batch32 p50 µs |
|---|---:|---:|---:|---:|---:|---:|
| Sequential | 10_434 | **12_485** | 10_504 | 71 | **57** | 59 |
| Sustained 10k | 10_000 | 10_000 | 9_991 | 72 | **56** | 58 |
| Sustained 50k | **34_556** | **37_221** | **39_666** | 24_417 | 14_281 | **5_931** |
| Sustained 100k | 34_413 | 36_436 | 39_755 | 32_717 | 26_262 | 12_025 |
| Multi 1 | 36_360 | 39_376 | 39_787 | 8_083 | 5_736 | 4_575 |

The knee **moved**, but not by a factor of two. ~15% more pipelined orders/s
(34.5k → 37–40k). At 50k offered, T0→T1 and T4→T5 are still milliseconds;
`exchange_processing` stays sub-microsecond; inbound queue occupancy p50 is
**0** and max is single-digit. The matcher is not the backlog. Extra offered
load is still socket in-flight.

`writer_batch=32` on top of busy-spin cut 50k e2e p50 further (14 ms → 6 ms)
and added a little more throughput. That is fewer `write()` syscalls, i.e. the
TCP path, not another wakeup.

### Occupancy at the knee

After each send, `matching_queue_size()`:

- Sustained 50k yield: p50=0, p99=4, max=7
- Sustained 50k busy: p50=0, p99=1, max=4

The 35k knee is not an inbound ring filling up.

### What busy-spin does *not* fix

Floods with many connections: each connection still has a reader and a writer.
Busy-spin means those writers never sleep. On this laptop, 16 spinning writers
plus matcher plus 16 readers oversubscribe. `busy --writer-batch 32` with 16
flood clients filled the 8192 inbound queue (p50 occupancy 7961, max 8192) and
achieved only ~2k/s. Yield at 16 clients was ~22k/s. Spin is a probe, not a
production policy for N connections.

Syscall counts per order were **not** collected (that wants `dtruss`/`dtrace`
and root). Process-wide context switches from `getrusage` are the substitute:
sequential ~11 switches/order on yield vs ~3 on busy.

### Conclusion

Wakeups **are** a real slice of sequential latency (~14 µs of a ~71 µs RTT)
and a modest slice of the pipelined ceiling (~15%). They are **not** the
thing that turns 50k offered into 24 ms e2e. That remains loopback TCP and
per-connection threads. Production stays on `Yield`. Next lever is the socket
path (shared writer, fewer syscalls in the default policy, or not using TCP
loopback as the measurement), not a hotter poll loop.

---

## 14. TCP/socket and outbound-I/O milestone

### 14.1 Existing socket and thread architecture

The path before this milestone was:

```text
client caller --blocking write()------------------------------+
client reader thread <--blocking read()-------------------+   |
                                                         |   v
server: accept thread -> reader thread / connection -> MPSC -> matcher
                                            matcher -> two SPSC queues
                                                        |
                                      writer thread / connection
                                         CV wake -> blocking write()
```

For N connections, the server owned **2N + 2 threads**: N blocking readers,
N sleeping writers, one accept thread, and one matching thread. The
in-process benchmark also has a client reader for every connection, so its
total scheduler pressure is higher still.

- `OrderEntryClient::send()` stamps T0, takes `write_mutex_`, encodes one
  42-byte request, and loops over blocking `write()` until complete.
- Each gateway reader blocks in `read()` with a 4096-byte buffer, appends to
  `read_buffer`, and decodes every whole frame already present before reading
  again. It claims session ownership and submits to the MPSC queue.
- The matching thread routes each private report under `sessions_mutex_` to
  that connection's bounded outbound SPSC queue. Account-mismatch replies
  use a second SPSC queue because their producer is the reader, not matcher.
- Notification happens after `sessions_mutex_` is released.
- The connection writer wakes, drains replay then session replies then
  matching reports, greedily encodes up to `writer_batch` messages, and
  calls `write()` immediately. It **never waits for a batch to fill**.
- `TCP_NODELAY` is enabled at both ends and connected sockets suppress
  `SIGPIPE` (`SO_NOSIGPIPE` on macOS, `MSG_NOSIGNAL` on Linux). A short read
  or write is legal; both paths preserve the unconsumed remainder.
- On disconnect the reader marks the connection closed, unbinds its session,
  shuts down the socket to unblock its writer, and wakes that writer.
  Connections are kept alive until gateway shutdown so routing maps never
  hold dangling pointers.

There is no old `submit_mutex_` production path left. References before this
section are historical comparison labels. Inbound is MPSC; the remaining
SPSC queues are valid per-connection **outbound** queues.

### 14.2 Opt-in syscall measurements

`OrderEntryGatewayOptions::enable_io_metrics` enables relaxed atomic counters
immediately around the actual server `read()` and `write()` sites. It records
calls, bytes, decoded frames, queued/written reports, errors, and bounded-queue
drops. Disabled is
the production default and costs one relaxed flag load at each site.

The IOC/empty-book workload has one 42-byte request and one 50-byte Accepted:

| Workload/config | frames | reads | frames/read | reports | writes | reports/write |
|---|---:|---:|---:|---:|---:|---:|
| Sequential, batch 1 | 8_000 | 8_000 | 1.00 | 8_000 | 8_000 | 1.00 |
| 10k/s, batch 4 | 8_000 | ~7_920–8_000 | ~1.00 | 8_000 | ~7_920–7_980 | ~1.01 |
| 50k offered, batch 1 | 8_000 | 6_766 | 1.18 | 8_000 | 8_000 | 1.00 |
| 50k offered, batch 4 | 8_000 | 6_787 | 1.18 | 8_000 | 5_169 | 1.55 |
| Multi 16, batch 1 | 32_000 | 2_092 | 15.30 | 32_000 | 32_000 | 1.00 |
| Multi 16, batch 4 | 32_000 | 3_867 | 8.28 | 32_000 | 8_801 | 3.64 |

At low rate, prompt sending correctly remains one report/write. At saturation,
batch 4 removes about **35% of server write syscalls** for one client. Under a
multi-client flood, each writer finds a deeper per-connection queue and removes
about **72%**. TCP already coalesces ingress frames under floods; paced ingress
still costs about one server read per order.

These are server-side counters. They do not include the client-side request
`write()` or response `read()`, so they deliberately do not claim to be whole-
process syscall totals.

### 14.3 Production-wait batching experiment

All rows use production `WaitPolicy::Yield` and the original per-connection
writer. Each configured limit was run with the exact suite; suspicious rows
were repeated.

| Batch limit | 50k achieved/s | 100k achieved/s | writes for 8k reports at 50k | reports/write |
|---:|---:|---:|---:|---:|
| 1 | 34_001 | 33_552 | 8_000 | 1.00 |
| 4 | 41_145 | 42_002 | 5_169 | 1.55 |
| 8 | 41_140 | 43_169 | 5_387 | 1.49 |
| 16 | 41_581 | 41_764 | 5_299 | 1.51 |
| 32 | 41_582 | 41_138 | 5_261 | 1.52 |
| 64 | 41_876 | 41_555 | 5_198 | 1.54 |

The useful step is **1 → 4**. Larger configured limits do not create larger
single-client batches because the writer catches up before that many reports
are simultaneously available.

Low-load latency did not regress: sequential p50 stayed ~71–73 µs and 10k/s
p50 stayed ~72–74 µs across all limits. There is no fill timer and no waiting
for a second report.

Representative full-distribution comparison:

| Workload | Config | Achieved/s | p50 µs | p90 µs | p99 µs | p99.9 µs | max µs |
|---|---|---:|---:|---:|---:|---:|---:|
| Sequential | batch 1 | 11_912 | 71.70 | 76.72 | 118.11 | 152.34 | 694.57 |
| Sequential | batch 4 | 12_063 | 71.47 | 76.20 | 109.51 | 150.63 | 304.80 |
| Sustained 10k | batch 1 | 10_000 | 72.16 | 76.99 | 139.36 | 738.90 | 1_170.73 |
| Sustained 10k | batch 4 | 10_001 | 72.24 | 77.30 | 164.44 | 709.07 | 1_257.51 |
| Sustained 50k | batch 1 | 34_975 | 24_192 | 40_542 | 42_015 | 42_199 | 42_232 |
| Sustained 50k | batch 4 | 41_978 | 1_827 | 3_576 | 4_115 | 4_178 | 4_222 |
| Sustained 100k | batch 1 | 35_167 | 31_008 | 49_206 | 50_605 | 50_748 | 50_765 |
| Sustained 100k | batch 4 | 42_335 | 11_245 | 18_690 | 19_706 | 19_753 | 19_784 |

Repeated batch-4 runs put the 50k achieved rate at 38.5–42.0k/s and 100k at
42.0–42.8k/s. The throughput improvement is repeatable; exact queueing
percentiles move with laptop scheduling. Batch 4 is therefore the new
production default.

### 14.4 Shared-writer design

The alternative implemented for measurement is:

```text
matching thread -> bounded outbound queue per connection
reader replies  -> second bounded queue per connection
                                  |
                                  v
                 one shared non-blocking writer
                    scans fairly across sockets
```

`WriterArchitecture::Shared` changes the server count to **N + 3 threads**:
N blocking readers, one shared writer, accept, and matcher.

The shared writer:

1. Keeps the existing per-connection queues and priority order, preserving
   report order and bounded memory.
2. Encodes an immediately available batch and calls `send(MSG_DONTWAIT)`.
3. Retains partially written bytes and their messages per connection; T4 is
   stamped only when the full batch has left.
4. Processes at most eight batches from one connection per scan before moving
   on, so a hot socket cannot monopolize the loop.
5. Sleeps on one shared CV when idle. If a socket returns would-block, it
   retries after at most 100 µs while new reports still wake it immediately.
6. On a hard write error, marks and shuts down only that connection. Its
   reader performs normal unbinding.

The matching thread still only enqueues. It never writes or waits for socket
writability.

### 14.5 Per-connection versus shared writer

Shared writer was an experimental second architecture. It is no longer in
the tree; these tables are the comparison that justified dropping it.

Representative production-wait runs, batch 4:

| Workload | Per-conn achieved/s | Shared achieved/s | Per-conn p50 µs | Shared p50 µs |
|---|---:|---:|---:|---:|
| Sequential | 12_063 | 11_697 | 71 | 70 |
| Sustained 10k | 10_001 | 9_998 | 72 | 73 |
| Sustained 50k | 41_978 | 40_588 | 1_827 | 4_314 |
| Sustained 100k | 42_335 | 41_411 | 11_245 | 9_989 |
| Multi 1 | 43_341 | 42_218 | 2_336 | 2_817 |
| Multi 2 | 44_259 | 47_527 | 46_567 | 40_160 |
| Multi 4 | 22_881 | 39_068 | 306_994 | 156_516 |
| Multi 8 | 20_042 | 25_264 | 507_396 | 523_953 |
| Multi 16 | 17_329 | 27_461 | 740_116 | 629_108 |
| Burst 8 | 27_754 | 46_737 | 583_516 | 66_315 |

Shared batch 4 was repeated. Sequential stayed 69–70 µs, 50k stayed
40.6–40.7k/s, multi 2 stayed ~47.5k/s, and burst stayed 46.1–46.7k/s.
High-client flood rows remained noisy: multi 8 ranged ~18–33k/s and multi 16
~20–29k/s in clean, fully-drained repeats. That variance is itself evidence
of scheduler pressure from N server readers plus 2N benchmark client threads.

Batch 16 lets the shared writer exploit deeper per-socket queues: burst was
~52–53k/s and multi 8 ~35–37k/s in repeated runs, while sequential remained
~70–72 µs. Multi 16 varied too widely (~19–51k/s) to claim a stable win.

Therefore the shared architecture is retained as an explicit experiment but
is **not** the production default yet. It clearly reduces writer thread count
and improves burst behavior; it does not yet provide stable high-N tails.

### 14.6 Slow clients and backpressure

The shared writer never performs a blocking send. Each connection retains its
own partial buffer and bounded queues, so one EAGAIN cannot stop other sockets.
`SharedWriterServesAnotherClientWhileFirstDoesNotRead` gives the server and
first client small socket buffers, leaves 20,000 replies unread, and verifies
another client still receives its Accepted. Whether macOS actually exposes
EAGAIN is buffer-autotuning dependent (and differs under TSan), so the test
asserts the isolation outcome rather than an OS-specific errno count.

The existing overflow policy is unchanged: a full connection outbound queue
drops that client's report rather than blocking matching. That protects the
engine but is not durable delivery; a real exchange protocol would disconnect
or explicitly gap/reconcile a slow private client.

### 14.7 What limits ~35–40k/s

The evidence now separates the costs:

- Matching/risk remains ~1–2 µs and the inbound MPSC queue is usually empty.
- Busy-spin removed wakeup latency but only moved the old knee ~15%.
- Production-wait batching alone removed ~35% of server write calls and moved
  the knee about **20%**, from ~34–35k to ~41–42k/s.
- Shared writing does not improve single-client saturation beyond batching,
  so one server writer thread is not the single-client limiter.
- Shared writing often helps burst/multi-client throughput because it removes
  N sleeping/waking writer threads, but remaining N reader threads and the
  in-process clients still create large scheduler variance.
- At the new knee, the queueing is still in T0→T1 and T4→T5. The next ceiling
  is the client write/server read and server write/client read TCP path, not
  the matcher or inbound queue.

The original ~35k limit was therefore a combination of **one server write
syscall per report**, TCP buffering, and scheduling. Syscall frequency was
material but not the entire cost.

### 14.8 Cleanup

- No mutex+SPSC inbound implementation or `submit_mutex_` member remains.
- Inbound diagrams/comments say MPSC and matching-thread sequencing.
- SPSC references retained here are outbound queues and are still correct.
- `bench_order_path_latency`, tracing, MPSC history, wait-policy and
  shared-writer **tables**, batching, and syscall counters remain as the
  measurement record. The wait-policy and shared-writer **implementations**
  were later removed; see §15.
- `bench_end_to_end_latency` is retained because its canned echo server
  measures a transport floor not provided by the staged harness; it is not
  used for the comparable workload tables.

### 14.9 Verification

- Normal Debug: 483 tests passed.
- ASan+UBSan: 476 non-UDP-replay tests passed.
- TSan: 476 non-UDP-replay tests passed.
- Warning-clean Debug and Release benchmark builds.

The unrelated `UdpReplayE2E` issue is now characterized rather than hidden:
under ASan, `SingleFrameOverLoopbackReconstructsBookState` sometimes receives
two packets and stops early; under TSan, that same first test can hang.
The normal Debug suite passed all UDP replay tests. This predates and does not
touch TCP order entry.

### 14.10 Remaining limitations

- macOS loopback is not a real NIC and these threads are not pinned.
- Benchmark floods are deliberately unpaced and amplify socket-buffer and
  scheduler effects; high-N results require ranges, not one lucky run.
- Metrics count server I/O only.
- Server I/O at the time of this milestone was still one blocking reader
  thread and one sleeping writer thread per connection. That is no longer
  production; see §20.

The remaining knee is T0→T1 and T4→T5 (TCP in-flight), not the matcher.

---

## 15. Settled production path (after MPSC / batching; before IoPoller)

After the MPSC, busy-spin, batching, and shared-writer measurements, production
was one path **until the I/O multiplexer in §20**:

```text
reader threads → MPSC → single matcher → bounded per-connection outbound queues
                                        → sleeping per-connection writers
```

- Matcher idle is `std::this_thread::yield()`. Writers slept on `wake_cv`.
- Outbound batching is opportunistic and capped at 4 by default: encode
  whatever is already queued, then `write()` immediately.
- Matching never writes sockets. A full outbound queue drops the report.
- Latency tracing and `enable_io_metrics` stay opt-in (one relaxed load when
  disabled).
- Shared writer, `WaitPolicy`, non-blocking `send()`, and socket-buffer
  overrides used only by those experiments were removed from the code. The
  historical tables in §13–§14 are the lesson; the extra implementations
  were not.

The remaining bench-only knob is `--writer-batch N` (default 4) so §14.3 can
be reproduced without a second writer architecture. That knob still applies
on the IoPoller I/O thread.

Verification after this simplification: Debug 479 tests passed; ASan+UBSan and
TSan 472 each with `UdpReplayE2E.*` excluded (same unrelated UDP-replay
limitation as §14.9). Four experimental tests were deleted with the
implementations they covered.

§18–§19 numbers were taken on this 2N+2 path. Current production is §20.

---

## 16. One million orders in a minute (soak)

This is a **paced** TCP soak, not a flood. 1_000_000 IOC `NewOrder`s against an
empty book at 1_000_000 / 60 ≈ **16_667/s** for 60 seconds. That rate sits
between the measured 10k/s (holds ~70 µs p50) and the ~41k/s pipelined ceiling
in §14.3. The question is whether latency, inbound-queue occupancy, and
completions stay flat for a full minute.

```bash
./build-release/bench_order_path_latency --workload soak
# shorter smoke: --soak-orders 20000 --soak-seconds 2
```

What to look at:

- `achieved` ≈ `offered`, wall ≈ 60s
- `received` includes warmup; should be `warmup + sent`
- matching-queue p50/p99 stay near 0 (matcher is not backing up)
- e2e p50 stays in the sequential/10k neighbourhood; p99 should not walk away
  over the minute (percentiles are over the whole run, not a time series)
- `traces` may be a bit below `sent`: the tracer is a direct-mapped table;
  soak uses 2²² slots. Collisions drop samples, not orders.

Not claimed: real NIC, pinned cores, 1M **resting** orders, or multi-instrument
mix. Matcher isolation remains `bench_matching_workload`.

### 16.1 Measurement (Release, Apple M3 Pro, loopback, 2026-08-29)

| | |
|---|---|
| Offered | 16_667/s |
| Achieved | 16_572/s |
| Wall | 60.34 s |
| Sent / received | 1_000_000 / 1_000_200 (incl. 200 warmup) |
| Traces | 1_000_000 |
| Matching queue | p50=0, p99=1, max=158 |
| Outbound drops | 0 |

End-to-end (µs): p50 **82.5**, p90 93.4, p99 203, p99.9 1_235, max 10_303.
`exchange_processing` p50 **1.63 µs**. The p99.9/max tail is still T0→T1 /
scheduler, not the matcher backing up.

A paced million orders in a minute completes. It does not prove the 50k/s
flood path, and max is still a laptop outlier.

---

## 17. Market-data routing thread

### 17.1 Change

`trading_server` used to run the entire market-data path synchronously from
`OrderEntryGateway::extra_event_sink`, on the matching thread:

```text
matching → MarketDataPublisher → pack_frames → send_to(each port)
```

It now uses one `MarketDataRouter`:

```text
matching → MarketDataPublisher → bounded SPSC<protocol::Event>
                                      ↓
                               routing thread
                               pack_frames → send_to(each port)
```

The matching thread still assigns the market-data event sequence and timestamp
before `try_push()`. This is intentional: if the queue is full, the event is
dropped but its sequence number is consumed, so the next received event exposes
a gap. Packet construction and every UDP syscall are on the routing thread.
Private TCP reports are unchanged and never enter this queue.

Queue capacity is 8192. The producer never blocks. Shutdown drains the queue
after the gateway has drained matching. `dropped_count()` and
`queue_high_water_mark()` make overload visible.

### 17.2 Before/after method

`bench_order_path_latency --market-data` changes its normal IOC request into a
GTC buy. Each command therefore produces one private `Accepted` plus one public
`BookOrderAdded`. The public event is translated, packed, and sent by UDP to
loopback. Release, 8,000 timed commands per row, Apple M3 Pro, 2026-08-30.

Both arms were measured with the same binary source, differing only in whether
the sink was invoked from the matching thread or handed to the router. The two
binaries were run **alternately, four times each**, and every number below is
the median of those four runs. This matters: a single inline run measured
sequential throughput at 10,093/s, while the median of four is 11,590/s, so
early single-run figures overstated the gain.

**T2→T3 (`exchange_processing`) — the stage the change targets, µs:**

| Workload | p50 in→rt | p90 in→rt | p99 in→rt | p99.9 in→rt | max in→rt |
|---|---|---|---|---|---|
| Sequential | 21.5 → **3.2** | 23.7 → **3.7** | 34.6 → **16.9** | 79.1 → **34.1** | 117 → **55** |
| Sustained 1k | 23.9 → **3.2** | 27.8 → **4.2** | 61.6 → **18.4** | 119 → **46** | 322 → **156** |
| Sustained 10k | 23.3 → **3.3** | 28.8 → **3.8** | 106 → **21.2** | 170 → **50** | 448 → **298** |
| Sustained 50k | 17.9 → **1.3** | 22.2 → **9.0** | 51.9 → **19.8** | 121 → **31** | 158 → **60** |
| Sustained 100k | 17.7 → **1.2** | 22.5 → **8.8** | 66.7 → **20.1** | 122 → **32** | 812 → **680** |

Every percentile improves, not only the median: the p99.9 of this stage falls
by roughly 3–4× at every rate. The remaining routed p99.9 of 30–50 µs is
scheduler noise on the matching thread, not market-data work.

**End-to-end (T0→T5) and throughput:**

| Workload | achieved/s in→rt | e2e p50 in→rt | e2e p90 in→rt | e2e p99.9 in→rt |
|---|---|---|---|---|
| Sequential | 11,590 → 11,468 | 71.5 → 73.9 µs | 79.1 → 79.7 µs | 222 → 230 µs |
| Sustained 1k | 1,000 → 1,000 | 235 → 256 µs | 280 → 289 µs | 1,760 → 3,001 µs |
| Sustained 10k | 9,998 → 10,000 | 74.4 → **70.5** µs | 113 → **94.0** µs | 1,305 → **399** µs |
| Sustained 50k | 33,057 → **40,800** | 30.4 → **3.49** ms | 49.8 → **6.56** ms | 52.2 → **7.15** ms |
| Sustained 100k | 32,890 → **40,222** | 40.6 → **13.7** ms | 59.3 → **24.7** ms | 61.7 → **24.7** ms |

Across all routed runs every datagram was sent, drops were zero, and queue
high-water was 20–86 of 8,192.

### 17.3 Why an 18–20 µs stage win is not an 18–20 µs end-to-end win

At sequential and 1k/s, end-to-end is flat (within noise, and p50 is even
~2 µs worse). That is not a measurement error, and the stage timings explain it:

| Sequential stage | p50 inline | p50 routed | delta |
|---|---:|---:|---:|
| client_to_server (T0→T1) | 29.9 | 31.0 | +1.1 |
| server_pre_match (T1→T2) | 3.6 | 3.7 | +0.1 |
| exchange_processing (T2→T3) | 21.5 | 3.2 | **−18.3** |
| writer_handoff (T4→T4′) | 16.9 | 16.3 | −0.6 |
| server_to_client (T4′→T5) | 20.4 | 21.8 | +1.4 |
| **end_to_end (T0→T5)** | **71.5** | **73.9** | **+2.4** |

The five stages sum to 92 µs while end-to-end is 71 µs, because they overlap.
`exchange_processing` is `t2_exchange_begin → t3_exchange_end`, spanning the
whole command, whereas `t4_writer_queued` is stamped inside `route_event()`
*before* the command finishes. `MatchingEngine::process_new_order()` emits the
private `OrderAccepted` first and only then rests the order and emits the public
`BookOrderAdded`. So the client's reply was already on its writer queue, being
encoded and written, while the matching thread was still inside `pack_frames()`
and `send_to()` for market data.

The inline UDP cost was therefore concurrent with the reply path, not in series
with it, and removing it shortens no critical path that one client's round trip
traverses. What it does free is the matching thread itself, for the *next*
command. That shows up exactly where serialisation matters:

- **10k/s tail**: e2e p99.9 drops 1,305 → 399 µs, and per-run p99.9 goes from
  `[669, 1097, 1512, 10066]` to `[290, 361, 437, 884]`. Inline, a `send_to()`
  stall let the inbound MPSC back up; the queue-depth p99 was 31 (max 72) in one
  such run. Routed, those stalls stop reaching the order path.
- **Saturation**: throughput rises ~1.22× at both 50k and 100k offered. The
  large e2e improvement there is a consequence of that, not an independent win —
  the client offers more than the system accepts, so e2e is mostly queueing
  delay, which drains faster at higher throughput.
- **1k/s p99.9** (1,760 → 3,001 µs) is the one row that looks worse. The
  per-run spread is `[417, 1410, 2111, 8510]` inline against
  `[428, 756, 5245, 5685]` routed — both dominated by single multi-millisecond
  scheduler outliers on an idle-ish laptop. Four runs cannot separate those, and
  this is not claimed as a regression or an improvement.

Summary: T2→T3 improves 3–7× at every percentile; end-to-end is unchanged at
low load, improves ~3× in the 10k/s tail, and improves largely via throughput
at saturation.

This does not measure UDP receive latency or guarantee delivery. It measures
the order-path cost of publishing real market data and proves that UDP routing
no longer holds the matching thread.

Verification: Debug **481/481** tests passed. ASan+UBSan and TSan each passed
**474/474** with the same seven `UdpReplayE2E` tests excluded as §14.9.

---

## 18. Two ceilings, flood vs idle sessions, and the thread-count knee

A real order-entry gateway serves member firms: a bounded population of
long-lived, well-behaved sessions (tens to low thousands), not tens of
thousands of anonymous connections. This is not a consumer-facing API
gateway. The useful question is not a round concurrent-user number. It is
where the connection-count knee sits for that population.

The tables in this section were measured on the **2N+2 thread-per-connection
gateway** of §15. They remain the record of that tax. Current production is
one I/O thread plus the matcher (§20); re-running idle/flood now measures
poller + N client reader threads, not 2N gateway threads.

The default `multi` workload does not answer member-firm capacity. It is an
in-flight flood: every client sends as fast as `write()` allows, then waits.
That stresses queue depth and loopback TCP, and it already leaves
sequential-RTT territory at two clients. Raising `--max-clients` past 16
does not suddenly reveal a hidden 10k-user path. It confirms the flood is a
latency pile-up, with occasional scheduler storms when too many hot threads
share a laptop.

The idle-population workload is the thread-count test for **that**
architecture: N sessions stay connected (gateway reader blocked in `read()`,
writer asleep on the CV, client reader likewise idle) and **one** session
sends sequential IOCs. Server thread count was `2N+2`.

```bash
./build-release/bench_order_path_latency --workload multi --max-clients 64 --multi-samples 800
./build-release/bench_order_path_latency --workload idle --idle-clients 512 --idle-samples 2000
```

`all` still defaults to `--max-clients 16` and does not run idle.

### 18.1 Two ceilings, same process (Release, Apple M3 Pro, 2026-08-30)

Same IOC empty-book `NewOrder`. Do not mix with `bench_matching_engine`'s GTC
rest (~169 ns) or with a filled book.

| Surface | Achieved | What it includes |
|---|---|---|
| Matching-core only (`MatchingEngine::process`) | **~55–82 M/s** (12–18 ns/op) | Matcher, discard sink, no sockets, no risk, no ledger |
| TCP sequential (1 session, wait for reply) | **~11 k/s** (~73 µs p50) | Real loopback TCP, 2+2 server threads of the §15 gateway, risk, ledger, routing, client reader |
| TCP pipelined (1 session, no wait, §14) | **~35–42 k/s** | Same path, in-flight queueing; e2e is then mostly wait in the pipeline |

The matcher has two orders of magnitude of headroom over the TCP path for
this command shape. End-to-end throughput is sockets and threads.

### 18.2 Flood: raise `--max-clients` past 16

Three Release runs, 400–800 samples/client. Throughput is noisy because the
timed window is a pile-up; p50 is the honest signal.

| Clients | Achieved /s (runs) | e2e p50 |
|---|---|---|
| 1 | 22k / 31k / 24k | 4–9 ms |
| 2 | 43–48k | 8–19 ms |
| 4 | 41–47k | 19–45 ms |
| 8 | 33–58k | 29–153 ms |
| 16 | 24–58k | 48–145 ms |
| 32 | 0.8k / 1.7k / 35k | 79–213 ms |
| 64 | 3.3k / 23k / 34k | 243–619 ms |

There is no throughput knee hiding past 16. Flood p50 is already milliseconds
at one pipelined client and grows with in-flight depth. At 32 and 64, some
runs fall to a few thousand/s with millions of involuntary context switches
— a scheduler storm, not the matcher backing up (`exchange_processing` p50
stays sub-microsecond). That is the design being asked to keep `2N` hot
threads plus `N` client readers runnable at once.

### 18.3 Idle sessions: the thread-count knee

One sequential sender, N−1 silent live sessions. 2,000 timed orders. Same
machine.

| Sessions N | Server threads (2N+2) | Achieved /s | e2e p50 µs | e2e p90 µs | e2e p99 µs | switches/order |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 4 | 10_944 | 72.9 | 94.8 | 163 | 10.9 |
| 8 | 18 | 11_267 | 72.9 | 80.9 | 161 | 11.5 |
| 16 | 34 | 11_213 | 73.0 | 81.4 | 163 | 12.1 |
| 32 | 66 | 10_670 | 73.7 | 92.6 | 182 | 13.6 |
| 64 | 130 | 9_892 | 75.4 | 116 | 176 | 16.9 |
| 128 | 258 | 8_524 | 79.9 | 142 | 211 | 24.8 |
| 256 | 514 | 5_115 | 139 | 266 | 404 | 55.6 |
| 512 | 1_026 | 178 | 222 | 10_535 | 45_392 | 882 |

Through **~64 sessions** the active client's sequential RTT is unchanged
(~73–75 µs p50, ~11 k/s). At **128** p50 is still 80 µs with a fatter p90.
**256** is the first clear tax: p50 almost doubles, throughput halves,
switches/order 5×. **512** is the cliff: p90 jumps to 10 ms, 882 involuntary
switches per order.

A member-firm population of tens to low hundreds of long-lived sessions sits
on the flat part of this curve on this laptop. A few hundred is the beginning
of the tax. A thousand thread-pairs is past the design. That is the number
this architecture owns, not "10k concurrent users."

Linux with pinned cores would still move this knee; it would not turn the
matcher into the limiter. The matcher was never the limiter on this path.
§19 is the Linux pinned-core measurement of that claim, still on 2N+2.
§20 is the later replacement of those 2N gateway I/O threads with `IoPoller`.

---

## 19. CPU isolation and matcher affinity (Linux GCP, 2026-08-30)

Laptop tables in §1–§18 stay the record they were. This section is a later
session on a different machine: a 4-vCPU GCP VM with kernel isolation and an
explicit matching-thread pin. The question was whether `server_pre_match`
(T1→T2: decoded-on-server to matching-thread-begin) is a scheduler-wakeup
tax that isolation can remove.

It is, once the matcher is actually on the isolated CPU. The dominant finding
is not a new idle policy and not a kernel-tuning laundry list. It is that
`--matching-cpu` is optional, `taskset` on the process is inherited, and a
run that looks “isolated” can still have the matcher on CPU0.

### 19.1 Machine and topology

- **Host:** GCP Debian 13 VM, Intel Xeon Platinum 8481C
- **Topology:** 4 logical CPUs / 2 physical cores
  - CPU0 + CPU2 = physical core 0
  - CPU1 + CPU3 = physical core 1
- **Clock:** x86 TSC path of `monotonic_ticks()` (not the AArch64 CNTVCT
  numbers in §2)
- **Build:** Release, `bench_order_path_latency`

Kernel boot parameters:

```text
isolcpus=1
nohz_full=1
rcu_nocbs=1
```

CPU3 is kept **offline** so the matcher’s physical core has no live SMT
sibling. CPU2 stays **online** as CPU0’s sibling, for non-matching work.

Final placement:

| CPUs | Role |
|---|---|
| CPU0 + CPU2 | Benchmark client, TCP gateway readers/writers, accept thread, general process / housekeeping (2N+2 gateway of §15) |
| CPU1 | Matching thread only |
| CPU3 | Offline |

The matching thread is pinned in-process:

```text
--matching-cpu 1
```

The rest of the process is constrained from the shell (nice is not a
scheduling-class change to `SCHED_FIFO`):

```bash
sudo nice -n -20 taskset -c 0,2 ./build-release/bench_order_path_latency --matching-cpu 1
```

`taskset` here lists only the housekeeping CPUs. It does **not** by itself
put the matcher on CPU1. `isolcpus=1` keeps unpinned threads off CPU1;
`--matching-cpu 1` is what moves the matcher onto it. Both are required.

### 19.2 The pinning trap (largest result)

Earlier runs used:

```bash
taskset -c 0 ./build-release/bench_order_path_latency
```

without `--matching-cpu 1`.

`MatchingPipelineOptions::matching_cpu` is unset by default. The matching
thread then inherits the process affinity, so it stays on CPU0 with the
gateway, the clients, and housekeeping — even when the operator believed
CPU1 was “the matching CPU.”

That configuration produced **misleading** `server_pre_match` of typically
**hundreds of microseconds** under sustained traffic. It is easy to blame
`std::this_thread::yield()` on an empty matching queue for that delay. That
blame is wrong. The matcher was not on the isolated core; it was competing
for CPU0, and `yield()` was handing that contended core back to everyone
else.

Once the matcher was genuinely pinned to CPU1, median `server_pre_match`
collapsed to **sub-microsecond**:

| Workload | `server_pre_match` p50 |
|---|---|
| Sequential | ~0.84–0.87 µs |
| Sustained 1k/s | ~0.85 µs |
| Sustained 10k/s | ~0.83–0.85 µs |
| Sustained 50k/s | ~0.81 µs |
| Sustained 100k/s | ~0.85–0.88 µs |

This was the largest and most important latency improvement of the
OS/affinity session. Future runs that omit `--matching-cpu` while using
`taskset` will rediscover the hundreds-of-microseconds number and should
treat it as a misconfigured pin, not as a new matcher bug.

### 19.3 Stable tuned baseline (`yield()`, CPU0+CPU2 / CPU1)

Repeated `yield()` runs on the topology in §19.1. Matcher idle remains
`std::this_thread::yield()` (see §19.5). Matching-core-only and TCP
end-to-end are different surfaces; do not compare them as if they were one
number.

Matching-core only (`MatchingEngine::process`, same IOC empty-book shape):
**~90–92 M ops/s**, **~10.9–11.0 ns/op**.

TCP path, low load:

| Workload | Achieved | e2e p50 | e2e p99 | `server_pre_match` p50 |
|---|---|---|---|---|
| Sequential | (RTT-limited) | ~28.1 µs | ~36–37 µs | ~0.85 µs |
| Sustained 1k/s | ~1k/s | ~28–29 µs | — | ~0.85 µs |
| Sustained 10k/s | ~10k/s | ~26 µs | — | ~0.84 µs |
| Sustained 50k/s | ~50k/s | ~27 µs | — | ~0.81 µs |
| Sustained 100k/s | **~96k/s** | ~29–31 µs | — | ~0.85 µs |

Laptop sequential e2e was ~73 µs p50 (§18.1). The VM sequential ~28 µs is
the same code on a different OS, clock, and pin; it is not a like-for-like
before/after against the M3 tables.

### 19.4 `yield()` vs busy-spin (`_mm_pause()`)

The matching thread idles with `std::this_thread::yield()` when the MPSC
queue is empty. A busy-spin using x86 `_mm_pause()` was measured on the
**correctly pinned** topology (matcher on CPU1).

Under that topology, busy-spin improved median `server_pre_match` by only
**~0.1–0.2 µs**:

| Idle policy | `server_pre_match` p50 |
|---|---|
| `yield()` | ~0.8–0.9 µs |
| `_mm_pause()` busy-spin | ~0.7 µs |

That is not a meaningful enough gain to justify burning a dedicated logical
CPU at 100% while idle. Whole-system e2e was noisy and often not better.
**Production keeps `std::this_thread::yield()`.** Busy-spin is not a retained
optimisation.

The earlier “`yield()` costs ~700 µs of `server_pre_match`” reading was the
§19.2 affinity trap. On an isolated CPU1, `yield()` is already sub-microsecond
at p50.

### 19.5 Housekeeping: CPU0 only vs CPU0+CPU2

With CPU1 for matching and only CPU0 for every other thread, low-load e2e
was better:

| Workload | e2e p50 (CPU0-only housekeeping) |
|---|---|
| Sequential | ~16–17 µs |
| Sustained 1k/s | ~17 µs |

Multi-client throughput suffered: gateway, client, reader, and writer
threads all competed for CPU0.

Bringing CPU2 back and allowing the non-matcher process `taskset -c 0,2`
raised loaded throughput while keeping matcher `server_pre_match` sub-µs:

| Workload | CPU0 only | CPU0+CPU2 |
|---|---|---|
| Sustained 100k/s achieved | ~82k/s | ~96k/s |
| Multi 4 achieved | ~384k/s | ~560–590k/s |
| Multi 8 achieved | ~378k/s | ~520–630k/s |
| Multi 16 achieved | ~400k/s | ~620–640k/s |

Tradeoff:

- **CPU0-only housekeeping:** best low-load e2e.
- **CPU0+CPU2 housekeeping:** better throughput and flood scaling; slightly
  higher low-load e2e (~16–17 µs → ~28 µs sequential p50).

The chosen topology is CPU0+CPU2 for non-matching work, CPU1 for matching,
CPU3 offline: the better balanced system, not the lowest sequential p50.

Multi-client floods vary run-to-run; the ranges above are representative,
not a single cherry-picked execution.

### 19.6 Other OS experiments

Tried, then discarded:

| Change | Outcome |
|---|---|
| THP always vs never | No meaningful improvement. Keep the distro default. |
| IRQ affinity | No useful gain; tails sometimes worse. Reverted. |
| Whole-process `SCHED_FIFO` | Disastrous. Starved other threads; multi-second latency. Never use whole-process real-time scheduling for this many concurrent I/O threads. |
| `sched_autogroup_enabled=0` | Did not help. Restored to enabled. |
| Larger TCP socket buffers | Not pursued: localhost tiny-message latency is not buffer-capacity limited. |
| `TCP_NODELAY` | Already enabled. |
| `net.core.busy_poll` / `busy_read` | Not enabled. This harness is localhost TCP; it does not traverse a physical NIC. |

### 19.7 Where latency remains

After a correct pin, the matcher is no longer the main latency bottleneck
on this path.

For normal single-client sustained workloads:

- `server_pre_match` p50 **< 1 µs**
- `exchange_processing` only a few microseconds

Remaining large p99 / p99.9 spikes frequently sit in `client_to_server`,
`writer_handoff`, and `server_to_client`. Future optimisation belongs on
the TCP / gateway / writer / output path, not on matching-queue handoff.

Matching-core-only ~90 M/s is still not the exchange’s TCP throughput.
Sockets and threads remain the ceiling (§18.1, restated on this host).

### 19.8 Integrity and caveats

- This is a cloud VM under KVM. Host scheduler noise affects tails. Use it
  for relative A/B, p50, and throughput — not as evidence of deterministic
  production-grade p99.9.
- Repeated `yield()` runs established the stable matcher-side result
  (~0.8 µs `server_pre_match` p50). One noisy multi-client execution is not
  a result.
- `server_pre_match` is T2 − T1 as in §3: decoded-on-server to
  matching-thread-begin.
- Matching-queue occupancy sometimes reports values near `UINT64_MAX`. That
  is an instrumentation / underflow bug, not a real queue depth. Do not
  interpret it as backlog.
- Do not mix these TCP numbers with `bench_matching_engine` GTC-rest (~169 ns
  on the laptop) or with a filled book.

### 19.9 Takeaway

The dominant optimisation from this tuning session was explicit physical-core
isolation of the matching thread. Correctly pinning the matcher to CPU1 reduced
median `server_pre_match` from hundreds of microseconds in the broken
affinity configuration to ~0.8 µs, while keeping matching-core throughput near
90 M ops/s. A second housekeeping SMT thread improved system throughput,
producing the final balanced topology of CPU0+CPU2 for non-matching work, CPU1
for matching, and CPU3 offline. Matcher idle stays `yield()`; busy-spin was
measured and not kept. The trap to remember: `taskset` without
`--matching-cpu` pins the matcher to the housekeeping set, and the resulting
hundreds of microseconds look like a `yield()` problem they are not.

---

## 20. IoPoller I/O thread (current production, 2026-08-31)

The tables in §1–§19 were measured against the 2N+2 gateway of §15.
Production I/O is now one thread on `IoPoller` (`kqueue` on this macOS
checkout, `epoll` on Linux) plus the matching thread.

```text
OrderEntryClient  (blocking send + one reader thread per client)
        │
        ▼  TCP
gateway I/O thread   IoPoller: accept, read, write; partial read/write buffers
        │            MatchingPipeline::submit() (MPSC)
        ▼
matching thread      yield() when idle; never writes a socket
        │            per-connection outbound SPSC + dirty-list wake
        ▼
same I/O thread      encode up to writer_batch, write()
```

Stamp meanings in §3 match this path: T1 and T4 are the I/O thread, not a
per-connection reader or writer. `writer_handoff` is poller wake + encode +
`write()`. `--writer-batch` still caps how many already-queued reports go
into one `write()`; it never waits to fill. `TcpSocket::read`/`write`/
`accept` return a tri-state (`Ok` / `WouldBlock` / `Error`) so EAGAIN is not
collapsed into a disconnect.

`bench_order_path_latency` now prints `2 server threads (IoPoller I/O +
matcher)` plus N client readers. `--workload idle` therefore no longer
stresses 2N gateway threads; it stresses registered fds, the poller, and
the in-process client readers. Connection scaling at a **low** rate per
session is `bench_capacity --scenario connections`, not the flood `multi`
workload.

Do not rewrite the historical IOC tables above. They remain valid for that
command shape on that architecture. Do not mix them with a re-run on
IoPoller as if they were the same experiment.

---

## Capacity program

`bench_order_path_latency` answers staged TCP latency for IOC-on-empty-book.
The questions below are a different program: mixed traffic, production queue
sizes (1024 ingest / 1024 outbound / 8192 market-data), and failure paths.
The harness is `bench_capacity`.

```bash
cmake --build build-release -j --target bench_capacity
./build-release/bench_capacity --scenario matching-thread
# Legacy A/B arm only:
./build-release/bench_capacity --scenario matching-thread --matching-accounts 1
./build-release/bench_capacity --scenario e2e-knee
./build-release/bench_capacity --scenario connections
./build-release/bench_capacity --scenario queue-drops
./build-release/bench_capacity --scenario fairness
./build-release/bench_capacity --scenario recovery
./build-release/bench_capacity --scenario soak --soak-hours 4
./build-release/bench_capacity --scenario all          # 1, 2, 4, 5, 6 — not connections or soak
./build-release/bench_capacity --quick --scenario all   # shorter streams for a smoke run
```

Three later additions sit outside `all` because each takes a median over
repeated runs and so is slower than a smoke run wants to be. Each writes a
result file carrying its own command line and machine specs:

```bash
# FOK cost split by outcome, which the 5%-FOK mixed stream averages away.
./build-release/bench_capacity --scenario fok-latency --repeats 5
# Feed drops at the offered rates e2e-knee sweeps, not at the flood rate
# queue-drops uses — different question, different answer.
./build-release/bench_capacity --scenario md-realistic --repeats 5
# Where the router's latency cost actually goes, at the one rate that
# regressed. Five arms; see below for why four of them are controls.
./build-release/bench_capacity --scenario md-cpu --repeats 5 --md-cpu-seconds 4
# Sustained one-directional drift, which walks the tick ladder's band and
# leaves the book in the overflow map. --drift-ticks 0 is the control.
./build-release/bench_capacity --scenario price-drift --repeats 5
./build-release/bench_capacity --scenario price-drift --repeats 5 --drift-ticks 0
```

| scenario | result file |
| --- | --- |
| `connections` (post-epoll) | `bench-results/capacity-connections-post-epoll.txt` |
| `fok-latency` | `bench-results/capacity-fok-latency.txt` |
| `md-realistic` | `bench-results/capacity-md-drops-realistic.txt` |
| `md-cpu` | `bench-results/md_router_cpu_attribution.txt` |
| `price-drift` | `bench-results/capacity-price-drift.txt` |

Shared mix: 40% rest / 25% cross / 20% cancel / 10% replace / 5% IOC-FOK,
1000 orders/side seed, 64-tick band (`WorkloadMix::realistic()`).
The matching-thread headline uses 128 uniformly selected, fully funded
accounts. Its validity gate is `risk_rejected_events=0`; the separately
reported `InsufficientLiquidity` count is the workload's intentional FOK
book-depth outcome, not an account-funding failure. The generated stream is
submitted in vector order because its cancels and replaces depend on earlier
operations. MPSC producer contention is measured by the queue/fairness
scenarios rather than by invalidating this stream through striped admission.

The same scenario also prints `MultiLevelSweep/{1,4,16,64,256}` through both
bare `MatchingEngine::process()` and
`CommandSequencer` → `RiskGatedEngine`, including ns/op and the full/bare
ratio. This exposes whether per-event ledger work grows with fill fan-out.

| Scenario | Question | Headline |
|---|---|---|
| `matching-thread` | How many commands/s can the **production matching thread** do with zero sockets? | 128 funded accounts; `CommandSequencer` → `RiskGatedEngine` (risk + ledger) → `MatchingEngine::process`; zero risk rejects required. Also prints five full-path sweep-depth ratios. |
| `e2e-knee` | At what offered TCP rate does achieved stop tracking and p99.9 blow up? | Geometric sweep from 5k/s; last tracking rate, first failure, achieved plateau. Mixed GTC, not a 100k token target. |
| `connections` | Does 2 server threads stay healthy as N grows with **low** rate per connection? | 1, 10, 100, 1k, 2k, 5k, 10k. Spawn/connect failure is a result, not a harness bug. This process also has N client readers. |
| `queue-drops` | When does each bounded queue actually drop, and what does a client see? | Ingest MPSC: `submit()` false; gateway currently **silent** (no Rejected). Outbound SPSC: engine committed, missing reports. MD `DroppingQueue`: detectable sequence gap. If TCP never fills ingest, that is reported; in-process flood is the matching-thread backpressure point. |
| `fairness` | Does a polite client's p99 stay near isolation when others flood? | One 1k/s client vs N flooders; polite-only percentiles. |
| `recovery` | How long is a subscriber blind after a gap? | `read_snapshot` + book rebuild vs depth. Lost sequences are **not** replayed (no retransmission). Second clock: UDP listen drain during delayed recovery. |
| `soak` | Do RSS / resting orders / ledger maps / queue HWMs stay flat for hours? | Default 4h below the e2e knee. Latency tracing is off so the tracer ring cannot look like a leak. Sample interval 30s (`--quick` is 2s). |
| `md-cpu` | `md-realistic` shows p50 collapsing when the router is attached. Which part of attaching it is responsible? | One offered rate held long enough to sample, five arms: **A** no router, **B** router + UDP subscriber, **C** B with `publish()` timed per step, **D** router with a discarding sink, **E** D instrumented. Reports the order path split at the tracer's stamps, per-thread CPU, and the `try_push` cost distribution. |

#### Why `md-cpu` has five arms (2026-09-04)

Attaching the router changes three things at once — work on the matching
thread, an extra thread, and a loopback UDP datagram per event — so the B-vs-A
comparison that `md-realistic` makes cannot say which one costs anything.

- **D** keeps the translation, the `DroppingQueue` push, the `notify_one` and
  the routing thread, and drops only the datagram. `D/A = 1.0x`, so none of
  the router's own machinery is responsible.
- **E** is D instrumented, and exists as the control for **C**: C and E push
  into the same queue from the same thread with the same code, differing only
  in whether the consumer is hammering the loopback stack or nearly idle.
  Since the producer reads the consumer-owned `tail_` on every push, that is
  the variable that decides whether a slow push is contention or preemption.
- **C** and **E** cost the matching thread two tick reads per step, so they
  are not the reproduction. Quote **B**.

`--md-cpu-arms` narrows the run to chosen arms, which is what makes an
external per-thread capture (`ps -M`, `sample`, Instruments) possible: the
sweep runs its arms back to back in one process, so a capture aimed at the
process would otherwise span all of them. The long-lived threads name
themselves (`mdh-matching`, `mdh-gateway-io`, `mdh-md-router`, …) via
`mdh::set_calling_thread_name`, so those tools can attribute CPU by name
instead of by reading stacks.

Result: the regression is the unbatched loopback UDP hop, not the router's
queue. `MarketDataRouterOptions::measure_publish_cost` is the opt-in counter
set behind arms C and E; it is off by default because its tick reads land on
the matching thread.

### Corrected matching-thread methodology (2026-09-02)

Release build on the Apple M3 Pro, five alternating A/B process pairs,
1,000,000 measured commands each:

- Legacy before: one account, legacy funding.
- Corrected after: 128 uniformly selected accounts, each funded with
  \(10^{18}\) cash ticks and \(10^{12}\) units.
- Both arms replay the identical deterministic mixed stream in vector order.
- The corrected funding gate passed in all five runs:
  `risk_rejected_events=0`.
- Each run also emitted 15,772 `InsufficientLiquidity` rejections. Those are
  intentional FOK book-depth outcomes. They are not risk/funding failures,
  which is why the benchmark now prints rejection reasons separately.

| Method | Commands/s runs | Median | Delta |
|---|---|---:|---:|
| Before: 1 account | 3,895,821; 3,043,372; 3,957,524; 3,940,830; 3,932,743 | 3,932,743/s | — |
| After: 128 funded accounts | 3,740,197; 3,748,659; 3,758,637; 3,748,385; 3,538,405 | **3,748,385/s** | **−4.69%** |

The one-account setup overstated full matching-thread capacity by about
4.9% relative to the corrected median. The earlier undifferentiated
`order_rejected_events` total was not a valid funding diagnostic: it mixed
intentional FOK liquidity outcomes with risk failures. The new
`risk_rejected_events` field is the funding gate.

Five-run medians for the sweep-depth comparison:

| Levels swept | Matching-only ns/op | Full-path ns/op | Full / matching |
|---:|---:|---:|---:|
| 1 | 118.6 | 202.0 | **1.70×** |
| 4 | 793.0 | 1,177.5 | **1.49×** |
| 16 | 3,089.8 | 4,481.0 | **1.45×** |
| 64 | 12,206.8 | 17,697.6 | **1.45×** |
| 256 | 47,739.2 | 70,324.5 | **1.47×** |

The ratio does not grow with fill fan-out. The absolute risk/ledger cost
does grow because `Ledger::apply()` runs per event, but matching work grows
at least proportionally; the full/bare ratio settles near 1.45–1.49× from
4 through 256 levels.

Do not rewrite the historical IOC tables in the sections above. They remain
valid for that command shape. Do not mix them with the matching-thread mixed
headline or with a mixed-GTC TCP knee.

