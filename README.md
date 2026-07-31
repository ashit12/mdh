# mdh — Market Data Handler & Limit Order Book Reconstruction

A C++20 application that consumes a custom binary market-data protocol
over a file or live UDP, validates message sequencing, reconstructs an
in-memory limit order book, supports deterministic replay, and reports
measured correctness and performance statistics.

**This is a simulated, ITCH/OUCH-inspired protocol built as a systems
programming portfolio project, not a production exchange implementation.**
It has no matching engine and no persistence yet — those are explicitly
out of scope for now (see *Planned Milestones*).

---

## Current Milestone: 3 of 6

Milestone 1 was a complete, self-contained vertical slice: binary message
definitions, an encoder/decoder, a binary event-file writer, deterministic
file replay, sequence-number validation, and limit-order-book
reconstruction.

Milestone 2 added UDP as a second transport, on top of (not instead of)
file replay: a `udp_sender` app, packet framing/batching, network byte
order, and packet-level duplicate/out-of-order tracking. `market_data_replay`
gained two mutually exclusive modes — `--input <file>` and `--listen
<port>` — that both funnel through the exact same decode/sequence-validate
/book-apply logic.

Milestone 3 decouples UDP ingestion from book reconstruction with a
lock-free SPSC queue: `--listen` now runs a producer thread (receive +
decode) and a consumer thread (validate + apply) connected by a bounded
queue, with an explicit drop-newest backpressure policy, occupancy metrics
(current size, high-water mark, dropped count), and a `--consumer-delay-us`
flag to deterministically simulate a slow consumer and observe backpressure
kick in on demand, rather than relying on it happening to occur under load.

Not yet implemented (by design, deferred to later milestones): sequence-gap
*recovery* (only *detection* exists so far), snapshotting, throughput/latency
benchmarking, and fault injection.

---

## Architecture

```
 events.bin ──► EventFileReader ──────────────────────────────────────┐
   (run_replay(): single-threaded, no queue)                          │
                                                                       ▼
 ── net::run_udp_listen(): two threads, connected by a DroppingQueue ──
                                                                       │
 UDP packets ──► UdpReceiver ──► unpack_frames() ──► DroppingQueue.push()
   [PRODUCER    (batched recv,     (packet framing;      (drop-newest on
    thread]      local recv         PacketSequenceTracker  full; counted,
                 timestamps)        observes packet_seq,   not fatal --
                                    purely as a stat)       see below)
                                                                       │
                                                    DroppingQueue.try_pop()
                                                                       │
                                                                       ▼ std::variant<Event, DecodeError>
                                              decode_event() already ran on
   [CONSUMER                                 the producer side -- this is
    thread]                                  just handing the decoded result
                                              (or a DecodeError) onward
                                                                       │
                                                                       ▼
                                                replay::apply_frame_result()
                                                -- SequenceValidator classifies
                                                   InOrder / Duplicate /
                                                   OutOfOrder / Missing on the
                                                   EVENT's own sequence_number
                                                   (the actual correctness
                                                   gate, regardless of
                                                   transport OR whether a
                                                   queue sits in front of it)
                                                                       │
                                                                       ▼
                                                BookManager -- one OrderBook
                                                per InstrumentId, created
                                                lazily; trade stats tracked
                                                separately
                                                                       │
                                                                       ▼
                                                ReplayStats / console summary
                                                -- measured counts + wall-
                                                   clock duration + (UDP only)
                                                   queue high-water mark /
                                                   dropped count
```

`replay::apply_frame_result()` is the single piece of logic all three paths
share: `run_replay()` (file, single-threaded) and the consumer half of
`net::run_udp_listen()` (UDP, two threads) each have their own
frame-sourcing loop, but both call the same function to decide what a
decoded frame means for the book and the stats. A dropped queue item is,
from `apply_frame_result()`'s point of view, indistinguishable from a
dropped UDP packet -- both just manifest as a gap in the sequence numbers
it sees.

### Directory layout

```
include/          public headers, mirrors src/
  common/          shared primitive types, endian-safe byte I/O,
                   SequenceValidator, SpscQueue (lock-free ring buffer),
                   DroppingQueue (adds the drop-on-full backpressure policy)
  protocol/        wire format: messages, encoder, decoder, errors
  replay/          file writer/reader, replay engine (transport-agnostic)
  book/            price level, order book, book manager
  net/             UDP socket, packet framing, batched receiver, packet-
                   level sequence tracking, the UDP↔replay bridge (now a
                   two-thread producer/consumer pipeline, see Architecture)
src/               implementations for the above
apps/
  feed_generator/      generates a deterministic binary event file
  udp_sender/          streams an event file over UDP, batched into packets
  market_data_replay/  replays a file OR listens live on a UDP port
tests/             GoogleTest suite (90 tests as of milestone 3)
docs/
  protocol.md      detailed wire-format spec (byte offsets, error taxonomy,
                   packet header layout)
```

---

## Binary Protocol (summary — full spec in [docs/protocol.md](docs/protocol.md))

- **Byte order:** big-endian ("network byte order"), encoded/decoded via
  explicit bit shifts (never `memcpy`/`reinterpret_cast` onto a struct) —
  safe regardless of host endianness or alignment. One wire format for
  both file replay and UDP; a replay file is a raw capture of the same
  bytes a UDP receiver would see.
- **Event frame:** a fixed 20-byte header (`type`, `reserved`,
  `payload_size`, `sequence_number`, `timestamp_ns`) followed by a
  type-specific fixed-size payload.
- **Packet frame (UDP only):** a separate 20-byte header (`magic`,
  `version`, `frame_count`, `packet_sequence`, `payload_length`) wrapping
  one or more event frames batched into a single datagram. Packet framing
  is a transport-level concern, kept deliberately separate from the
  event-frame format — `decode_event()`/`BookManager` never need to know
  whether a frame arrived via file or was unpacked from a UDP packet.
- **Message types:** `AddOrder`, `CancelOrder`, `ModifyOrder`, `Trade`,
  `ClearBook`.
- **Price:** `int64_t` scaled-integer ticks (1 tick = 0.0001 currency
  unit), never floating point.
- **Decoder:** returns `std::variant<T, DecodeError>` — structured errors
  instead of exceptions or process termination, at both the event-frame
  level (`TruncatedHeader`, `TruncatedPayload`, `InvalidReserved`,
  `InvalidMessageType`, `InvalidMessageSize`, `InvalidSide`) and the
  packet level (`PacketError`: bad magic/version, truncated packet header,
  payload-length mismatch, unreadable inner frame). A packet-level error
  invalidates the whole datagram; an event-level `DecodeError` for one
  frame inside an otherwise-valid packet does not.

---

## Order-Book Model

Each instrument gets its own `OrderBook`:

- **Bids:** `std::map<Price, PriceLevel, std::greater<Price>>` — highest
  price first.
- **Asks:** `std::map<Price, PriceLevel>` — lowest price first.
- **Within a level:** `std::list<RestingOrder>` for FIFO order, plus a
  running aggregate quantity.
- **Cancel/modify routing:** `unordered_map<OrderId, OrderLocation>` maps
  an order id directly to its side, price, and list iterator.

**Cost of cancel/modify:** O(1) hash lookup to find the side and price,
then an **O(log P)** `std::map` lookup to reach the price level (P =
distinct price levels on that side), then O(1) list erase. This is *not*
O(1) overall — the map lookup dominates. A flat array indexed by
`price - base_price` would make level lookup O(1) at the cost of wasted
space for sparse price ranges and a linear best-price scan when the top
level empties. `std::map` was chosen because it's simpler to reason about
and the book depths in this milestone's synthetic feeds are small enough
that the log(P) factor is not the bottleneck.

**Modify semantics:** implemented as cancel + re-add at the back of the
(possibly new) price level — it always loses time priority. A real
exchange typically preserves priority for a quantity-decrease-only modify;
this milestone doesn't distinguish that case, to keep the implementation
and its tests simple.

**Trade messages:** informational only. The wire format has no `order_id`
on a `Trade`, so there is nothing to link it back to a specific resting
order — trades update per-instrument trade statistics (count, volume, last
price) but do not mutate book depth. `AddOrder`/`CancelOrder`/`ModifyOrder`
are the only book mutators.

**Validation:** `add_order` rejects non-positive price/quantity and
duplicate order ids; `cancel_order`/`modify_order` reject unknown order
ids. All return `std::optional<BookError>` (`nullopt` = success).

**No matching engine:** since nothing here matches a crossing bid against
a resting ask, a reconstructed book *can* show a crossed spread (best bid
above best ask) if the input feed's independently-generated orders happen
to cross — this is expected given the current scope, not a reconstruction
bug.

---

## Building

Requirements: CMake ≥ 3.20, a C++20 compiler (tested with Apple Clang on
arm64). GoogleTest is fetched automatically via `FetchContent`. UDP
networking uses the POSIX BSD sockets API, portable to Linux and macOS.

```bash
# Standard debug build
cmake -S . -B build
cmake --build build -j

# AddressSanitizer + UndefinedBehaviorSanitizer build
cmake -S . -B build-asan -DMDH_ENABLE_ASAN=ON -DMDH_ENABLE_UBSAN=ON
cmake --build build-asan -j

# ThreadSanitizer build (separate binary -- can't combine with ASan/UBSan)
cmake -S . -B build-tsan -DMDH_ENABLE_TSAN=ON
cmake --build build-tsan -j
```

Compiled with `-Wall -Wextra -Wpedantic -Wshadow -Wconversion`; the build
is warning-clean.

## Testing

```bash
ctest --test-dir build --output-on-failure
# or directly:
./build/mdh_tests
```

90 tests across protocol round-trip/error-handling, sequence validation,
event-file I/O, order-book behavior, end-to-end file replay (including a
determinism check), UDP sockets (a genuine loopback round-trip, not
mocked), packet framing (including "one bad frame doesn't invalidate an
otherwise-valid packet"), packet-level sequence tracking, batched UDP
receive, end-to-end UDP replay (send over a real loopback socket, assert
reconstructed book state — including a corrupt-packet-mixed-with-good-
packets case and a cross-packet sequence-gap case), `SpscQueue` (including
a genuine multi-threaded producer/consumer stress test, checked clean
under ThreadSanitizer), `DroppingQueue`'s drop-and-count policy, and the
full two-thread UDP pipeline including a deterministic backpressure test
(tiny queue + artificially slowed consumer forces drops on demand, no
timing luck required).

A deterministic backpressure test replaced an earlier one that turned out
to be flaky (~1-in-15 runs): `SequenceValidator` only detects a gap
*retrospectively*, when a later sequence number actually arrives, so a
drop landing on the tail end of a burst with nothing surviving afterward
is genuinely undetectable, not a bug. That limitation is now pinned down
as two explicit tests
(`test_backpressure_integration.cpp`) rather than an intermittent failure.

## Example Commands

```bash
# Generate a deterministic 100k-order feed
./build/feed_generator --output events.bin --orders 100000 --seed 42

# Replay it from a file and print statistics + top-5 price levels per instrument
./build/market_data_replay --input events.bin --top-levels 5

# ...or stream it over UDP and have market_data_replay listen live instead:
./build/market_data_replay --listen 9000 --top-levels 5 &
./build/udp_sender --input events.bin --host 127.0.0.1 --port 9000

# Force and observe backpressure on demand: a tiny queue + an artificially
# slowed consumer (5ms/event) guarantees drops, rather than hoping a real
# workload happens to be slow enough to trigger it:
./build/market_data_replay --listen 9001 --queue-capacity 8 --consumer-delay-us 5000 &
./build/udp_sender --input events.bin --host 127.0.0.1 --port 9001
```

Sample `market_data_replay --listen` output (truncated), from an actual
run streaming 5,000 orders' worth of events (7,082 total, batched into 355
packets) over loopback, default queue settings:

```
=== Replay Summary ===
listened on port:    9123
packets received:    355
packet errors:       0
packet sequencing:   in_order=355 duplicate=0 out_of_order=0 gaps=0
queue dropped:       0
queue high water:    51
messages processed:  7082
decode failures:     0
sequence failures:   0
book errors:         0
adds:                5000
cancels:             1061
modifies:            664
trades:              293
clears:              64
replay duration:     1353.06 ms
messages/sec:        5234.1

instruments seen:    10

-- instrument 1 --
    bid 101348 x 17 (1 orders)
    ask 101096 x 64 (1 orders)
    trades: 24, volume: 1097, last price: 101308
```

Even with no artificial slowdown and a generous default capacity (1024),
the queue's high-water mark of 51 shows the consumer (book reconstruction)
genuinely does fall behind the producer (raw receive+decode) at times --
just not by enough to ever hit the cap and drop anything here.

Now the same feed against a deliberately tiny, slow configuration
(`--queue-capacity 8 --consumer-delay-us 5000`), forcing backpressure on
demand instead of hoping for it:

```
=== Replay Summary ===
listened on port:    9789
packets received:    36
packet errors:       0
packet sequencing:   in_order=36 duplicate=0 out_of_order=0 gaps=0
queue dropped:       707
queue high water:    8
messages processed:  9
decode failures:     0
sequence failures:   1
book errors:         0
adds:                7
cancels:             0
modifies:            2
trades:              0
clears:              0
replay duration:     593.339 ms
messages/sec:        15.2

replay stopped early: missing sequence(s) [10..360]
```

707 of 717 events were dropped (the queue never grows past its 8-slot
cap), the high-water mark is pinned exactly at that cap, and the resulting
gap (sequences 10 through 360 never arrived) was caught by the same
`SequenceValidator` that gates file replay -- a dropped queue item and a
dropped UDP packet look identical from its point of view.

All numbers above are from actual runs on the development machine, not
invented figures — they will vary by hardware and are not a benchmark
claim (see *Milestone 5* below for planned formal benchmarking).

---

## Design Decisions

**Milestone 1:**
- **Hand-rolled wire format, not struct serialization.** Struct layout
  depends on compiler padding/alignment/endianness, none of which are part
  of a wire contract. Every field is written/read via explicit bit shifts.
- **`std::variant<T, DecodeError>` for decode results**, not exceptions.
  Malformed input is an expected condition on this path, not an
  exceptional one; `std::expected` was ruled out only because it's
  C++23-only and this project targets C++20.
- **`std::optional<BookError>` for book mutations** (`nullopt` = success)
  — simpler than a variant when there's no success value to carry.
- **Sequence validation is feed-wide**, matching how a single physical feed
  channel typically sequences multiple instruments together.
- **Reused buffers on the decode/read/write paths** (`EventFileReader`,
  `EventFileWriter`) to avoid a heap allocation per message.

**Milestone 2:**
- **Switched to big-endian, one format for file and UDP** (not a separate
  LE file format and BE wire format). The migration was confined entirely
  to `common/byte_io.hpp` — `encoder.cpp`/`decoder.cpp` only ever called
  `io::put_u16()`/`io::get_u16()` etc. and never depended on which byte
  order those used internally, so nothing else needed to change. All of
  milestone 1's tests passed unmodified after the switch.
- **`SequenceValidator` moved to `common/`**, since it's now used two
  places — event-level sequencing (the correctness gate, in
  `replay::apply_frame_result`) and packet-level sequencing (purely
  observational, in `net::PacketSequenceTracker`) — and having either
  `net/` or `replay/` depend on the other for a generic classifier would
  have been a backwards layering dependency.
- **Packet-level duplicate/out-of-order detection is observational, not a
  correctness gate.** UDP packets can legitimately arrive out of order or
  duplicated for reasons unrelated to data correctness (different network
  paths, retransmits). The existing event-level `SequenceValidator`
  (unchanged) remains the actual gate.
- **`net::run_udp_listen()` lives in `net/`, depending on `replay/`** (not
  the other way around) — `replay_engine.hpp` itself has zero knowledge of
  UDP or sockets; `net/udp_listener.*` is the one deliberate place that
  bridges the two, so that dependency exists in exactly one file.
- **Extended `market_data_replay` with `--listen` rather than a new
  `udp_receiver` binary.** The decode→validate→apply→report pipeline is
  identical regardless of source, so the app is the natural place for that
  shared reporting logic to live once; `udp_sender` is a genuinely new
  capability (streaming a file), which is why it's a new binary.
- **`udp_sender` re-encodes events rather than replaying raw file bytes.**
  It reads decoded `Event`s from `EventFileReader` and re-encodes them via
  `net::pack_frames()` (which calls `protocol::encode_event()`
  internally), rather than adding a separate "raw frame bytes" reading
  path just to avoid one redundant encode step. Simpler, and it
  continuously exercises decode→encode round-trip correctness as a side
  effect.
- **`--listen` stops via an idle timeout, not a signal handler.** After
  receiving at least one packet, if `--idle-timeout-ms` elapses with no
  further traffic, the listener assumes the sender finished and stops.
  There is no `SIGINT`/graceful-shutdown story for a true "run until told
  to stop" server — out of scope for this milestone.

**Milestone 3:**
- **Lock-free `SpscQueue<T>`, no CAS loop.** The producer only ever writes
  `head_`, the consumer only ever writes `tail_` — since neither atomic
  ever has concurrent writers, a plain atomic load (acquire)/store
  (release) pair per operation is sufficient. A CAS loop is what a true
  MPMC queue needs (multiple threads racing to write the *same* atomic);
  SPSC rules that out by contract, so it would just be unused generality.
- **Raw storage + `std::construct_at`/`std::destroy_at`, not
  `std::vector<T>`.** Avoids requiring `T` to be default-constructible
  just so a slot can represent "nothing here yet" — a slot only comes
  into existence when an element is actually constructed into it.
- **`SpscQueue` itself stays policy-free; `DroppingQueue<T>` wraps it to
  add the drop-newest backpressure policy** (plus a counter for how often
  it kicked in). Mirrors how `PacketSequenceTracker` wraps
  `SequenceValidator` — a classifier/mechanism stays reusable for any
  policy, and turning its result into a decision lives one layer up.
- **`std::jthread` + a single shared `std::stop_source`, not
  `std::thread` + a hand-rolled atomic bool.** `jthread`'s destructor
  joins automatically (no `std::terminate` risk from a forgotten `.join()`
  after an early return), and `stop_source`/`stop_token` is the standard
  mechanism for telling *both* threads to stop — used instead of each
  `jthread`'s own private per-object token, since this needs one signal
  both sides observe.
- **Only decoded frames cross the queue; packet-level bookkeeping
  (`PacketSequenceTracker`, `packet_errors`) stays entirely on the
  producer thread.** It's tied to receiving a datagram, not to anything
  the consumer does, so there's no reason for it to cross a thread
  boundary at all.
- **A flaky test taught a real lesson about the design, not just about
  the test.** An initial backpressure test asserted a dropped frame
  always shows up as a detected sequence gap; it failed ~1-in-15 runs.
  `SequenceValidator` only detects a gap *retrospectively* (a later
  sequence number has to actually arrive), so a drop with nothing
  surviving after it is genuinely invisible downstream — not a bug, an
  inherent limit of retrospective detection. Fixed by asserting only what
  a real end-to-end run actually guarantees (drops happen; the
  drop/processed accounting balances exactly), and adding a fully
  deterministic test (no sockets, no threads, no timing) that drives the
  same real components directly to prove both sides of that limitation
  as explicit, tested facts.

## Current Limitations

- `SequenceValidator` only remembers the last accepted sequence number, so
  it cannot distinguish "duplicate of an old sequence" from generic
  out-of-order arrival once the feed has moved past it — both are reported
  as `OutOfOrder`.
- No sequence-gap *recovery* — replay just stops (milestone 4 territory).
- `Trade` doesn't mutate book depth (no `order_id` to link it to a resting
  order — see *Order-Book Model* above).
- `Modify` always loses time priority, even for a quantity-only decrease.
- No matching engine — a reconstructed book can show a crossed spread; see
  *Order-Book Model* above.
- `feed_generator` only emits valid, well-formed feeds; it does not yet
  have a flag to inject corruption (planned for milestone 6).
- **UDP is unicast only** (single sender, single receiver) — no multicast
  fan-out, which is how real exchange feeds actually distribute data.
- **`udp_sender` has no pacing/rate limiting** — sends as fast as it can,
  with no deliberate loss/reorder/corruption injection (milestone 6 scope).
- **No DNS resolution** — `--host` must be an IPv4 dotted-decimal literal
  (e.g. `127.0.0.1`), not a hostname. IPv6 is out of scope.
- **`recvmmsg()` batching (Linux-only, multiple datagrams in one syscall)
  is not implemented**, only designed for: this dev/test machine is macOS,
  where `recvmmsg()` doesn't exist, and shipping an unverifiable
  syscall-level fast path seemed worse than not having it. `UdpReceiver`
  instead batches via a `recvfrom()` loop over a non-blocking socket,
  which is portable and is what's actually tested.
- **`SpscQueue`'s capacity is fixed at construction** — no dynamic
  resizing if a workload's needs change at runtime. Given how cheap it is
  to just start with a larger capacity, this hasn't been a real
  constraint, but it's a real one if memory footprint mattered more than
  it does for a portfolio project.
- **A drop is still just a drop** — there's no way to recover, replay, or
  even know *which* sequence numbers were lost beyond the gap range the
  next successful frame reveals (see the *drop-with-no-later-survivor* case
  above). Sequence-gap *recovery* is milestone 4 territory, same as the
  gaps caused by a genuinely dropped UDP packet.

## Planned Milestones

4. Sequence-gap *recovery*, snapshot generation/loading, buffered
   incremental messages, resume after recovery.
5. Allocation profiling, decode throughput benchmarks, end-to-end latency
   (p50/p99/p99.9), burst-load tests, CPU affinity where supported,
   comparison of alternative book representations.
6. Fault injection: corrupt messages, truncated packets, duplication,
   packet loss, delayed consumer, recovery validation.
