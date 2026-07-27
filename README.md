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

## Current Milestone: 2 of 6

Milestone 1 was a complete, self-contained vertical slice: binary message
definitions, an encoder/decoder, a binary event-file writer, deterministic
file replay, sequence-number validation, and limit-order-book
reconstruction.

Milestone 2 adds UDP as a second transport, on top of (not instead of)
file replay: a `udp_sender` app, packet framing/batching, network byte
order, and packet-level duplicate/out-of-order tracking. `market_data_replay`
now has two mutually exclusive modes — `--input <file>` and `--listen
<port>` — that both funnel through the exact same decode/sequence-validate
/book-apply logic.

Not yet implemented (by design, deferred to later milestones): a
queue-based ingestion pipeline, sequence-gap *recovery* (only *detection*
exists so far), snapshotting, throughput/latency benchmarking, and fault
injection.

---

## Architecture

```
 events.bin ──► EventFileReader ──┐
                                   │
 UDP packets ──► UdpReceiver ──► unpack_frames() ──┘
                 (batched recv,     (packet framing;
                  local recv         PacketSequenceTracker
                  timestamps)        observes packet_sequence,
                                     purely as a stat)
                                   │
                                   ▼ std::variant<Event, DecodeError>
                          decode_event() -- validates type/size/
                          truncation/side; never reinterpret_casts
                                   │
                                   ▼
                    replay::apply_frame_result()
                    -- SequenceValidator classifies InOrder /
                       Duplicate / OutOfOrder / Missing on the
                       EVENT's own sequence_number (the actual
                       correctness gate, regardless of transport)
                                   │
                                   ▼
                          BookManager -- one OrderBook per
                          InstrumentId, created lazily; trade
                          stats tracked separately
                                   │
                                   ▼
                    ReplayStats / console summary -- measured
                    counts + wall-clock duration
```

`replay::apply_frame_result()` is the single piece of logic both transports
share: `run_replay()` (file) and `net::run_udp_listen()` (UDP) each have
their own frame-sourcing loop, but both call the same function to decide
what a decoded frame means for the book and the stats. Only "where do
frames come from" differs between them.

### Directory layout

```
include/          public headers, mirrors src/
  common/          shared primitive types, endian-safe byte I/O, SequenceValidator
  protocol/        wire format: messages, encoder, decoder, errors
  replay/          file writer/reader, replay engine (transport-agnostic)
  book/            price level, order book, book manager
  net/             UDP socket, packet framing, batched receiver, packet-
                   level sequence tracking, the UDP↔replay bridge
src/               implementations for the above
apps/
  feed_generator/      generates a deterministic binary event file
  udp_sender/          streams an event file over UDP, batched into packets
  market_data_replay/  replays a file OR listens live on a UDP port
tests/             GoogleTest suite (70 tests as of milestone 2)
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
```

Compiled with `-Wall -Wextra -Wpedantic -Wshadow -Wconversion`; the build
is warning-clean.

## Testing

```bash
ctest --test-dir build --output-on-failure
# or directly:
./build/mdh_tests
```

70 tests across protocol round-trip/error-handling, sequence validation,
event-file I/O, order-book behavior, end-to-end file replay (including a
determinism check), UDP sockets (a genuine loopback round-trip, not
mocked), packet framing (including "one bad frame doesn't invalidate an
otherwise-valid packet"), packet-level sequence tracking, batched UDP
receive, and end-to-end UDP replay (send over a real loopback socket,
assert reconstructed book state — including a corrupt-packet-mixed-with-
good-packets case and a cross-packet sequence-gap case).

## Example Commands

```bash
# Generate a deterministic 100k-order feed
./build/feed_generator --output events.bin --orders 100000 --seed 42

# Replay it from a file and print statistics + top-5 price levels per instrument
./build/market_data_replay --input events.bin --top-levels 5

# ...or stream it over UDP and have market_data_replay listen live instead:
./build/market_data_replay --listen 9000 --top-levels 5 &
./build/udp_sender --input events.bin --host 127.0.0.1 --port 9000
```

Sample `market_data_replay --listen` output (truncated), from an actual
run streaming 5,000 orders' worth of events (7,141 total, batched into 358
packets) over loopback:

```
=== Replay Summary ===
listened on port:    9123
packets received:    358
packet errors:       0
packet sequencing:   in_order=358 duplicate=0 out_of_order=0 gaps=0
messages processed:  7141
decode failures:     0
sequence failures:   0
book errors:         0
adds:                5000
cancels:             1086
modifies:            676
trades:              307
clears:              72
replay duration:     1376.04 ms
messages/sec:        5189.5

instruments seen:    10

-- instrument 1 --
    bid 100826 x 49 (1 orders)
    bid 100803 x 19 (1 orders)
    bid 100800 x 43 (1 orders)
    ask 100689 x 41 (1 orders)
    ask 100721 x 33 (1 orders)
    ask 100756 x 7 (1 orders)
    trades: 29, volume: 1873, last price: 100764
```

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
- No SPSC queue yet — the UDP receive loop and book reconstruction run
  synchronously in one thread (milestone 3 territory).

## Planned Milestones

3. SPSC queue between ingestion and book reconstruction, backpressure
   policy, queue occupancy metrics, slow-consumer simulation.
4. Sequence-gap *recovery*, snapshot generation/loading, buffered
   incremental messages, resume after recovery.
5. Allocation profiling, decode throughput benchmarks, end-to-end latency
   (p50/p99/p99.9), burst-load tests, CPU affinity where supported,
   comparison of alternative book representations.
6. Fault injection: corrupt messages, truncated packets, duplication,
   packet loss, delayed consumer, recovery validation.
