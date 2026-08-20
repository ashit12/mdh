# Wire Protocol Specification

This is a simulated, ITCH/OUCH-inspired binary market-data protocol built
for a portfolio project. It is not a real exchange spec, and no claim is
made that it matches any production feed byte-for-byte.

## Byte order

**Big-endian ("network byte order")**, throughout -- for both the file
replay format and UDP transport. There is exactly one wire format: a
replay file is a raw capture of the same bytes a UDP receiver would see,
not a separate on-disk encoding.

Network byte order is the right default the moment bytes cross a wire --
it's the same reason `htons`/`htonl` exist in the sockets API: intermediate
routers and receivers on different architectures can't assume anything about
the sender's endianness. The choice is confined entirely to
`include/common/byte_io.hpp`; `encoder.cpp`/`decoder.cpp` only ever call
`io::put_u16()`/`io::get_u16()` and never depend on which byte order those
functions use internally, so the endianness of the whole protocol can be
changed in one file without touching anything else.

Every multi-byte field is encoded/decoded via explicit bit shifts (see
`include/common/byte_io.hpp`), never via `memcpy`-ing a struct onto the
wire. That sidesteps three separate hazards at once: struct padding
(compiler-dependent, not part of the wire contract), alignment (a `uint64_t`
read via `reinterpret_cast` on an odd byte offset is undefined behaviour),
and host endianness (a `memcpy` approach silently breaks on a big-endian
host). The byte-shift approach is correct on any host, at the cost of being
one field-width switch statement instead of a single `memcpy` call.

## Frame layout

A frame is `header || payload`, with no separator and no outer file
header/footer -- an event file is simply frames concatenated back to back.

### Header (20 bytes, always present)

| Offset | Field | Type | Notes |
|---|---|---|---|
| 0 | `type` | `u8` | one of `MessageType` below |
| 1 | `reserved` | `u8` | must be `0`; reserved for future flags |
| 2 | `payload_size` | `u16` | byte length of the payload that follows |
| 4 | `sequence_number` | `u64` | feed-wide, monotonically increasing |
| 12 | `timestamp_ns` | `u64` | nanoseconds, arbitrary epoch |

`payload_size` is redundant with `type` today (every type has a fixed
payload size) -- it exists as an independent corruption cross-check:
the decoder rejects a frame whose declared `payload_size` doesn't match the
fixed size for its `type`, which catches a class of bit-flip corruption
that a `type`-only check would miss. It also means adding a variable-length
message type later doesn't require a header format change.

`sequence_number` and `timestamp_ns` live in the header once, not in each
payload, even though the spec describes them as fields of every message
type. Decoded in-memory message structs (`AddOrder`, `CancelOrder`, ...)
still expose both fields, copied from the header at decode time -- so a
decoded message is self-contained, without duplicating 16 bytes across five
different payload encodings on the wire.

### Payloads

**AddOrder** (29 bytes): `order_id:u64, instrument_id:u32, price:i64, quantity:u64, side:u8`

**CancelOrder** (12 bytes): `order_id:u64, instrument_id:u32`

**ModifyOrder** (28 bytes): `order_id:u64, instrument_id:u32, new_price:i64, new_quantity:u64`

**Trade** (21 bytes): `instrument_id:u32, price:i64, quantity:u64, aggressor_side:u8`

**ClearBook** (4 bytes): `instrument_id:u32`

`side` / `aggressor_side` are one byte: `0 = Buy`, `1 = Sell`. Any other
value is rejected by the decoder as `DecodeError::InvalidSide`.

## Price scale

`Price` is `int64_t`, scaled integer ticks -- never floating point. The
scale is fixed at **1 tick = 0.0001 currency unit** (4 implied decimal
places). That scale is a convention, not something carried on the
wire; the encoder and every consumer must agree on it out of band, the same
way real fixed-point feeds do. Floating point was ruled out because
`0.1 + 0.2 != 0.3` in IEEE 754 -- unreliable for price-level equality and
routing in a book.

`Quantity` is `uint64_t`. `OrderId` is `uint64_t`. `InstrumentId` is
`uint32_t`.

## Decode error taxonomy

| Error | Meaning |
|---|---|
| `TruncatedHeader` | fewer than 20 bytes available where a header was expected |
| `TruncatedPayload` | header decoded, but fewer than `payload_size` bytes followed |
| `InvalidReserved` | header's reserved byte was non-zero |
| `InvalidMessageType` | `type` byte didn't match any `MessageType` enumerator |
| `InvalidMessageSize` | `payload_size` didn't match the fixed size for `type` |
| `InvalidSide` | a side byte was neither `0` nor `1` |

Decode errors are returned as `std::variant<T, DecodeError>` rather than
thrown as exceptions or terminating the process -- decoding untrusted bytes
is exactly the situation where a caller needs to inspect *what* went wrong
and decide what to do next (for replay: stop), not unwind a
call stack. `std::expected<T, E>` was the more obviously-named alternative
but is a C++23 feature; this project targets C++20.

## Sequence validation

Sequencing is validated feed-wide (one counter across all instruments), not
per instrument -- matching how ITCH-style feeds sequence a single physical
channel that happens to carry multiple instruments. `SequenceValidator`
(see `include/common/sequence_validator.hpp` -- it lives in `common/`
because it is reused for packet-level tracking too, see below)
classifies every sequence number relative to the last one it accepted:

- **InOrder** -- exactly `last + 1`.
- **Duplicate** -- exactly equal to `last` (a repeat of the immediately
  preceding message).
- **OutOfOrder** -- less than `last`, but not the immediately preceding
  value.
- **Missing** -- greater than `last + 1` (a gap of one or more sequence
  numbers).

The validator does not remember every sequence number it has ever seen --
only the last one. That means it cannot distinguish "a duplicate of
sequence 40" from "an out-of-order arrival of sequence 40" once the feed
has already moved on to sequence 90; both are reported as `OutOfOrder`.
Tracking full history would fix that at the cost of unbounded memory for
an unbounded replay -- a trade-off not worth making when all the caller
needs is to detect the problem and stop.

Replay always stops on the first non-`InOrder` classification
(`ReplayOptions::stop_on_sequence_error`, defaulted `true`), for both
transports -- file replay (`run_replay`) and UDP (`net::run_udp_listen`)
both funnel through the shared `replay::apply_frame_result()`, which is
where this policy is applied. The validator itself has no opinion on this
-- it only classifies -- so a skip-and-continue or buffer-and-reorder policy
could be added at that one call site without touching `SequenceValidator`.

## Packet framing (UDP)

UDP is a datagram protocol: each `sendto()`/`recvfrom()` pair transfers one
packet, either whole or not at all (no partial-packet delivery the way a
file read can hand back a truncated frame). To let one datagram carry more
than one event frame (batching), packets get their own framing, wrapping
one or more event frames:

```
PacketHeader (20 bytes) || event frame 1 || event frame 2 || ...
```

This is a second, separate header from the 20-byte event-frame header
above -- packet framing is a transport-level batching concern (how many
messages fit in one datagram), while the event-frame header/payload is the
application-level message format. Keeping them separate means
`decode_event()`/`SequenceValidator`/`BookManager` never need to know or
care whether a frame arrived via file or was unpacked from a UDP packet.

### Packet header (20 bytes)

| Offset | Field | Type | Notes |
|---|---|---|---|
| 0 | `magic` | `u32` | `0x4D444831` (ASCII `"MDH1"`); rejects non-mdh traffic early |
| 4 | `version` | `u16` | `1`; rejects a future incompatible packet format |
| 6 | `frame_count` | `u16` | how many event frames follow |
| 8 | `packet_sequence` | `u64` | transport-level sequence, distinct from any event's `sequence_number` |
| 16 | `payload_length` | `u32` | bytes of packed event frames that follow the packet header |

`payload_length` is the same kind of redundant cross-check as an event
frame's `payload_size`: it must exactly match the bytes actually present
after the packet header, independent of walking the contained frames'
own lengths.

### Unpacking

Each contained event frame carries its own `payload_size` (in its own
20-byte header), so the unpacker walks the packet payload one frame at a
time: decode a frame's header to learn its total length, slice that many
bytes off as the frame, advance, repeat -- exactly analogous to how
`EventFileReader` walks a file one frame at a time. `frame_count` bounds
how many frames to expect; if walking frames doesn't consume exactly
`payload_length` bytes by the time `frame_count` is reached, that's a
`FrameCountMismatch`.

### Two-tier error model

Packet-level errors (`PacketError`: bad `magic`/`version`, truncated
packet header, `payload_length` mismatch, an inner frame whose own header
is unreadable so its length -- and therefore the next frame's start --
can't be determined) invalidate the **whole datagram**: framing itself
couldn't be trusted, so nothing inside it can be safely extracted.

Event-level errors (`DecodeError`, e.g. `InvalidSide` on one contained
frame) do **not** invalidate the rest of the packet: framing succeeded
(frame boundaries were all locatable), only that one frame's content was
invalid. `net::unpack_frames()` returns one `std::variant<Event,
DecodeError>` per contained frame in this case, and callers (see
`net::run_udp_listen`) apply each independently via
`replay::apply_frame_result()`.

## Packet-level sequence tracking (observational only)

Every packet carries its own `packet_sequence`, tracked by a second,
independent `SequenceValidator` instance (`net::PacketSequenceTracker`)
purely for diagnostics -- packets received, in-order, duplicate,
out-of-order, and gap counts. This does **not** gate book reconstruction.
UDP packets can legitimately arrive out of order or duplicated for reasons
unrelated to data correctness (different network paths, retransmits,
redundant feeds); the actual correctness gate remains the event-level
`SequenceValidator` used inside `apply_frame_result()`, which validates
each event's own `sequence_number` regardless of which packet carried it.

## A dropped queue item looks identical to a dropped packet

`net::run_udp_listen()` decodes on a producer thread and
applies on a consumer thread, connected by a bounded queue
(`common/spsc_queue.hpp`) with a drop-newest policy under backpressure
(`common/dropping_queue.hpp`). Nothing about that queue is part of the
wire protocol -- it's purely an internal pipeline detail -- but it's worth
being explicit about its effect on sequencing: from
`apply_frame_result()`'s point of view, an event dropped by the queue and
an event lost because the underlying UDP packet never arrived are
*exactly the same thing*. Both simply never reach the validator, so both
show up identically as a gap in `sequence_number` once a later event
arrives. This is deliberate, not incidental: drop-newest was chosen
specifically because it
degrades into the same, already-handled failure mode as ordinary packet
loss, rather than introducing a second kind of gap the validator would
need to reason about differently.

One consequence worth calling out precisely: `SequenceValidator` detects a
gap *retrospectively*, only once a higher sequence number actually
arrives to reveal it. If a drop (queue-induced or packet-induced) happens
to be the last thing that would have arrived in a session -- nothing
comes after it -- the gap is never observed at all. That's not a bug in
the validator; it has no way to know how many messages *should* have
followed. It does mean "sequence_failures == 0" is not, on its own, proof
that nothing was ever dropped.

## Snapshot format

A snapshot (`replay/snapshot.hpp`) is a separate file format from both the
event-file and packet formats above, but deliberately reuses as much of
them as possible: every resting order becomes an ordinary `AddOrder` wire
frame -- the exact same 20-byte event header + 29-byte payload described
earlier in this document, encoded via the same `encode_event()` used
everywhere else. A snapshot entry and a live `AddOrder` message are
wire-identical; only the source (a book dump vs. an incoming message)
differs, and neither `encode_event()` nor `decode_event()` needs to know
which one they're looking at.

### Snapshot header (24 bytes, once, before all entries)

| Offset | Field | Type | Notes |
|---|---|---|---|
| 0 | `magic` | `u32` | `0x4D444832` (ASCII `"MDH2"`) -- distinct from `net::PACKET_MAGIC` (`"MDH1"`), so a snapshot file is never mistaken for a captured UDP packet |
| 4 | `version` | `u16` | `1` |
| 6 | `reserved` | `u16` | must be `0` |
| 8 | `sequence_number` | `u64` | the sequence_number of the last event fully applied before this snapshot was taken |
| 16 | `entry_count` | `u64` | how many `AddOrder` frames follow |

`entry_count` is 64 bits, unlike `net::PacketHeader::frame_count` (16
bits). A UDP packet's frame count is naturally bounded by how many frames
fit under an MTU-sized datagram; a snapshot has no such bound (a book can
hold arbitrarily many resting orders), so it needs the wider field.

Because the header has to be written *before* any entries, and
`entry_count` has to already be known at that point, `write_snapshot()`
encodes every entry into an in-memory buffer first, counts them, then
writes `[header][buffer]` in that order -- unlike `EventFileWriter`'s
streaming per-message writes, which never needed to know a total count
upfront since there is no header field depending on it.

### Entries

Each entry is a normal `AddOrder` frame: `order_id`, `instrument_id`,
`price`, `quantity`, `side`, exactly as specified earlier in this
document. `sequence_number` and `timestamp_ns` in each entry's header are
both set to the snapshot's own `sequence_number` / `0` respectively --
neither is meaningful for a snapshot entry (there is no real "when was
this added" for a book dump), and `read_snapshot()` never reads them back
out; it decodes each entry only for its `AddOrder` payload fields and
applies them via `OrderBook::add_order()`.

A snapshot only captures book depth (resting orders) -- not
`book::InstrumentStats` (trade count/volume/last price). Those are
already documented as informational-only elsewhere in this project (see
*Trade messages* above), so losing them across a `read_snapshot()` is a
named, deliberate simplification, not an oversight.

### Recovery semantics

`replay::apply_frame_result()` loads a snapshot only on a
`SequenceOutcome::Missing` classification (a genuine gap) when
`ReplayOptions::recovery_snapshot_path` is set -- not on `Duplicate` or
`OutOfOrder`, which stay governed by `stop_on_sequence_error` as before.
On a Missing classification with a snapshot path
configured: `outcome.books` is replaced wholesale by the snapshot's
state (not merged with whatever was applied before the gap), and the
event that revealed the gap becomes the new `SequenceValidator` baseline
(via `SequenceValidator::reset()`) rather than the snapshot's own
sequence number -- there is no gap-fill/retransmission service to
reconstruct exactly what happened in between, so re-checking that event
against the snapshot's sequence would just immediately re-trigger the
same gap. That event is then applied normally on top of the freshly
loaded state; if it references an order that only ever existed during the
now-unrecoverable window, that surfaces as an ordinary `BookError`/
`book_errors` count, not a crash -- the same machinery that already
handles any "operating on an order the book doesn't know about" case.
