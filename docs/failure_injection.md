# Failure injection

**Status:** implemented and run for real, including under ThreadSanitizer (see §4) —
same documentation discipline `docs/benchmarks.md` applies to its numbers, applied
here to *behavior* instead: every claim below is backed by an actual test in
this repository that drives the live, networked gateway (not the codec in isolation)
with deliberately adversarial bytes and asserts on the observed outcome.

Where `docs/benchmarks.md` covers *performance*, this covers *robustness*: dropped
packets, corrupt messages, and disconnects mid-stream.

---

## 1. What "failure injection" means here, precisely

Two test files, two different transports, one shared method: connect to the real,
running server component over a real socket (loopback TCP or UDP — no mocks, no
calling a decode function directly on a hand-built `std::span`) and send bytes no
well-behaved client would ever send, then assert on two properties every fault class
below must satisfy:

1. **The server component itself never crashes or hangs.** A brand-new, well-behaved
   connection/packet sent *after* the fault must still work normally.
2. **A fault on one connection/packet never degrades or blocks any *other*
   connection/packet.** Concretely: a second, well-behaved client's requests must not
   be delayed, dropped, or starved by another client's malformed input.

This is a different (and strictly stronger, for the paths it covers) claim than the
existing unit-level decode-error tests already in this repository
(`tests/test_order_entry_decode_errors.cpp`, `tests/test_decoder_errors.cpp`): those
prove the codec *classifies* malformed byte spans correctly in isolation. Nothing
about a decode function returning the right `std::variant` alternative proves that the
*live, threaded, buffering* component built on top of it — `connection_reader_loop()`'s
partial-frame accumulation, `market_data_loop()`'s per-datagram
`SequenceValidator`/`apply_frame_result()` sequencing — behaves correctly when that
malformed input arrives interleaved with real traffic, at the wrong time, or split
across socket reads/UDP datagrams in ways only a live transport can produce. That gap
is what `tests/test_failure_injection_gateway.cpp` and
`tests/test_failure_injection_market_data.cpp` close.

## 2. How to run these yourself

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j --target mdh_tests
./build/mdh_tests --gtest_filter="*FailureInjection*"
```

Both files are already part of the default `mdh_tests` target/CTest suite (see
`CMakeLists.txt`) — no separate flag or opt-in is needed to run them as part of the
normal test suite.

---

## 3. `OrderEntryGateway` TCP fault matrix (`tests/test_failure_injection_gateway.cpp`)

Six tests, each isolating one fault against the live gateway's I/O thread
(accept + read + write on IoPoller) or matching thread (see
`order_entry_gateway.hpp`'s own concurrency-model comment).

| Fault injected | Test | Gateway's documented response | Verified consequence |
|---|---|---|---|
| A single byte that is not any known `MessageType` | `InvalidTypeByteDesyncsOnlyThatConnectionNotTheGatewayOrOtherConnections` | Treated identically to "header not fully arrived yet" — there is no way to tell the two apart from a length-prefixed byte stream alone, so the byte is never discarded | *This* connection is permanently desynchronized (even a subsequent well-formed message on it is never recognized) — a deliberate, documented consequence, not a crash. A brand-new connection still round-trips normally; `connection_count()` still reflects both. |
| Well-formed header, `side` byte mutated to neither `Buy` nor `Sell` | `WellFormedHeaderWithInvalidPayloadFieldDropsOnlyThatFrameAndKeepsTheConnectionOpen` | Framing (header) is trusted, so only that one frame is dropped; the connection stays open and synchronized | The very next, valid message on the same connection is accepted normally (proven by it getting `client_order_id` 2, not 1 — the corrupted frame never produced a delayed response either). |
| Header promising N payload bytes, connection closed after sending only N/2, no FIN | `TruncatedPayloadNeverCompletedThenAbruptDisconnectDoesNotCrashTheGateway` | I/O thread keeps the partial frame in `read_buffer` until hangup; `read()` then returns EOF/Error | Gateway keeps running; a fresh connection afterward works normally. |
| TCP handshake completes, then immediate disconnect, zero bytes ever sent | `AbruptDisconnectImmediatelyAfterConnectingNeverHavingSentAnythingIsHarmless` | I/O thread never assumes a client message is coming | Repeated 5x; gateway still services a well-behaved connection afterward. `connection_count()` correctly reflects all 6 connections (5 abandoned + 1 live) — it never decreases, by design (see `Connection`'s own comment on why dead connections are not pruned). |
| A client that sends far more orders than `outbound_queue_capacity` can hold and never reads any response | `SlowNonReadingClientOverflowingItsOutboundQueueNeverBlocksAnotherConnection` | `route_event()` uses non-blocking `try_push()`; a full per-connection outbound queue silently drops that message rather than blocking the shared matching thread | A second, well-behaved client gets fast, complete service throughout (asserted per-message, not just at the end) — proving the matching thread was never blocked by the first client's full queue. The slow client eventually sees `0 < responses ≤ capacity` once drained, proving the system stayed consistent even though that one client lost messages by policy. |
| 4KB of deterministic non-protocol noise (a stand-in for a port scanner or misconfigured client) | `FloodOfRandomBytesFromANonProtocolClientDoesNotCrashTheGatewayOrOtherConnections` | Same header/payload framing logic as above — never calls anything on a "message" that was never decoded | Gateway and a second, well-behaved connection are both unaffected. |

**Why the first two rows differ in outcome (permanent desync vs. single dropped
frame) is itself the interesting, intentional finding**, not an inconsistency: it's a
direct consequence of framing being length-prefixed. An unrecognized *type* byte
corrupts the reader's ability to know where the *next* header even starts (there is no
resynchronization primitive in this wire format — no delimiter to scan for), so the
connection is unrecoverable without disconnecting and reconnecting. An unrecognized
*payload field value* under an otherwise-valid header changes nothing about where the
next header starts (`payload_size` in that header is still correct), so the connection
recovers immediately. This is documented directly in `order_entry_gateway.hpp`'s
`connection_reader_loop()` comment and independently confirmed here by actually driving
both cases over a real socket.

---

## 4. UI gateway UDP market-data fault matrix (`tests/test_failure_injection_market_data.cpp`)

Five tests against `UiGateway::market_data_loop()`, sending hand-built
raw UDP datagrams directly to its market-data port — bypassing `MarketDataPublisher`
entirely so every test has full, deliberate control over exactly what "goes wrong" on
the wire, then asserting on the live order book exposed via `GET /api/book/:id`.

| Fault injected | Test | Pipeline's documented response | Verified consequence |
|---|---|---|---|
| Whole datagram with the wrong magic number (not this wire format at all) | `CorruptWholeDatagramBadMagicIsSafelyDroppedAndDoesNotBlockLaterValidPackets` | `net::PacketError::InvalidMagic`; `market_data_loop()`'s `if (std::holds_alternative<net::PacketError>(unpacked)) continue;` drops the whole datagram | A subsequent valid packet is still processed normally — the corrupt datagram never left the loop in a bad state. |
| One packet with two frames: the first has an invalid `side` byte, the second is valid | `CorruptSingleEventInsideAWellFramedPacketIsDroppedButSiblingFramesStillApply` | Per `net/packet.hpp`'s documented contract, a per-frame `DecodeError` does not invalidate the packet as a whole — `unpack_frames()` walks past the corrupt frame using its own correctly-set `payload_size` | Only the valid sibling frame's order reaches the book; the corrupt frame's order never appears on either side. |
| A sequence gap (packet with sequence 2 never sent) | `SequenceGapDropsOnlyTheRevealingEventButThePipelineSelfResyncsAfterward` | See finding below — the event that *reveals* the gap is dropped, but the pipeline does not halt | The gap-revealing event never lands (checked *after* later events already arrived, to rule out a race won by asking too early); every event after the gap resumes applying normally. |
| The exact same packet sent twice (duplicated datagram) | `DuplicatedPacketNeverDoubleAppliesAndTheOrderCanStillBeCancelledExactlyOnce` | `SequenceValidator` classifies the repeat as `Duplicate`; dropped via the same early-return path as the gap case | No second resting order, no doubled quantity; the single canonical order is still cleanly cancellable afterward (proving `order_index_` was never corrupted into believing there are two orders, or zero, for that `order_id`). |
| A datagram shorter than `PACKET_HEADER_SIZE` (3 bytes) | `TruncatedDatagramShorterThanItsOwnPacketHeaderIsSafelyDropped` | `net::PacketError::TruncatedHeader` | A subsequent valid packet is still processed normally. |

**A real, non-obvious finding about the live path, verified here rather than merely
asserted from reading code** (the point of the sequence-gap test above): both
`market_data_loop()` and the offline `replay::run_replay()` call the exact same
`replay::apply_frame_result()` with the exact same default `ReplayOptions`
(`stop_on_sequence_error == true`, no recovery snapshot path) — but `market_data_loop()`
discards that call's return value (`(void)replay::apply_frame_result(...)`), while
`run_replay()` uses it to actually stop. So a sequence gap that would halt an offline
replay over a recorded file never halts the *live* UI gateway's feed. What it does
still do — and this is the part the test actually exercises over a socket, not just
reads off the source — is drop the one event that revealed the gap (the function
returns before reaching `apply_event()` for that event) while leaving
`SequenceValidator`'s high-water mark advanced to the new sequence, per its own
documented reason ("so later messages are checked against the new high-water mark
rather than re-reporting the same gap forever" — `SequenceValidator::check()`'s own
comment). Net effect: a live gap silently costs exactly one event, then the feed
carries on — a deliberate, reasonable choice for a live dashboard (staying up matters
more than halting on every transient UDP loss) that would be the wrong choice for
offline replay determinism, which is exactly why the two paths differ.

---

## 5. Concurrency-safety of these tests themselves

Every test in both files was additionally run under ThreadSanitizer
(`-DMDH_ENABLE_TSAN=ON`) — meaningful here specifically because several fault classes
(the slow non-reading client, the flood of random bytes, the duplicated/gapped UDP
packets) are only actually testing anything real if the reader/writer/matching/
market-data threads they exercise are genuinely running concurrently, not accidentally
serialized by test timing:

```bash
cmake -S . -B build-tsan -DMDH_ENABLE_TSAN=ON -DMDH_BUILD_BENCHMARKS=OFF
cmake --build build-tsan -j --target mdh_tests
./build-tsan/mdh_tests --gtest_filter="*FailureInjection*"
```

All 11 tests pass clean under TSan with no data-race reports — the same accept
thread / per-connection reader-writer threads / matching thread (gateway side) and
market-data thread / HTTP thread (UI gateway side) documented in each component's own
concurrency-model comment hold up under fault injection exactly as they do under normal
traffic in every other test in this repository.

---

## 6. Summary: what these tests establish

- **A fault on one TCP connection or one UDP datagram never propagates.** Every one of
  the 11 tests above proves this concretely, over a real socket, for a distinct fault
  shape — not just for the "nice" cases a codec unit test would already cover, but for
  live, threaded, buffering behavior (partial frames across reads, packets arriving
  out of order relative to `SequenceValidator`'s state) no unit test of the codec alone
  can observe.
- **Every fault degrades gracefully into an explicit, documented policy**, never an
  unhandled crash: permanently desync just this connection (bad type byte), drop just
  this frame (bad payload field), drop just this event (sequence gap/duplicate), or
  drop just this datagram (bad magic/truncated header) — each traceable to a specific
  comment in `order_entry_gateway.hpp`, `net/packet.hpp`, or `SequenceValidator`.
- **One real, previously-undocumented divergence was found and is now recorded**: the
  live UI gateway's market-data path does not halt on a sequence gap the way offline
  replay does, because it discards `apply_frame_result()`'s stop signal. Verified
  behaviorally here (§3's `SequenceGapDropsOnlyTheRevealingEventButThePipelineSelfResyncsAfterward`),
  not merely inferred from reading the two call sites.
- **These properties hold under ThreadSanitizer**, not just under whatever thread
  interleaving happened to occur on one run — giving real, not just observed-once,
  confidence that the concurrency claims in §1 hold in general.
