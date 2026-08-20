# Optimising the matching engine: what four stages bought, and what they cost

This is the record of a four-stage rework of `MatchingEngine` and `MatchingBook` — what
changed, what each change actually measured, what got worse, and what the whole exercise
did *not* achieve. The per-stage measurement files it draws on are in
[`bench-results/`](../bench-results/), one pair of before/after runs per stage plus the raw
benchmark output behind each.

The short version: the matching engine got between 40% and 60% faster on every path that
matters and roughly 40% smaller per resting order, and the product's end-to-end latency did
not move by a nanosecond. Both halves of that sentence are the result.

---

## 1. Where it started, and why these four stages

The engine before any of this was not badly written; it was written for clarity, and it had
three structural costs that no amount of local tuning removes:

- **A hash per instrument.** `std::unordered_map<InstrumentId, MatchingBook>`, looked up on
  every single command, and — because it was `books_[instrument_id]` — a *lazy insert*. Any
  instrument id a client invented got a book allocated for it, permanently.
- **A pooled list node per resting order.** A price level was a `std::pmr::list<BookOrder>`:
  64 bytes of node per order, reached by pointer chase, plus a 32-byte list header living
  inside the price-index map node. A `Handle` was `{side, price, list iterator}`, 24 bytes,
  which made the engine's own order directory node 80 bytes.
- **A red-black tree per side.** Finding a price level was a tree descent; finding the touch
  was a walk to one end of it. Cancelling against a book with 4096 price levels cost eight
  times what cancelling against one level cost, and that gap was entirely the tree.

The four stages attack those in dependency order. That order is not cosmetic: stage 3's tick
ladder is sized `instruments × band × 2 sides`, which is unbudgetable until the instrument set
is bounded and known — which is stage 1. Stage 2 sits between them so that the slab's effect
could be measured on its own rather than being tangled up with the ladder's.

---

## 2. Stage 1 — an instrument registry with dense book slots

**What changed.** The engine takes its universe at construction. `slot_of_id_` is a
direct-mapped `vector<uint32>` sized to the largest id (capped at 2²², so 16 MB worst case),
`books_` is a dense vector indexed by slot, and `by_id_` is a sorted `(id, slot)` list so
`snapshot()` no longer sorts on every call. The bounds check *is* the validity check: an id
past the end is simply not registered, and its command is rejected with
`RejectReason::InvalidInstrument` ahead of every other field.

The command journal gained a fourth frame type, `RegisterInstrument`, written once per
instrument when the journal opens. Replay learns the universe from the file instead of being
told it out of band.

**What it bought** (within-session before/after, `bench-results/stage1-registry-before-after.txt`):

| | before | after | change |
|---|---|---|---|
| mixed realistic workload | 103.6 ns/op | 92.1 ns/op | −11.1% |
| resting onto an existing level, 1024 levels | 72.7 ns/op | 61.4 ns/op | −15.5% |
| cancel against 4096 levels | 119 ns | 75.6 ns | −36.5% |
| FOK rejected, nothing crossable | 14.7 ns | 6.51 ns | −55.7% |
| 1M-order footprint | 345.9 B/order | 345.9 B/order | unchanged |

The FOK row is the cleanest read on what the stage did: that path is a lookup and an immediate
answer, and it used to pay a *hash miss* (`crossable_quantity()` called `books_.find()` and
returned zero when it missed). A miss is also what a tail sample is, which is why p99 moved
further than p50 (417 → 291 ns against 125 → 84).

**What it cost.** A real behaviour change: commands for unregistered instruments are now
rejected rather than silently creating a book. That is the point — the old behaviour let a
client drive unbounded allocation from a field it controlled — but it is a change, and it
rippled through 69 construction sites, the journal format, and every layer that had to start
carrying a universe.

**Why it really exists.** Not the nanoseconds. It exists so the engine stops allocating on
client-controlled input, so replay is self-describing, and so stage 3 becomes affordable.

---

## 3. Stage 2 — a slab and intrusive FIFO levels

**What changed.** The `std::pmr::list` per level is gone. Orders live in one flat per-book
slab and a price level is a pair of indices into it:

```cpp
struct SlabOrder { BookOrder order; uint32 next, prev; };  // 56 bytes, free list threaded through `next`
struct LevelSlot { uint32 head, tail; };                   // 8 bytes, still in a std::map at this stage
```

`Handle` collapses from `{side, price, list iterator}` to a single `uint32` slot — the order
knows its own side and price, so the handle no longer has to. That takes `OrderRef` from 48
bytes to 24 and the engine's directory node from 80 to 56.

Using indices rather than pointers has a consequence worth naming: a handle survives the slab
being relocated. The list-iterator version needed the pool to guarantee that.

**What it bought** (`bench-results/stage2-slab-before-after.txt`):

| | before | after | change |
|---|---|---|---|
| cancel against 4096 levels | 154.7 ns | 80.1 ns | −48.2% |
| cancel against 65,536 levels | 125 ns | 79.1 ns | −36.7% |
| sweep, per level consumed | 32.8 ns | 28.5 ns | −13.1% |
| match one level of 256 orders | 5827 ns | 4085 ns | −29.9% |
| 1M-order footprint, 1 order/level | 345.9 B/order | 208.0 B/order | −39.9% |

The cancel rows were the surprise. The slab was supposed to pay for itself on insert and
sweep; a cancel is dominated by the tree walk, which this stage did not touch. The explanation
is indirect: the map node lost the 32-byte list header it used to carry, so a node is 48 bytes
where it was 72, more of the tree fits in cache, and every level of the descent is likelier to
hit. p50 barely moved while p90 and p99 improved 15–30% — the signature of removing a cache
miss rather than removing instructions.

**What it cost.** A flat vector grows by relocating, and the amortisation is exactly the
problem: at a million resting orders an insert measured **104 ns** against a slab that grew
into itself, versus 61 ns for the list it replaced. Every other row improved; that one
regressed 69%. The fix was to use the `expected_resting_orders` figure the engine already
took and every caller already passed honestly — each book reserves its share — and the row
came back at 47 ns, 24% *faster* than the list. The alternative (chunked, never-relocating
storage) was rejected because it adds an indirection to every slab access including the sweep,
which is the path this stage most improves.

---

## 4. Stage 3 — a tick ladder with a hierarchical occupancy bitmap

This stage had a decision in front of it that the plan deliberately deferred until there was
something to measure, so the measurement came first.

### 4.1 The measurement that chose the design

A ladder is an array indexed by tick, so what it costs is the *span* of occupied prices, not
how many of them are occupied. A new section in `bench_matching_workload` reports both:

| scenario | orders | levels | span | in 8192 |
|---|---|---|---|---|
| realistic workload, 1 instrument | 8918 | 74 | 84 | 100% |
| realistic workload, 64 instruments | 1033 | 60 | 86 | 100% |
| benchmark seed, 4096 levels | 4096 | 4096 | 4096 | 100% |
| benchmark seed, 65,536 levels | 65,536 | 65,536 | 65,536 | 12.5% |
| benchmark seed, 1,000,000 levels | 1,000,000 | 1,000,000 | 1,000,000 | 0.8% |

A realistic book is tiny and dense — 84 ticks of span holding 74 occupied levels, 88% of the
range in use — which is close to the ideal case for a ladder. The benchmark's own scaling rows
are the opposite by design: they seed one order per tick across up to a million consecutive
ticks to push the price index until it breaks.

That ruled out two of the three options. **Rejecting** out-of-band prices would reject the
majority of the orders in four of this suite's own rows, and adopting a risk control to avoid
writing a fallback is letting the implementation choose the semantics. **Sliding** the band
exists to follow drift, and this generator anchors prices to a static mid — there is no drift
here, so anything built would have been tuned against a constant. The full reasoning is in
[`bench-results/stage3-ladder-band-decision.txt`](../bench-results/stage3-ladder-band-decision.txt).

### 4.2 What was built

Per side: a flat `LevelSlot[band]` plus a three-level occupancy bitmap (one bit per tick, one
bit per bitmap word, one summary word). Finding the touch is three dependent loads and three
count-leading-zeros over 1 KB that stays in L1. Finding a level is an array index. Out-of-band
prices go to a `std::pmr::map`, and every walk merges the two indexes.

Three details that matter:

- **The ladder array is never initialised.** An entry means nothing unless the bitmap says the
  tick is occupied, so there is nothing to zero and untouched pages are never faulted in.
- **The band comes from a whole-engine memory budget**, 8 MB divided across the universe and
  rounded down to a power of two: 8192 ticks at up to 64 instruments, 4096 at 128, 1024 at 512,
  and no ladder at all past that. This is the part stage 1 made possible, and it is what keeps
  the 20,000-instrument test affordable — those books get no ladder and run on the map alone.
- **An empty side re-anchors.** Not a sliding band, and no cost on any hot path: just the
  observation that a side with nothing resting has no reason to keep its old base.

### 4.3 What it bought

Within-session before/after, where "before" is the same binary with the budget set to zero —
same allocator, same session, one variable (`bench-results/stage3-ladder-before-after.txt`):

| | before | after | change |
|---|---|---|---|
| mixed realistic workload | 134.0 ns/op | 80.3 ns/op | −40.1% |
| mixed workload p50 | 125 ns | 83 ns | −33.6% |
| resting onto an existing level, 1024 levels | 51.0 ns/op | 32.7 ns/op | −35.9% |
| 64 instruments, mixed workload | 166.5 ns/op | 98.7 ns/op | −40.7% |
| FOK rejected against 1024 crossable levels | 45,088 ns | 5,255 ns | −88.3% |
| cancel against 4096 levels | 84.6 ns | 64.1 ns | −24.2% |

The FOK preflight rows are the single clearest result of the whole exercise.
`crossable_quantity()` walks levels from the touch until the price bound stops it, and walking
N levels used to be N tree hops through pooled nodes scattered across memory. It is now N steps
through a bitmap that fits in L1.

A useful control: **priority-preserving replace never touches the price index** — it writes a
quantity or a client order id into the slab and stops — and it is the one operation in the
whole table that does not move. That is the shape the change predicts.

### 4.4 What it cost

- **A one-level book is 24% slower to replace into.** Creating and destroying a price level
  costs six dependent accesses across three arrays where a one-node map costs a single pooled
  node already in cache. The ladder's fixed per-level cost is higher; it wins by making the
  *count* of levels stop mattering. The crossover is visible in the same family of rows: 1
  level is 24% worse, 16 levels is 38% better, 256 levels is 66% better.
- **A thousand-order book uses 43% more memory.** The ladder is a fixed 65 KB per active side.
  By ten thousand orders that is 3%; by a hundred thousand it has disappeared. A book too small
  to amortise a ladder is also a book too small for any of this to matter.
- **Books wider than the band are unchanged**, by construction. The 65,536-, 262,144- and
  1,000,000-level rows spill 94%, 98% and 99.6% of their levels to the map and measure within a
  few percent of where they started. That was the prediction and it held.
- **A 1.7 ns tax on trivial operations** — an FOK rejected against an empty book went from 10.9
  to 12.5 ns, which is the extra indirection with nothing else to hide behind.

---

## 5. Stage 4 — re-profiling, and the result that matters most

Everything above measures the matching engine. Stage 4 asks what any of it did to the product,
by measuring a real `NewOrder` over a real loopback TCP connection to the fully-wired gateway —
decode, risk, ledger, matching, encode, and two thread handoffs.

To make that interpretable, `bench_end_to_end_latency` now measures a **transport floor** in
the same process and the same run: the identical client sending the identical bytes to a server
whose entire job is to notice a whole frame arrived and write back a canned `Accepted`.

| | p50 | p90 | p99 |
|---|---|---|---|
| fully-wired gateway | 69.8 µs | 78.7 µs | 147.5 µs |
| transport floor | 47.8 µs | 53.5 µs | 87.0 µs |
| **what this codebase adds** | **22.0 µs** | **25.2 µs** | **60.4 µs** |

End-to-end p50 was 70.6 µs before any of these four stages. It is 69.8 µs after them. **The
matching engine work moved the product's latency by nothing measurable, and this was the
predicted outcome, written into the plan before stage 1 began.**

The arithmetic is not subtle. Two thirds of the round trip is loopback TCP and the macOS
scheduler, which this codebase cannot touch. Of the 22 µs it *does* own, the matching engine is
about 0.08 µs — the mixed workload runs at 80 ns/op, and this benchmark's operation (an IOC
against an empty book) is cheaper still. So the matching engine is roughly **0.1% of the round
trip and 0.4% of the addressable part of it.** Making it twice as fast again would be
unmeasurable end to end.

A sampling profile of the round trip agrees, though it has to be read carefully — `sample`
samples every thread including ones parked waiting, so this is not a time attribution of the
critical path. What it does show is that there is essentially no compute anywhere to attribute:
the top of stack is `read`, `__semwait_signal`, `swtch_pri`, `__psynch_cvwait` and `write` by
several orders of magnitude, and user code barely appears.

---

## 6. Cumulative, and an honest caveat about it

The three timing stages were each measured before-and-after **within a single session**, which
is the only comparison this machine supports. Absolute figures drift substantially between
sessions — the same code measured 94 ns/op at the end of stage 2 and 134 ns/op at the start of
stage 3 — because this is a laptop with thermal management and other work on it. So the
per-stage deltas are trustworthy and their product is only indicative:

- Stage 1: −11%, stage 2: −7%, stage 3: −40% on the mixed realistic workload. Composed, that
  is roughly **half the time per operation**, but no single run measured that end to end.

What *is* exactly comparable, because it is deterministic rather than timed:

| | before stage 1 | after stage 3 |
|---|---|---|
| 1,000,000 orders, 1 order/level | 345.9 B/order | 207.8 B/order |
| 1,000,000 orders, 100 orders/level | 214.9 B/order | 140.3 B/order |
| allocations for a 1M-order book | 815 | 563 |
| allocations per price level | 1 | 0 (in band) |
| `MatchingBook::Handle` | 24 bytes | 4 bytes |
| `OrderRef` / directory node | 48 / 80 bytes | 24 / 56 bytes |

Creating a price level no longer allocates at all. That shows up in the footprint table as two
book shapes — a thousand orders across a thousand levels, and a thousand orders stacked ten
deep — reporting an identical figure where they used to differ by 40%.

---

## 7. Correctness, and how it was kept

Every stage ended with the full suite green and clean under AddressSanitizer: **407 tests, 24 of
them added by this work.** The ones that carry the most weight are differential rather than
example-based:

- `MatchingBookStress.PriceLevelLifecycleMatchesReferenceModel` runs 400,000 random operations
  against an independent `std::map`-based reference model, comparing best-price on every
  operation and full book structure every 512. Stage 3 added two more instances of it: one at a
  price spread five times the band, so four prices in five are out of band and every walk is a
  live merge of the two indexes, and one with no ladder at all.
- `MatchingEngineStress.MixedCommandStreamPreservesEveryInvariant` replays a 3,000,000-operation
  generated stream and checks every invariant as it goes.
- `MatchingEngine.RegisteringAnInstrumentKeepsRestingOrdersReachable` pins the handle-stability
  invariant that both stage 1 and stage 2 depend on: rest an order, register 63 more instruments
  (forcing the book vector to relocate every book), then cancel through a handle taken before
  the moves.

---

## 8. What is left

**Do not optimise the matching engine further for latency reasons.** Section 5 is the argument.
There may be good reasons to keep going — the out-of-band path is untouched, and a sliding band
would matter for an instrument that trends through its band during a session — but "it will make
the system faster" is no longer one of them, and pretending otherwise would be the easiest
mistake available here.

If end-to-end latency is the goal, the target is the 22 µs this codebase adds, and the honest
first step is per-stage instrumentation inside the gateway — timestamps at decode, risk, ledger,
match, encode, and each of the two thread handoffs — because right now that 22 µs is a single
undifferentiated number and any guess about its composition would be exactly that. The two
handoffs are the obvious suspects, but they are a suspect, not a finding.

The stage 5 that was planned — prefetching, branch shaping, page prefaulting, CPU affinity,
`mlock`, huge pages — is deliberately not done, and now looks like it should stay that way.
Every item on it is a local constant-factor win on a component that is 0.1% of the round trip,
and half of them (affinity, huge pages) cannot be measured honestly on macOS at all.

---

## 9. Reproducing any of this

Release build only; a debug build's numbers are not representative of anything.

```bash
cmake --build build -j
./build/bench_matching_workload                    # whole-engine latency, throughput, scaling, level distribution
./build/bench_matching_engine --benchmark_repetitions=5 --benchmark_report_aggregates_only=true
./build/bench_matching_memory                      # footprint and allocation counts
./build/bench_end_to_end_latency                   # gateway round trip against the transport floor
```

The before/after arms for stage 3 are reproducible by setting
`MatchingEngine::kLadderByteBudget` to zero, which turns the ladder off and leaves stage 2's
structure exactly as it was.
