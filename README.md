# mdh — a simulated exchange, end to end

A C++20 project that implements the pieces a stock exchange is actually made
of: a matching engine that decides which orders trade, a TCP gateway that
clients connect to, a UDP market-data feed that broadcasts what happened, a
trader-side stack that consumes the feed and sends orders back, and a web
dashboard to watch it all happen live.

It is a learning and portfolio project, not a production venue. Everything
runs on one machine, over loopback, with synthetic money. But the shapes are
real: the concurrency model, the wire formats, the risk checks, and the
data structures are the ones a real system uses, and the performance numbers
in this repo are measured, not estimated.

**New to trading systems?** Start with [What an exchange
does](#what-an-exchange-does) below — it explains every term used later. No
finance background is assumed anywhere in this README.

---

## Contents

- [What an exchange does](#what-an-exchange-does)
- [The 60-second version](#the-60-second-version)
- [The pieces, and which process they live in](#the-pieces-and-which-process-they-live-in)
- [Walkthrough: the life of one order](#walkthrough-the-life-of-one-order)
  - [1. The client connects](#1-the-client-connects)
  - [2. Bytes on the wire](#2-bytes-on-the-wire)
  - [3. Reading a message off a TCP stream](#3-reading-a-message-off-a-tcp-stream)
  - [4. Crossing into the exchange: the command queue](#4-crossing-into-the-exchange-the-command-queue)
  - [5. Risk checks and the ledger](#5-risk-checks-and-the-ledger)
  - [6. Matching](#6-matching)
  - [7. Events, and the three places they go](#7-events-and-the-three-places-they-go)
  - [8. Getting the reply back to the right client](#8-getting-the-reply-back-to-the-right-client)
- [Market data: the public broadcast](#market-data-the-public-broadcast)
- [The trader side](#the-trader-side)
- [Simulated participants](#simulated-participants)
- [The dashboard](#the-dashboard)
- [Determinism, journaling, and replay](#determinism-journaling-and-replay)
- [Why these networking and queueing choices](#why-these-networking-and-queueing-choices)
- [Performance](#performance)
- [Repository layout](#repository-layout)
- [Building and running](#building-and-running)
- [What is real and what is simulated](#what-is-real-and-what-is-simulated)
- [Further reading](#further-reading)

---

## What an exchange does

An exchange is a matchmaker. Buyers and sellers send it **orders**, and its
job is to pair them up fairly and tell everyone what happened.

A **limit order** says "buy 100 shares of instrument 5, at 99.50 or better."
The price is a limit, not a request: a buyer will pay 99.50 or less, never
more. If nobody is willing to sell at that price right now, the order does
not disappear — it waits. An order that is waiting is called a **resting
order**, and the collection of all resting orders for one instrument is the
**order book**.

The book has two sides. **Bids** are orders to buy, sorted highest first,
because the buyer offering the most money is the most attractive one to sell
to. **Asks** (or offers) are orders to sell, sorted lowest first, for the
mirror-image reason. The highest bid and the lowest ask are the **top of
book**, and the gap between them is the **spread**. Normally the spread is
positive: the best buyer wants to pay less than the best seller wants to
receive, so nothing trades and both sit there waiting.

A trade happens when a new order **crosses** the spread — someone finally
offers to buy at or above what a seller is asking. The new order is the
**aggressor** (it took liquidity off the book); the order that was already
sitting there is the **passive** or resting side (it provided liquidity).

When several resting orders could fill an incoming aggressor, the exchange
needs a rule to decide who trades first, and that rule has to be published
and mechanical, because it is worth money. This project uses the most common
one, **price-time priority**: best price wins first, and among orders at the
same price, whoever arrived earliest wins. That second half is why each price
level is a **FIFO queue** (first in, first out) rather than a set.

Two more conventions, both of which show up in the code:

- **A trade executes at the resting order's price, not the aggressor's.**
  If a seller has been sitting at 99.50 and a buyer arrives willing to pay
  100.00, the trade happens at 99.50. The resting order committed to a price
  first, so it gets it, and the aggressor gets a better fill than it asked
  for. This is called price improvement.
- **Prices are integers, never floating point.** A price is stored as a
  count of **ticks**, where one tick is 0.0001 of a currency unit. `0.1 +
  0.2 != 0.3` in binary floating point, and an exchange cannot afford that
  kind of imprecision in something it must reproduce exactly. Every price in
  this codebase is an `int64_t`.

Orders can also carry a **time in force**, which says how long they are
allowed to live:

| | meaning |
|---|---|
| **GTC** (good till cancelled) | fill what you can, rest the remainder on the book until cancelled |
| **IOC** (immediate or cancel) | fill what you can right now, throw away the rest |
| **FOK** (fill or kill) | fill *completely* right now or do nothing at all |

Finally, two acronyms you will see everywhere in this space. **ITCH** and
**OUCH** are the names Nasdaq gave to its two protocols, and the industry now
uses them as generic labels for the pair of jobs any exchange needs. ITCH is
the *market data* protocol: one-way, public, broadcast to everybody, "here is
what just happened to the book." OUCH is the *order entry* protocol:
two-way, private, one session per client, "here is my order" / "here is what
happened to your order." They are separate protocols because they have
genuinely different requirements, which the next sections get into. **FIX**
is a third protocol, older and text-based (human-readable tag=value pairs),
widely used for institutional order flow precisely because it is flexible and
self-describing — and largely avoided on latency-critical paths for exactly
the same reason, since parsing text is far slower than reading a fixed-layout
binary struct.

This project does not implement real ITCH, OUCH, or FIX. It implements two
small binary protocols of its own that are *shaped like* ITCH and OUCH, so
the design tradeoffs are the real ones without the spec-compliance work.

---

## The 60-second version

Here is the whole system in one picture. Each box is code in this repo.

```
   YOUR CLIENT                        THE EXCHANGE                       EVERYONE ELSE
   ───────────                        ────────────                       ─────────────

   strategy                 ┌──────────────────────────────┐
      │                     │  order-entry gateway (TCP)   │
      v                     │                              │
   trader risk              │   accept thread              │
      │                     │   reader thread (per client) │
      v                     │        │                     │
   OMS ──────TCP────────────┼───► [MPSC queue]  N readers, no mutex
      │   (private,         │        │                     │
      ▲    reliable,        │        v                     │
      │    ordered)         │   ═══ THE MATCHING THREAD ═══│
      │                     │   risk check → ledger hold   │
      │                     │   → matching engine          │
      │                     │        │                     │
      │                     │        v  events             │
      │                     │   ┌────┴────┬──────────┐     │
      └──[SPSC queue]◄──────┼───┤         │          │     │
        writer thread       │  ledger   market-data  │     │
        (per client)        │           publisher    │     │
                            └────────────┬───────────┘     │
                                         │                 │
                                         └───UDP broadcast──┼──►  market-data
                                          (public, lossy,   │      listeners,
                                           unordered)       │      the dashboard,
                                                            │      other strategies
```

Read left to right: a client sends an order over a private TCP connection,
one thread does all the matching so that the result is deterministic, and
whatever happened gets broadcast publicly over UDP while the private
confirmation goes back down the client's own socket.

The two arrows leaving the exchange are the ITCH/OUCH split from the previous
section, and the transport choice follows directly from it. Order entry is
TCP because losing a client's order is unacceptable. Market data is UDP
because it is broadcast to many listeners, it is fine for a listener to miss
a message and recover, and waiting for retransmissions would delay everybody
to accommodate the slowest receiver.

---

## The pieces, and which process they live in

Everything above can run in a single process, `trading_server`, which is what
the demo does:

```bash
./build/trading_server --tcp-port 7000 --market-data-port 7001 --http-port 8080
```

That one process owns:

| component | what it does | code |
|---|---|---|
| **OrderEntryGateway** | accepts TCP connections, manages sessions, translates wire messages to commands | `exchange/gateway/` |
| **MatchingPipeline** | the queue and the single matching thread behind it | `exchange/sequencing/` |
| **RiskEngine + Ledger** | pre-trade checks and per-account cash/position balances | `exchange/risk/`, `exchange/ledger/` |
| **MatchingEngine** | the order books and the matching rules | `exchange/matching/` |
| **MarketDataPublisher** | turns internal events into public feed messages | `exchange/market_data/` |
| **UiGateway** | REST + Server-Sent-Events for the browser dashboard | `ui_gateway/` |

The trader-side stack (`trader/`) is a *client* of all this. It runs inside
`trading_server` for the dashboard's demo accounts, and as a separate process
in `apps/live_strategy_demo`, but either way it talks to the exchange only
over real sockets — it never reaches into exchange internals.

---

## Walkthrough: the life of one order

### 1. The client connects

A client opens an ordinary TCP connection to the gateway's port. The accept
thread hands each new connection two threads of its own: a **reader** that
does nothing but pull bytes off that socket, and a **writer** that does
nothing but push bytes onto it. Nothing else in the system is allowed to
touch that socket.

The connection is a **session**. The gateway needs to know which account a
session is trading for, because it must never let one client see another
client's fills. There is no login handshake — every client message already
carries an `account_id`, so the session binds to whatever account its first
valid message names, and two rules follow:

1. **The binding is permanent.** If a later message on that connection names
   a different account, the gateway answers `Rejected{AccountMismatch}` and
   the message never reaches the matching engine. A session can never trade,
   or spend the balance of, an account other than the one it bound to.
2. **One account may have many sessions.** A second connection for the same
   account joins it rather than displacing the first.

Rule 2 raises the obvious question: if account 42 has three connections open,
which one gets the fill report? Not all three — these are private messages,
and a session that placed nothing should not learn about an order it never
sent. So the gateway remembers, for every order, which session submitted it,
keyed on `(account_id, client_order_id)`. When a report comes back it goes
to the originating session; if that session has disconnected, to another live
session of the same account; and if the account has no live session at all,
into a small bounded queue that is replayed to the next session that
connects. That last case is real, not hypothetical: a resting order outlives
the connection that placed it, and can fill hours later.

### 2. Bytes on the wire

Both protocols in this project are binary, fixed-layout, and big-endian.

**Binary and fixed-layout**, because a decoder that already knows a
`NewOrder` payload is exactly 39 bytes can validate and read it with a
handful of loads, while a text protocol has to scan for delimiters and parse
numbers. Every message type here has a size known at compile time.

**Big-endian** ("network byte order") because it is the convention for wire
formats, and — more importantly — because every field is read and written
with explicit bit shifts rather than by `memcpy`-ing a struct. Struct layout
depends on the compiler's padding and alignment choices and on the host's
endianness, none of which are part of a wire contract. Shifts are correct on
any machine, aligned or not.

The two protocols deliberately share nothing — not a header, not an
enum, not a message type. They have different jobs:

| | market data (`protocol/`) | order entry (`protocol/order_entry/`) |
|---|---|---|
| resembles | ITCH | OUCH |
| transport | UDP | TCP |
| direction | one-way, exchange → world | two-way, one client ↔ gateway |
| audience | public, everybody | private, one account |
| header | 20 bytes | 3 bytes |
| header carries | type, size, **sequence number, timestamp** | type, size |
| messages | AddOrder, CancelOrder, ModifyOrder, Trade, ClearBook | NewOrder, CancelOrder, ReplaceOrder / Accepted, Rejected, Cancelled, Replaced, TradeReport |

The header sizes are the interesting difference, and both follow from the
transport:

- **The order-entry header is 3 bytes** (a type and a payload length)
  because that is all a TCP reader needs. TCP is a byte stream with no
  message boundaries — one `read()` can return half a message, or three
  and a half — so the header's only remaining job is framing: telling the
  reader how many more bytes to wait for.
- **The market-data header is 20 bytes** because UDP gives you the framing
  for free (one `recvfrom()` returns exactly one datagram) but takes away
  the reliability. It therefore has to carry a **sequence number**, so a
  receiver can notice that message 41 arrived after message 39 and conclude
  that 40 was lost. Order entry needs no sequence number at all: TCP already
  guarantees ordered, lossless, exactly-once delivery of the stream, so an
  application-level sequence number would have nothing left to detect.

Market-data messages are additionally batched: `net::pack_frames()` wraps one
or more event frames in a second, packet-level 20-byte header before sending
the datagram. Packet framing is a transport concern kept deliberately
separate from message format, so the decoder and the book never need to know
whether a frame came from a socket or a file.

### 3. Reading a message off a TCP stream

The reader thread loops on a blocking `read()` and appends whatever it gets
to a per-connection buffer. Then it tries to decode as many complete messages
as that buffer now holds. This is the part people usually get wrong on their
first TCP protocol: **there is no guarantee that one `read()` returns one
message**, and a reader that assumes otherwise works fine on loopback and
falls apart on a real network.

Two failure modes are handled differently, and the distinction matters:

- **A truncated message, or a header with an unrecognized type byte**, means
  the reader waits for more bytes. It cannot tell these two cases apart from
  a length-prefixed stream alone.
- **A malformed payload under a valid header** drops that one message and
  keeps the connection open, because the framing is still intact — the
  reader knows exactly where the next message starts.

A decoded message becomes an `ExchangeCommand`: `NewOrderCommand`,
`CancelOrderCommand`, or `ReplaceOrderCommand`. This is the boundary where
the network stops. Commands know nothing about sockets, sessions, or wire
bytes, which is what makes the exchange core replayable (see
[Determinism](#determinism-journaling-and-replay)).

### 4. Crossing into the exchange: the command queue

Every command now has to reach one specific thread.

**Why one thread?** Because the matching engine must be deterministic. Given
the same commands in the same order, it must produce exactly the same trades
— that is what makes it auditable, replayable, and testable. Two threads
matching against the same book would make the outcome depend on which one won
a race, and no amount of locking makes that reproducible. So there is exactly
one matching thread, it owns the books outright, and it needs no locks
because nothing else ever touches them.

Commands reach it through a bounded **multi-producer / single-consumer
(MPSC) ring** (`common/mpsc_queue.hpp`). Every connection reader may call
`MatchingPipeline::submit()` concurrently; there is no submit mutex.
Producers CAS-reserve slots, and per-slot publication tickets let the
consumer preserve reservation order even when producers finish stores out
of order.

That defines sequencing explicitly:

- One reader submitting A then B preserves A-before-B, so each session keeps
  its TCP decode order.
- Concurrent sessions are ordered by the lock-free admission race, not by
  wall-clock TCP arrival or mutex acquisition.
- The matching thread assigns `command_sequence` only after dequeue,
  immediately before processing. A full-queue rejection consumes no
  sequence number.

Matching itself remains single-threaded and owns the books outright.

**A full queue is a rejection, not a drop.** This is the sharpest contrast
with the market-data side of the codebase, which has a `DroppingQueue` that
silently discards on overflow. Dropping a market-data frame is recoverable:
the receiver notices a sequence gap and repairs it. Dropping a client's order
is not: the client would believe it had an order working that the exchange
never saw, with no way to tell that apart from an acknowledged rejection. So
`submit()` returns `false` and the client is told.

### 5. Risk checks and the ledger

On the matching thread, before the engine sees the command, two things
happen.

**The risk check** (`exchange/risk/`) asks whether this account can afford
this order, against its *available* balance — what it owns minus what its
existing resting orders have already committed. An order over the venue's
maximum size is rejected too. Cancels are never risk-checked, since a cancel
cannot increase exposure. A replace is checked only on the *additional*
exposure it creates, crediting the reservation the original order already
holds, so shrinking an order always passes.

**The ledger** (`exchange/ledger/`) tracks cash and positions per account,
and holds a **reservation** against a resting order. The reasoning behind
reservations is worth spelling out, because it explains a design decision
that looks inconsistent at first:

A GTC order can rest for a long time while other commands are processed. The
money behind it must be locked for that entire period, or two GTC orders from
the same account could each pass a balance check against the same funds.

IOC and FOK orders are different. The engine resolves them completely inside
a single `process()` call, and matching is single-threaded, so no other
command can interleave. There is no window in which anything could
double-spend against them, so **they are never reserved at all** — their
fills settle directly against the total balance. This is not just an
optimization: IOC discards its unfilled remainder silently, with no event to
observe, so a reservation opened for an IOC order would have no signal to
ever release it. Not taking one avoids the problem entirely.

Reservations are held at the order's *limit* price, the worst case the
account could owe. When an aggressive buy fills at a better price than its
limit, the full reservation is released but only the true trade price is
debited, and the difference reappears in the available balance automatically.

### 6. Matching

Now the engine (`exchange/matching/`) runs. For a new order it checks, in
this order: is this an instrument we trade, is the price positive, is the
quantity non-zero, and is this client order id already in use. Instrument
first, deliberately — an order for an unknown instrument was sent to the
wrong venue, and saying so is more useful than reporting whichever other
field also happens to be wrong.

FOK is then resolved before anything else, by asking the book how much
quantity is crossable at this price. If it is less than the order wants, the
order is rejected without ever touching the book: no acceptance, no partial
fill, nothing to undo.

Otherwise the order is accepted and matched. The engine walks the opposite
side of the book from the best price inward, trading against each resting
order in FIFO arrival order, at the resting order's price, until either the
incoming order is exhausted or the next resting price no longer crosses.
Whatever remains rests on the book if it is GTC, and is discarded if it is
IOC or FOK.

Underneath, the book is not the `std::map` of price levels you would write
first. It is three structures working together, arrived at through four
rounds of measurement documented in
[`docs/matching_engine_optimisation.md`](docs/matching_engine_optimisation.md):

- **A slab.** Every resting order lives in one contiguous vector, and the
  per-price FIFO queues are intrusive linked lists of *indices* into it, not
  pointers to separately-allocated nodes. One allocation instead of one per
  order, no pointer chasing between neighbours, and a handle to an order
  shrinks from 24 bytes to a 4-byte index.
- **A tick ladder.** Price levels live in a flat array indexed directly by
  price, so finding a level is an array subscript rather than a tree
  descent, alongside a three-level bitmap that marks which ticks are
  occupied. Finding the best price becomes a couple of
  count-leading-zeros instructions instead of a walk to the end of a tree.
- **An overflow map.** The ladder covers a fixed band of prices around
  wherever the book is currently active. Prices outside it fall back to an
  ordered map, and the two are merged transparently when the book is walked.
  Real books are dense and narrow (a measured realistic workload used 74
  price levels spanning 84 ticks) so the fallback is essentially never hit,
  but it is there so that a legitimate far-away order is never rejected.

### 7. Events, and the three places they go

The engine's only output is a stream of **events**, delivered synchronously
through a callback (`EventSink`) as they are produced. One command usually
produces several: a crossing new order emits an `OrderAccepted`, then a
`TradeExecuted` per fill, then a `BookOrderReduced` or `BookOrderRemoved` for
each resting order it consumed, and finally a `BookOrderAdded` if any
remainder rested.

The event types split into two groups, and the split is enforced by the type
system rather than by a filter:

- **Private, account-addressed**: `OrderAccepted`, `OrderRejected`,
  `OrderCancelled`, `OrderReplaced`, `TradeExecuted`. These carry
  `account_id` and `client_order_id`.
- **Public, anonymous**: `BookOrderAdded`, `BookOrderReduced`,
  `BookOrderRemoved`. These carry an exchange order id and nothing about
  whose order it is — exactly what an observer of the public feed can see.

Keeping that distinction in the types is what lets the market-data publisher
forward the public events without a filtering step that could leak private
data by omission. The one event that appears in both worlds is
`TradeExecuted`, and the publisher strips the counterparty accounts off it
before it goes out.

Three consumers subscribe to this stream, all running on the matching thread:

1. **The ledger**, which settles cash and positions.
2. **The gateway's router**, which turns private events into wire responses.
3. **The market-data publisher**, which turns public events into feed
   messages.

Because all three run on the matching thread, none of them may block. That
constraint is what shapes the next step.

### 8. Getting the reply back to the right client

The router resolves each report to a session (the ownership rules from step
1) and pushes it onto **that connection's own outbound SPSC queue**. It does
not write to the socket.

This is the single most important structural decision in the gateway. If
`route_event()` called `write()` directly, the matching thread — the one
thread the entire exchange's throughput depends on — would block on the TCP
send buffer of whichever client happened to be slowest. One client on a bad
connection would slow down matching for everybody. Instead the matching
thread does a non-blocking push and moves on, and each connection's own
writer thread drains its queue at whatever pace that client can accept.

If a connection's outbound queue fills, its reports are dropped. That client
has fallen catastrophically behind, and the alternative is stalling the
exchange.

The writer thread blocks on a condition variable rather than polling. This is
not a stylistic preference: an earlier version slept a fixed interval when it
found the queue empty, and that sleep *dominated the measured end-to-end
latency* of the whole system. It was the single largest improvement in the
project's latency history.

---

## Market data: the public broadcast

The publisher (`exchange/market_data/`) translates public exchange events
into the same wire format the trader-side book already speaks:

| exchange event | feed message |
|---|---|
| `BookOrderAdded` | `AddOrder` |
| `BookOrderReduced` | `ModifyOrder` |
| `BookOrderRemoved` | `CancelOrder` |
| `TradeExecuted` | `Trade` (accounts stripped) |

It stamps each message with its own monotonic sequence number and a
timestamp. Both are genuinely new: the exchange's internal event sequence and
command sequence both exist for other purposes, and neither means "position
in the outbound feed." The timestamp is a deliberate, narrow exception to
this codebase's rule against wall-clock time in deterministic code — the
matching engine never reads a clock, but the moment a fact left the building
is exactly what a feed timestamp is for.

Messages go out over UDP, which means a receiver may miss some. The receiving
side (`replay/`, `book/`, `common/sequence_validator.hpp`) handles that:

- **`SequenceValidator`** classifies every arriving message as in-order,
  duplicate, out-of-order, or missing, by comparing sequence numbers. This
  is the correctness gate.
- **Packet-level tracking** (`net::PacketSequenceTracker`) watches the
  datagram sequence separately, but purely as a statistic. UDP packets can
  legitimately arrive out of order for reasons that say nothing about data
  correctness.
- **Recovery** on a detected gap loads a **snapshot** — a saved
  point-in-time picture of the book — and resumes from there. The snapshot
  format reuses the `AddOrder` frame as-is, so no new per-order encoding was
  needed for it.

One limitation is worth stating plainly because it is inherent rather than a
bug: a gap is only detected *retrospectively*, when a later message arrives.
If the last few messages of a burst are lost and nothing arrives after them,
nothing downstream can know. There is no real gap-fill or retransmission
service here; recovery is snapshot-only.

The same UDP path can be driven without an exchange at all, from a generated
file, which is how the market-data half of this project is tested in
isolation:

```bash
./build/feed_generator --output events.bin --orders 100000 --seed 42
./build/market_data_replay --listen 9000 --top-levels 5 &
./build/udp_sender --input events.bin --host 127.0.0.1 --port 9000
```

The listener runs two threads — a producer that receives and decodes, and a
consumer that validates and applies to the book — connected by a
`DroppingQueue`. This is where dropping *is* the right policy: a live receive
loop must never stall waiting for a slow consumer, because not reading the
socket just moves the drop into the kernel where you cannot see it. Dropping
here is counted and visible.

---

## The trader side

`trader/` is the other half of the story: the code a participant runs.

- **`OrderEntryClient`** owns the TCP socket to the gateway.
- **`OrderManagementSystem`** turns "buy 100 at 99.50" into wire messages and
  turns responses back into a per-order state machine, so a strategy can ask
  what happened to its order. It knows nothing about sockets — the transport
  is injected as a pair of function-shaped seams, which is what makes it
  unit-testable against a fake.
- **`TraderRiskEngine` / `TraderRiskGatedOms`** apply the *client's own*
  limits before anything is sent. This is separate from the exchange's risk
  check and exists for a different reason: the exchange protects itself, the
  trader protects itself from its own strategy.
- **`PositionTracker`** maintains what the trader believes it owns, and
  **`PnlTracker`** — a second, independent consumer of the same fill stream —
  maintains how its trading has *done*. Two classes rather than one because
  they answer different questions and cannot share a representation: holdings
  are an unsigned quantity that seeded inventory contributes to, whereas a
  trading position is signed, starts at zero, and ignores seeding.
- **`FeedSubscriber`** subscribes to the live UDP feed and drives a
  `StrategyRuntime` from it, reusing the same sequence-validation and
  book-application code file replay uses.
- **Strategies**: `MarketMakerStrategy` quotes both sides of the book and
  earns the spread; `CrossVenueArbitrageStrategy` watches two books for a
  price discrepancy; `LadderMarketMaker` and `MomentumStrategy` are the two
  simulated participants below.

The trader keeps its own copy of the order book (`book/`), built from the UDP
feed. It is a genuinely different class from the exchange's book and cannot
share one, for a reason that is easy to miss: the exchange's book is
authoritative and contains orders it must be able to *match and mutate*,
while the trader's book is a reconstruction from a public feed that shows
anonymous depth and may be stale or gapped. They have different invariants,
different keys, and different failure modes. `docs/end_to_end_architecture.md`
covers this in more detail.

---

## Simulated participants

`apps/market_simulator` makes the exchange visibly trade by itself. Two
participants connect to a running `trading_server` as ordinary external
traders and trade against each other:

- a **market maker** quoting a two-sided ladder around a seeded random walk,
  so the book is continuously tradeable and keeps moving;
- a **momentum strategy** that buys when the midpoint it reconstructs from the
  UDP feed has risen over a short window, and sells when it has fallen.

Both are deliberately simple. The objective is plausible liquidity and a
visible, working system, not alpha.

What makes this worth having is what it *refuses* to do. The simulator holds no
`MatchingEngine`, no `MatchingBook`, no `RiskEngine` and no `Ledger` — it
cannot, since it links only the trader-side components and its entire contact
with the exchange is two sockets. So every order genuinely travels the whole
path: trader-side risk, OMS, encode, TCP, gateway, sequencer, exchange risk and
ledger, matching engine, and then back out both as a private execution report
over TCP and as public market data over UDP, which is the momentum strategy's
only input. That exercises the entire exchange rather than calling the matcher
in a loop.

Each participant is its own account on its own session with its own OMS,
positions, risk limits and P&L. A run prints a periodic status block and a
shutdown summary; `--seed` determines the reference-price path exactly, so runs
are reproducible even though which quotes fill is not.

See **[docs/market_simulation.md](docs/market_simulation.md)** for the design,
the P&L accounting, and a real run with its numbers explained.

---

## The dashboard

`ui_gateway/` exposes a running exchange to a browser: a REST API for
snapshots and actions, and **Server-Sent Events** for the live stream. SSE
rather than WebSockets because the traffic is entirely one-directional
(server pushes updates, browser sends orders over plain REST), and SSE is a
long-lived HTTP response with automatic client reconnection built in.

It holds one OMS per pre-seeded demo account, each connected to the gateway
over real TCP exactly as an external strategy would be — the dashboard is a
client of the exchange, not a privileged window into it. The frontend is a
React + Vite + TypeScript app in `ui/`.

---

## Determinism, journaling, and replay

The exchange core is deterministic by construction: single-threaded matching,
no wall-clock reads, no hash-order iteration, no randomness. Given the same
command sequence it produces byte-identical events. Every event type defines
`operator==` specifically so a test can assert that by comparing two
`std::vector<ExchangeEvent>`s directly.

`exchange/persistence/` makes that useful. `CommandJournalWriter` records
every command to a binary file, and `run_command_replay()` feeds them back
into a fresh engine. The journal opens with one `RegisterInstrument` frame per
instrument, so the file describes the engine that can replay it — without
that, a journal would replay into an engine that rejects every command in it
and produce an empty book that looks fine.

Commands, not events, are journaled — the same choice a real exchange makes.
Commands are the inputs; replaying them re-derives every event, which also
proves the engine is still deterministic. `state_hash.hpp` reduces a whole
engine state to one value, so a replay can be verified against the original
in a single comparison.

---

## Why these networking and queueing choices

Collected in one place, since they are scattered through the walkthrough:

| decision | reason |
|---|---|
| **TCP for order entry** | losing an order is unacceptable; a client needs a reliable, ordered, private session |
| **UDP for market data** | one-to-many broadcast; a slow receiver must not delay everyone else; a missed message is recoverable from sequence numbers |
| **No sequence number on order entry** | TCP already guarantees what it would detect |
| **3-byte order-entry header** | TCP is a byte stream, so framing is the header's only job |
| **20-byte market-data header** | UDP gives framing for free but needs a sequence number and timestamp |
| **Big-endian, explicit shifts** | struct layout is not a wire contract; shifts work on any host |
| **Fixed-size payloads** | size known at compile time, so validation and decode are a few loads |
| **`TCP_NODELAY` on** | Nagle's algorithm batches small writes to save bandwidth, which is exactly wrong for latency |
| **MPSC inbound command queue** | every connection reader may `submit()` concurrently; FIFO is CAS admission order, not a mutex |
| **SPSC outbound per connection** | matching thread is the sole producer for that writer's queue |
| **Cache-line-padded indices** | prevents false sharing between producer and consumer |
| **Drop on a full market-data queue** | a stalled receive loop drops in the kernel instead, invisibly |
| **Reject on a full command queue** | a silently dropped order is indistinguishable to the client from one never sent |
| **Per-connection outbound queues** | one slow client must not block the matching thread |
| **Condition variable, not polling** | the poll interval was the largest single component of measured latency |
| **One matching thread** | determinism, and no locks on the hot path |
| **`std::jthread` + `stop_source`** | joins automatically on destruction; one stop signal both threads observe |

---

## Performance

All numbers below are measured on the development machine (Apple Silicon,
macOS), with the raw output committed in `bench-results/`. They are honest
about their limits: a laptop under a scheduler is not a tuned server, and
these are relative improvements, not absolute claims.

Four rounds of optimization on the matching engine, in order: an instrument
registry replacing a hash lookup, a slab allocator with intrusive FIFO
levels, a tick ladder with an occupancy bitmap, and a struct-layout pass.

| | before | after | change |
|---|---|---|---|
| mixed realistic workload | 134.0 ns/op | 80.3 ns/op | −40% |
| cancel against 4096 price levels | 84.6 ns | 64.1 ns | −24% |
| FOK rejected against 1024 crossable levels | 45,088 ns | 5,255 ns | −88% |
| memory, 1M resting orders | 345.9 B/order | 207.8 B/order | −40% |
| allocations for a 1M-order book | 815 | 563 | −31% |

The end-to-end picture is the more instructive one. Measured over a real
loopback TCP round trip, against a bare echo server used as a transport
floor:

| | p50 | p90 | p99 |
|---|---|---|---|
| fully-wired gateway | 69.8 µs | 78.7 µs | 147.5 µs |
| transport floor (echo server, same bytes) | 47.8 µs | 53.5 µs | 87.0 µs |
| **what this codebase adds** | **22.0 µs** | **25.2 µs** | **60.4 µs** |

Two thirds of the round trip is loopback TCP and OS scheduling, which no
amount of application-level work can remove. Against that, the matching
engine's ~80 ns is roughly **0.1% of the round trip** — a 40% improvement in
it is invisible end to end, and a sampling profile confirms the time goes to
scheduler wakeups, not to matching.

That is the honest conclusion, and it is the most useful thing in the
benchmark story: the engine work is real and measurable in isolation, and it
is not what a client feels. Knowing which of those is true, and being able to
prove it, is the point.

Full methodology, per-stage before/after tables, the reasoning behind each
data structure, and what was tried and rejected:
[`docs/matching_engine_optimisation.md`](docs/matching_engine_optimisation.md)
and [`docs/benchmarks.md`](docs/benchmarks.md).

---

## Repository layout

```
include/          public headers, mirroring src/
  common/          shared types, endian-safe byte I/O, SequenceValidator,
                   SpscQueue, DroppingQueue
  protocol/        market-data wire format (ITCH-like)
    order_entry/   order-entry wire format (OUCH-like)
  net/             TCP and UDP sockets, packet framing, batched receive
  book/            trader-side reconstructed order book
  replay/          event file I/O, replay engine, snapshots
  exchange/
    core/          commands, events, the EventSink boundary
    matching/      the matching engine and its book
    risk/          pre-trade risk checks
    ledger/        cash and position balances
    sequencing/    command sequencer and the matching pipeline
    gateway/       the TCP order-entry gateway and session model
    market_data/   event-to-feed publisher
    persistence/   command journal, replay, state hashing
    testing/       shared workload and scenario generators
  trader/          OMS, trader-side risk, positions, strategies
  ui_gateway/      REST + SSE front door
src/              implementations
apps/
  trading_server/      the full stack in one process
  feed_generator/      deterministic synthetic feed file
  udp_sender/          streams a feed file over UDP
  market_data_replay/  replays a file or listens on UDP
  live_strategy_demo/  a real strategy against a running trading_server
  market_simulator/    two simulated participants trading a running
                       trading_server over TCP + UDP
ui/               React + Vite + TypeScript dashboard
benchmarks/       microbenchmarks + staged and transport-floor TCP harnesses
tests/            GoogleTest suite (483 tests)
bench-results/    raw benchmark output, committed
docs/             see below
```

---

## Building and running

Requirements: CMake ≥ 3.20 and a C++20 compiler (developed with Apple Clang
on arm64; the networking is plain POSIX sockets, so Linux works too).
GoogleTest and Google Benchmark are fetched automatically.

```bash
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Sanitizer builds:

```bash
cmake -S . -B build-asan -DMDH_ENABLE_ASAN=ON -DMDH_ENABLE_UBSAN=ON
cmake -S . -B build-tsan -DMDH_ENABLE_TSAN=ON   # separate: can't combine with ASan
```

Everything compiles with `-Wall -Wextra -Wpedantic -Wshadow -Wconversion`
and the build is warning-clean.

**Run the whole stack:**

```bash
# --market-data-port may be repeated: the first is the UI gateway's, and every
# event is published to all of them.
./build/trading_server --tcp-port 7000 \
    --market-data-port 7001 --market-data-port 7002 --http-port 8080
# then, in another terminal, watch the exchange trade with itself:
./build/market_simulator --tcp-port 7000 --market-data-port 7002 --seed 42
# and open http://localhost:8080 for the dashboard
```

`market_simulator` runs a simulated market maker and a momentum strategy as two
ordinary external traders, over the same TCP and UDP interfaces any client uses --
see [docs/market_simulation.md](docs/market_simulation.md). For a single
market-making strategy driven off the dashboard's REST book instead,
`./build/live_strategy_demo`.

**Run the market-data half on its own:**

```bash
./build/feed_generator --output events.bin --orders 100000 --seed 42
./build/market_data_replay --input events.bin --top-levels 5
```

**Force backpressure on demand** — a tiny queue plus an artificially slowed
consumer guarantees drops rather than hoping a workload happens to trigger
them:

```bash
./build/market_data_replay --listen 9001 --queue-capacity 8 --consumer-delay-us 5000 &
./build/udp_sender --input events.bin --host 127.0.0.1 --port 9001
```

**Benchmarks:**

```bash
./build/bench_matching_engine
./build/bench_matching_workload
./build/bench_end_to_end_latency    # real TCP round trip vs. transport floor
./build/bench_order_path_latency    # staged latency/load/syscall measurements
```

---

## What is real and what is simulated

**Real:** the concurrency model, the socket code (genuine loopback TCP and
UDP in the tests, not mocks), the wire encoding, price-time priority
matching, the reservation semantics, the determinism guarantee, and every
benchmark number.

**Simulated or out of scope**, deliberately:

- One process, one machine, loopback only. No multicast, no NIC tuning, no
  kernel bypass, no CPU pinning. Market data is fanned out by sending a copy
  of each datagram to every configured port, which is what lets the dashboard
  and `market_simulator` both subscribe; a real venue would use multicast.
- The simulated participants in `market_simulator` trade only with each other,
  so their P&L is zero-sum by construction, and both run in one process
  (properly separated, but not adversarially).
- No authentication, no TLS, no credentials. Session-to-account binding is
  by assertion.
- No clearing, settlement, margin, or corporate actions. The ledger is cash
  and positions.
- Risk is a short list of credible checks, not a venue's real stack — no
  margin, no self-trade prevention, no kill switch.
- No market orders, hidden orders, iceberg orders, auctions, circuit
  breakers, or trading halts. Limit orders with GTC/IOC/FOK only.
- No gap-fill or retransmission service; recovery is snapshot-only, and a
  drop with nothing arriving after it is undetectable by construction.
- Replace always loses time priority, even for a quantity-only decrease. A
  real venue usually preserves it in that case.
- Outbound reports for a disconnected account are retained in memory only,
  bounded, and lost on restart.
- The trader-side book has no matching engine, so a reconstructed book can
  show a crossed spread if the feed's orders happen to cross.

---

## Further reading

| document | what's in it |
|---|---|
| [`docs/exchange_flow.md`](docs/exchange_flow.md) | code-level walkthrough of the exchange side, with a worked order trace |
| [`docs/end_to_end_architecture.md`](docs/end_to_end_architecture.md) | the full system shape, and why the two order books can't share a class |
| [`docs/protocol.md`](docs/protocol.md) | complete wire spec: byte offsets, error taxonomy, packet and snapshot layout |
| [`docs/matching_engine_optimisation.md`](docs/matching_engine_optimisation.md) | the four optimization rounds, what was measured, and what was rejected |
| [`docs/benchmarks.md`](docs/benchmarks.md) | benchmark methodology and interpretation |
| [`docs/failure_injection.md`](docs/failure_injection.md) | the fault matrix against the live TCP gateway and UDP listener |
| [`docs/live_demo.md`](docs/live_demo.md) | one real end-to-end run, with a dashboard screenshot |
| [`docs/market_simulation.md`](docs/market_simulation.md) | the simulated market maker and momentum strategy, and a real run of them |
| [`ui/README.md`](ui/README.md) | the React dashboard |
