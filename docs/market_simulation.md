# Market simulation

**Status:** actually run, end to end, on this machine, on 2026-08-27 -- every number and
log line below is real output from the run in §9, not illustrative -- the same
documentation discipline `docs/benchmarks.md`, `docs/failure_injection.md` and
`docs/live_demo.md` apply to their own claims.

`apps/market_simulator` makes the exchange visibly trade by itself. Two simulated
participants connect to a running `trading_server` as ordinary external traders, over
the ordinary public interfaces, and trade against each other:

- a **market maker** that quotes a two-sided ladder around a seeded random walk, so the
  book is continuously tradeable and keeps moving;
- a **momentum strategy** that buys when the midpoint it reconstructs from the UDP feed
  has risen over a short window, and sells when it has fallen.

Neither of them can see inside the exchange. That is the point of the exercise.

---

## 1. What is actually wired to what

```
                        apps/trading_server
              ┌──────────────────────────────────────┐
              │  OrderEntryGateway        tcp:7100   │
              │        │                             │
              │  MatchingPipeline                    │
              │  risk + ledger                       │
              │        │                             │
              │  MatchingEngine                      │
              │        │                             │
              │  MarketDataPublisher                 │
              │     udp:7101 (UI)  udp:7102 (sim)    │
              └───┬──────────────────────────▲───────┘
                  │                          │
        execution reports (tcp)      market data (udp:7102)
                  │                          │
         ┌────────▼──────────────────────────┴────────┐
         │            apps/market_simulator            │
         │                                             │
         │   LadderMarketMaker        MomentumStrategy  │
         │   account 9002             account 9003      │
         │        │                        │            │
         │   TraderRiskGatedOms      TraderRiskGatedOms │
         │   OrderEntryClient        OrderEntryClient   │
         └──────────────── tcp:7100 ───────────────────┘
```

The two participants are separate accounts on separate TCP sessions, with separate
OMS, position, risk and P&L state. They share only the process they happen to run in
and the market-data socket they both read.

## 2. The path an order takes, and why it is not shortcut

`market_simulator` holds no `MatchingEngine`, no `MatchingBook`, no `RiskEngine` and no
`Ledger`. It cannot: it constructs only trader-side components, and its entire contact
with the exchange is two sockets. Every order therefore travels the full path:

```
strategy
  -> TraderRiskGatedOms          trader-side risk check, position mirror
  -> OrderManagementSystem       assigns a client_order_id, tracks the order
  -> protocol::order_entry       encode
  -> OrderEntryClient -> TCP
     -> OrderEntryGateway        decode, session -> account
     -> MatchingPipeline         sequencing, journal
     -> risk + ledger            exchange-side authorization and settlement
     -> MatchingEngine           match
     -> ExchangeEvent
        ├─> execution report -> TCP -> OrderEntryClient
        │      -> OrderManagementSystem -> PositionTracker + PnlTracker
        └─> MarketDataPublisher -> UDP
               -> FeedSubscriber -> apply_frame_result -> BookManager
                  -> StrategyRuntime -> strategy
```

Both halves of that loop matter. The private half (execution reports over TCP) is how a
participant learns its own fills and computes its own P&L. The public half (market data
over UDP) is the *only* input the momentum strategy has -- it has no other way to know a
price exists, so a trade it makes is proof the whole feed path works.

`tests/test_market_simulator_e2e.cpp` asserts this end to end, including that the
public feed's trade tally and the two participants' privately-reported fills agree.

## 3. What was reused, and what was added

Almost all of this already existed. The new code is the two strategies, the missing
live-feed driver, and the P&L accounting.

**Reused unchanged:** `OrderEntryClient` and `OrderManagementSystem` (order lifecycle
over TCP), `TraderRiskGatedOms` and `TraderRiskEngine` (trader-side risk), `PositionTracker`
(holdings), `protocol::order_entry` codecs, `net::UdpReceiver` / `net::unpack_frames`,
`replay::apply_frame_result` (sequence validation and book application -- the same
function file replay uses, so gap handling behaves identically), `book::BookManager`,
`strategies::StrategyRuntime`, and `OrderEntryGateway` with its `extra_event_sink`.

**Added:**

| Component | Why it did not already exist |
| --- | --- |
| `trader::market_data::FeedSubscriber` | `StrategyRuntime::on_event()` documented that something had to drive it from a live feed, and nothing did. The only live UDP listener was `UiGateway`, which has no strategies, so `live_strategy_demo` polled the UI's REST endpoint instead. |
| `trader::positions::PnlTracker` | `PositionTracker` answers "what does this account hold" with an unsigned `Quantity`, which cannot express a net-short trading position. P&L needs a signed position that starts at zero and ignores seeded inventory. |
| `strategies::RestingQuote` | The per-level submit/hold/replace/cancel/replenish state machine, factored out so the ladder does not duplicate `MarketMakerStrategy`'s order handling. |
| `strategies::ReferencePriceWalk` | The seeded random walk. |
| `strategies::LadderMarketMaker` | `MarketMakerStrategy` quotes around the *book's* midpoint, so with an empty book it never quotes and nothing starts. See §4. |
| `strategies::MomentumStrategy` | Participant 2. |
| `apps/market_simulator` | The executable. |

One change to an existing app: `trading_server --market-data-port` may now be repeated,
and every event is published to all the given ports. Previously the publisher had a
single destination, which is exactly the limitation `docs/live_demo.md` §5 recorded as
the reason `live_strategy_demo` polls REST instead of subscribing to the feed. The UI
gateway and the simulator can now both consume the real feed at once.

## 4. How the market maker quotes

`LadderMarketMaker` is driven by a **timer**, not by market data, and prices from its
**own** reference price, not the book's. Both inversions are deliberate:

- A participant whose job is to *create* a market cannot derive its prices from that
  market. With an empty book there is no midpoint to quote around, so a book-driven
  maker returns without quoting and the market never starts.
- The public feed contains the market maker's own quotes. A maker that centred on the
  book it reads would be centring on its own reflection.

Each cycle it advances the walk one step and recomputes the ladder. With the defaults
and a reference of 100.0000:

```
99.98  bid  x100          100.01  ask  x100
99.99  bid  x100          100.02  ask  x100
```

Then each level is brought toward its desired price by a `RestingQuote`, which:

- submits a new order if it has none;
- **holds** an acknowledged order whose price is within `requote_threshold` of the
  desired price *and* whose remaining quantity is still the full quote size -- keeping
  its queue position;
- **replaces** it otherwise, which both reprices it and replenishes a quote that has
  been partially filled back up to full size;
- **cancels** it when the desired price is `nullopt`, which is how the inventory cap
  withdraws a whole side;
- sends nothing at all while a previous request on that order is still in flight.

Two details worth naming. First, the side moving *away* from the market is updated
first: a reference price that jumped further than the ladder is wide would otherwise
briefly place a new bid through the strategy's own still-resting stale ask, and it would
trade with itself. Second, the inventory cap is symmetric -- at `+max_position` the bid
side is withdrawn entirely and at `-max_position` the ask side is -- because a
participant trading against seeded inventory can genuinely end up net short.

This is intentionally not a good market-making algorithm. There is no adverse-selection
model, no volatility-dependent spread, no order-book-imbalance skew. Plausible liquidity
that exercises the exchange is the whole objective.

## 5. How the momentum strategy works

The entire signal:

```
mid(now) - mid(lookback updates ago)  >  +threshold   ->  BUY
mid(now) - mid(lookback updates ago)  <  -threshold   ->  SELL
```

Its only input is the book `StrategyRuntime` hands it, which `FeedSubscriber` built from
the UDP feed. The window is measured in **book updates**, not wall-clock time, so
behaviour does not depend on how fast the feed happens to be.

Orders are **IOC**, priced `cross_slack` ticks through the far touch. A signal is worth
acting on only against the market that produced it; a GTC order resting at the touch
would sit there while the signal decayed, and would leave the strategy managing resting
orders it has no logic for. The slack covers the book moving in the microseconds between
the feed event and the order reaching the matching engine, and costs nothing when it has
not moved, since the fill happens at the resting order's price.

Two guards are load-bearing rather than decorative. `on_book_update()` runs on every
feed event -- thousands per second on a busy instrument -- and the signal stays above
its threshold for as long as a trend lasts, so without `cooldown_updates` the strategy
would fire one order per event for the whole duration of a move. `max_position` bounds
what one sustained trend can accumulate, and orders are shrunk to fit the remaining room
rather than rejected at the cap, so the cap is approached smoothly instead of bounced
off.

## 6. How P&L is calculated

`PnlTracker` consumes the same OMS fill stream `PositionTracker` does, as a second
independent `FillSink`. Per (account, instrument) it keeps a signed `position`, the
signed `open_notional` of the still-open position, and cumulative `realized`.

It stores an exact **cost basis** rather than an average entry price. Prices are integer
ticks, so an average entry price generally cannot be represented exactly (3 units at
100/101/101 average to 100.667); storing the truncated average and computing realized
P&L against it would let that truncation compound across every partial close. Instead a
close removes a proportional slice of `open_notional`, which makes a full close exact by
construction and leaves at most a sub-tick rounding on a partial one. The average entry
price is derived for display only.

```
realized    accumulated as positions are closed, from the exact basis slice
unrealized  position * mark - open_notional
total       realized + unrealized
mark        the reconstructed book's midpoint, or the last trade price when the
            book is one-sided, or nothing (in which case unrealized is zero)
```

Seeded inventory is deliberately **not** an opening position. It is funding -- what lets
a participant sell before it has bought -- and counting it as a position at a fictional
entry price would make every P&L figure meaningless. So `PnlTracker`'s position starts
at zero and goes negative when a participant has sold more than it bought, even while
`PositionTracker` still reports a large positive holding.

## 7. Reproducibility

`--seed` fully determines the market maker's reference-price path. The walk takes exactly
one draw per quote cycle and is advanced by cycle count rather than by elapsed time, so
cycle *k* has the same price on every run. The desired ladder is a pure function of that
price, and the momentum strategy's decision function is a pure function of the midpoints
it observes.

What is *not* reproducible, and cannot be while orders travel over real sockets, is which
quotes actually fill -- that depends on TCP/UDP interleaving and thread scheduling.
`--steps` bounds a run by quote cycles rather than wall-clock so two runs at one seed
process the same reference-price path, and the summary prints the seed and cycle count so
any run can be re-invoked identically.

Verified on this machine -- two runs, same seed, same final reference price:

```
$ ./market_simulator --tcp-port 7400 --market-data-port 7402 --seed 4242 --steps 30 ...
Seed:              4242
Quote cycles:      30
Final reference:   99.99
-- run 1 done --
Seed:              4242
Quote cycles:      30
Final reference:   99.99
-- run 2 done --
```

`tests/test_market_simulator_e2e.cpp` asserts the stronger form of this over the real
feed: two independent runs of the whole stack at one seed produce an identical sequence
of best-bid prices *as observed on the UDP feed*, and a different seed produces a
different one.

## 8. Running it

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j --target trading_server market_simulator

# The server needs a second market-data port for the simulator: the first is the
# UI gateway's, and every event is published to all of them.
./build-release/trading_server --tcp-port 7100 \
    --market-data-port 7101 --market-data-port 7102 --http-port 8180 &

./build-release/market_simulator --tcp-port 7100 --market-data-port 7102 \
    --seed 42 --steps 200 --quote-interval-ms 50 --status-interval-ms 4000 \
    --momentum-lookback 6 --momentum-threshold 100 \
    --momentum-trade-size 30 --momentum-cooldown 5
```

The server confirms the fan-out and the seeded accounts:

```
order-entry gateway listening on tcp:7100
ui gateway listening on http:8180 (market data on udp:7101)
market data published to udp: 7101 7102
demo accounts: 1001 1002 1003
live-strategy-demo account: 9001 (see apps/live_strategy_demo)
market-simulator accounts: 9002 (market maker) 9003 (momentum strategy) (see apps/market_simulator)
press Ctrl+C to stop
```

Accounts 9002 and 9003 are seeded with cash and inventory by `trading_server` *before*
its gateway starts (`--sim-cash` / `--sim-position`). `market_simulator`'s own
`--seed-cash` / `--seed-position` seed only its local mirrors, which trader-side risk
checks against -- the same documented agreement between two processes that
`live_strategy_demo` already relies on. Because the UI gateway is running too, you can
watch all of this in the dashboard at `http://127.0.0.1:8180/` while it happens.

Every price argument is in `Price` ticks, where one tick is 0.0001: `1000000` is
100.0000 and `100` is 0.0100.

## 9. A real run

Status output, taken mid-run (seed 42, 200 quote cycles at 50 ms):

```
===== MDH MARKET SIMULATION =====

Instrument: 1

Best Bid:  99.91 x 70
Best Ask:  99.93 x 100
Last:      99.91

Trades:        46
Traded qty:    1,380
Orders sent:   257
Feed messages: 510

Market Maker
  Position:        +240
  Avg entry:       99.93
  Realized P&L:    +13.95
  Unrealized P&L:  -2.85
  Total P&L:       +11.10
  Orders sent:     211
  Fills:           46
  Reference:       99.92 (cycle 76)

Momentum Strategy
  Position:        -240
  Avg entry:       99.93
  Realized P&L:    -13.95
  Unrealized P&L:  +2.85
  Total P&L:       -11.10
  Orders sent:     46
  Fills:           46
  Signal:          -0.01 over 508 updates
```

And the shutdown summary from the same run:

```
===== SIMULATION SUMMARY =====

Instrument:        1
Seed:              42
Quote cycles:      200
Final reference:   99.90
Mark price:        99.89

Market (from the UDP feed)
  Trades:          134
  Traded quantity: 4,020
  Best bid:        (none)
  Best ask:        (none)

Feed health
  Packets:         1,446 (0 malformed)
  Messages:        1,446
  Decode failures: 0
  Sequence gaps:   0
  Book errors:     0

Market Maker (account 9002)
  New orders:      4
  Replaces:        585
  Cancels:         4
  Requests total:  593
  Fills:           134
  Traded quantity: 4,020
  Position:        +300
  Avg entry:       99.91
  Realized P&L:    +48.28
  Unrealized P&L:  -5.98
  Total P&L:       +42.30

Momentum Strategy (account 9003)
  New orders:      134
  Replaces:        0
  Cancels:         0
  Requests total:  134
  Fills:           134
  Traded quantity: 4,020
  Position:        -300
  Avg entry:       99.91
  Realized P&L:    -48.28
  Unrealized P&L:  +5.98
  Total P&L:       -42.30
```

Several things in those numbers are worth reading carefully, because each one is a
consistency check that the loop closed properly:

- **The two participants are exact mirrors.** Positions `+300` / `-300`, traded quantity
  4,020 each, and P&L that sums to zero to the tick. They are each other's only
  counterparty, and the two figures are computed independently by two OMS instances from
  two separate TCP execution-report streams.
- **The feed's public trade tally matches the private fill reports.** 134 trades and
  4,020 units on the UDP feed; 134 fills and 4,020 units on each participant's own
  execution reports. The public and private views of the same matching engine agree.
- **The feed was clean.** 1,446 datagrams, zero malformed, zero sequence gaps, zero book
  errors -- so the reconstructed book the momentum strategy traded from was a faithful
  copy of the exchange's.
- **The market maker's request mix is 4 new orders and 585 replaces.** With a quote size
  of 100 against a 30-lot aggressor, fills are partial, so a level stays live and is
  repriced-and-replenished by a replace rather than being replaced by a fresh order.
  The 4 cancels are `withdraw_all()` at shutdown, which is why the final book is empty.
- **The momentum strategy sent 134 orders and got 134 fills**, because every one was an
  IOC priced through the touch against a market maker quoting more size than it asked
  for.
- **The market maker made money and the momentum strategy lost it**, which is the
  expected shape rather than a claim about either: the maker earns the spread on every
  round trip, and the momentum strategy pays it on entry and again on exit. Neither is
  tuned to be profitable, and the market maker's reference price is a random walk with no
  predictable drift to be right about.
- **Realized and unrealized move against each other** between the mid-run status and the
  summary, because the mark changes: mid-run it is the book's midpoint, and after
  `withdraw_all()` empties the book it falls back to the last trade price. Both are
  labelled, and `total = realized + unrealized` in every block.

## 10. Deliberate scope limits

- **The two participants trade only with each other.** There is no third flow, so the
  P&L is zero-sum by construction. That makes it a good consistency check and a poor
  model of a real market.
- **Neither strategy is trying to be good.** Both are deliberately simple enough to read
  in one sitting; see §4 and §5 for the specific modelling each one omits.
- **Fills are not reproducible, only the reference-price path is.** See §7.
- **Both participants run in one process.** They are properly separated -- separate
  accounts, sessions, OMS, risk and P&L state, and neither can see the other's -- but a
  genuinely adversarial test would run them as separate processes. Nothing in the design
  prevents that; `market_simulator` is one binary because it is a demo.
- **Seeding across the two processes is a documented agreement, not an enforced one.**
  `trading_server` seeds the exchange-side balances and `market_simulator` seeds its own
  local mirrors, and the two are only consistent because both default to the same
  accounts. The same pre-existing simplification `live_strategy_demo` relies on.
