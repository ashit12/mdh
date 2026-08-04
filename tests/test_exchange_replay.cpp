#include <gtest/gtest.h>

#include <cstdio>
#include <string>

#include "exchange/persistence/command_journal_writer.hpp"
#include "exchange/persistence/command_replay.hpp"
#include "exchange/persistence/state_hash.hpp"

using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::persistence;

namespace {

class TempFile {
public:
    explicit TempFile(std::string name) : path_(std::move(name)) {}
    ~TempFile() { std::remove(path_.c_str()); }
    [[nodiscard]] const std::string& path() const { return path_; }

private:
    std::string path_;
};

// Writes a journal exercising every command type and a mix of resting,
// crossing, partial-fill, cancel, and replace behavior across two
// instruments -- enough variety that a nondeterministic bug (e.g.
// accidentally iterating an unordered_map without sorting) would show up as
// a mismatch between two replays of the same file.
void write_mixed_journal(const std::string& path) {
    CommandJournalWriter writer(path);
    ASSERT_TRUE(writer.is_open());

    auto new_order = [](CommandSequence seq, AccountId account, ClientOrderId client_id, InstrumentId instrument,
                         Side side, Price price, Quantity qty, TimeInForce tif = TimeInForce::GTC) {
        return ExchangeCommand{NewOrderCommand{.command_sequence = seq,
                                                .account_id = account,
                                                .client_order_id = client_id,
                                                .instrument_id = instrument,
                                                .side = side,
                                                .price = price,
                                                .quantity = qty,
                                                .order_type = OrderType::Limit,
                                                .time_in_force = tif}};
    };

    writer.write(new_order(1, 100, 1, 1, Side::Buy, 99, 5));
    writer.write(new_order(2, 100, 2, 1, Side::Buy, 100, 5));
    writer.write(new_order(3, 200, 3, 1, Side::Sell, 105, 5));
    writer.write(new_order(4, 300, 4, 2, Side::Buy, 50, 10));
    writer.write(ExchangeCommand{
        CancelOrderCommand{.command_sequence = 5, .account_id = 100, .client_order_id = 1, .instrument_id = 1}});
    writer.write(new_order(6, 400, 5, 1, Side::Sell, 100, 3)); // partially fills order 2
    writer.write(ExchangeCommand{ReplaceOrderCommand{.command_sequence = 7,
                                                      .account_id = 100,
                                                      .original_client_order_id = 2,
                                                      .new_client_order_id = 6,
                                                      .instrument_id = 1,
                                                      .new_price = 100,
                                                      .new_quantity = 1}});
    writer.write(new_order(8, 500, 7, 2, Side::Sell, 50, 10, TimeInForce::IOC)); // crosses order 4 fully
    writer.write(new_order(9, 600, 8, 1, Side::Sell, 200, 1, TimeInForce::FOK)); // rejected: insufficient liquidity
}

} // namespace

TEST(ExchangeReplay, SameJournalProducesIdenticalEventsAcrossTwoRuns) {
    TempFile tmp("mdh_test_exchange_replay_events.bin");
    write_mixed_journal(tmp.path());

    CommandReplayOutcome run1 = run_command_replay(tmp.path());
    CommandReplayOutcome run2 = run_command_replay(tmp.path());

    ASSERT_FALSE(run1.stopped_early) << run1.stop_reason;
    ASSERT_FALSE(run2.stopped_early) << run2.stop_reason;
    EXPECT_EQ(run1.commands_processed, 9u);
    EXPECT_EQ(run1.commands_processed, run2.commands_processed);

    // The core requirement: byte-for-byte identical event streams from two
    // independent replays of the same journal.
    ASSERT_EQ(run1.events.size(), run2.events.size());
    EXPECT_EQ(run1.events, run2.events);
}

TEST(ExchangeReplay, SameJournalProducesIdenticalFinalStateAcrossTwoRuns) {
    TempFile tmp("mdh_test_exchange_replay_state.bin");
    write_mixed_journal(tmp.path());

    CommandReplayOutcome run1 = run_command_replay(tmp.path());
    CommandReplayOutcome run2 = run_command_replay(tmp.path());

    const auto snapshot1 = run1.engine.snapshot();
    const auto snapshot2 = run2.engine.snapshot();

    // Direct structural equality of the canonical state dump...
    EXPECT_EQ(snapshot1, snapshot2);
    // ...and the single-value hash derived from it, both proving the same
    // thing two different ways (the milestone allows either).
    EXPECT_EQ(hash_state_snapshot(snapshot1), hash_state_snapshot(snapshot2));

    // Sanity: the final state isn't trivially empty (i.e. this is actually
    // exercising the matcher, not just proving two empty books are equal).
    bool any_resting_orders = false;
    for (const auto& instrument : snapshot1.instruments) {
        any_resting_orders = any_resting_orders || !instrument.bids.empty() || !instrument.asks.empty();
    }
    EXPECT_TRUE(any_resting_orders);
}

TEST(ExchangeReplay, FinalStateMatchesHandComputedExpectation) {
    // Cross-checks the replay driver against manual bookkeeping of
    // write_mixed_journal()'s effects, so the determinism tests above are
    // not just "equal to itself" but "equal to the *correct* answer":
    //   - order 1 (buy 99x5) cancelled -> gone.
    //   - order 2 (buy 100x5) partially filled 3 by order 5 -> remaining 2,
    //     then replaced (qty decrease, same price) to remaining 1, exchange
    //     order id unchanged, still resting.
    //   - order 3 (sell 105x5) untouched, still resting.
    //   - order 4 (buy 50x10 on instrument 2) fully consumed by the IOC sell
    //     at command 8 -> gone; that IOC's own remainder is discarded, not
    //     resting.
    //   - command 9 (FOK sell 200x1 on instrument 1) rejected outright.
    TempFile tmp("mdh_test_exchange_replay_expected.bin");
    write_mixed_journal(tmp.path());

    CommandReplayOutcome run = run_command_replay(tmp.path());
    ASSERT_FALSE(run.stopped_early) << run.stop_reason;

    const auto snapshot = run.engine.snapshot();
    ASSERT_EQ(snapshot.instruments.size(), 1u); // instrument 2's only order was fully consumed, no resting book left
    const auto& instrument1 = snapshot.instruments[0];
    EXPECT_EQ(instrument1.instrument_id, 1u);

    ASSERT_EQ(instrument1.bids.size(), 1u);
    EXPECT_EQ(instrument1.bids[0].client_order_id, 6u); // order 2's replacement id
    EXPECT_EQ(instrument1.bids[0].remaining_quantity, 1u);
    EXPECT_EQ(instrument1.bids[0].price, 100);

    ASSERT_EQ(instrument1.asks.size(), 1u);
    EXPECT_EQ(instrument1.asks[0].client_order_id, 3u); // original resting sell, untouched
    EXPECT_EQ(instrument1.asks[0].remaining_quantity, 5u);
}
