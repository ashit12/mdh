#include <gtest/gtest.h>

#include <cstdio>
#include <string>

#include "exchange/market_data/market_data_publisher.hpp"
#include "exchange/matching/matching_engine.hpp"
#include "replay/event_file_writer.hpp"
#include "replay/replay_engine.hpp"

// The Milestone 6 loop-closing test: proves the exchange side and the
// trader side actually agree, not just that MarketDataPublisher's
// translation is correct in isolation (see test_market_data_publisher.cpp
// for that). Commands are processed by a real MatchingEngine, its events
// are translated by MarketDataPublisher and written with the trader side's
// OWN, completely unmodified EventFileWriter, then read back through the
// trader side's OWN, unmodified replay::run_replay(). The reconstructed
// book::BookManager is then asserted to agree with the authoritative
// MatchingEngine's own snapshot() -- e.g. an order that both sides believe
// is resting at the same reduced quantity after a partial fill, or a book
// that both sides believe is now completely empty after a cancel.
//
// Nothing about EventFileWriter, replay::run_replay(), or book::BookManager
// changed to make this pass -- only MarketDataPublisher (this milestone)
// is new. That is exactly the point: it's a drop-in producer of the same
// wire format feed_generator has always produced synthetically.
using namespace mdh;
using namespace mdh::protocol;
using namespace mdh::replay;
using namespace mdh::exchange;
using namespace mdh::exchange::market_data;

namespace {

class TempFile {
public:
    explicit TempFile(std::string name) : path_(std::move(name)) {}
    ~TempFile() { std::remove(path_.c_str()); }
    [[nodiscard]] const std::string& path() const { return path_; }

private:
    std::string path_;
};

constexpr InstrumentId kInstrument = 7;
constexpr AccountId kBuyer = 1;
constexpr AccountId kSeller = 2;

ExchangeCommand new_order(CommandSequence seq, AccountId account, ClientOrderId client_id, Side side, Price price,
                           Quantity qty) {
    return ExchangeCommand{NewOrderCommand{.command_sequence = seq,
                                            .account_id = account,
                                            .client_order_id = client_id,
                                            .instrument_id = kInstrument,
                                            .side = side,
                                            .price = price,
                                            .quantity = qty,
                                            .order_type = OrderType::Limit,
                                            .time_in_force = TimeInForce::GTC}};
}

} // namespace

TEST(MarketDataE2e, MatchingEngineOutputReplaysIntoAnAgreeingReconstructedBook) {
    TempFile tmp("mdh_test_market_data_e2e_cancel.bin");

    MatchingEngine engine{kInstrument};
    MarketDataPublisher publisher;
    {
        EventFileWriter writer(tmp.path());
        ASSERT_TRUE(writer.is_open());
        const EventSink sink = publisher.sink([&](const Event& wire_event) { writer.write(wire_event); });

        // 1) Buy 10 @ 100, GTC -> rests in full (private OrderAccepted +
        //    public BookOrderAdded).
        engine.process(new_order(1, kBuyer, /*client_id=*/1, Side::Buy, 100, 10), sink);

        // 2) Sell 6 @ 100, GTC -> crosses fully against (1): one trade,
        //    order (1)'s resting remainder reduced to 4, not removed
        //    (private OrderAccepted + public TradeExecuted + BookOrderReduced).
        engine.process(new_order(2, kSeller, /*client_id=*/1, Side::Sell, 100, 6), sink);

        // 3) Cancel the buyer's now-partially-filled order (1) -> removed
        //    entirely; nothing is left resting on this instrument (private
        //    OrderCancelled + public BookOrderRemoved).
        engine.process(ExchangeCommand{CancelOrderCommand{.command_sequence = 3,
                                                           .account_id = kBuyer,
                                                           .client_order_id = 1,
                                                           .instrument_id = kInstrument}},
                       sink);
    } // writer's destructor closes/flushes the file before it's replayed below

    // The authoritative side's own view of final state.
    const EngineStateSnapshot authoritative = engine.snapshot();
    EXPECT_TRUE(authoritative.instruments.empty()); // every resting order on this instrument is gone

    // The trader side's reconstruction, built ENTIRELY from the wire bytes
    // MarketDataPublisher produced, via the existing, unmodified replay
    // pipeline.
    const auto outcome = run_replay(tmp.path());

    EXPECT_FALSE(outcome.stopped_early) << outcome.stop_reason;
    EXPECT_EQ(outcome.stats.decode_failures, 0u);
    EXPECT_EQ(outcome.stats.sequence_failures, 0u);
    // AddOrder, Trade, ModifyOrder, CancelOrder -- exactly one wire message
    // per publishable ExchangeEvent; none at all for the three private ones
    // (two OrderAccepted, one OrderCancelled) this scenario also produced.
    EXPECT_EQ(outcome.stats.messages_processed, 4u);
    EXPECT_EQ(outcome.stats.adds, 1u);
    EXPECT_EQ(outcome.stats.trades, 1u);
    EXPECT_EQ(outcome.stats.modifies, 1u);
    EXPECT_EQ(outcome.stats.cancels, 1u);

    const auto* book = outcome.books.find_book(kInstrument);
    ASSERT_NE(book, nullptr);
    EXPECT_FALSE(book->best_bid().has_value());
    EXPECT_FALSE(book->best_ask().has_value());

    const auto* trade_stats = outcome.books.trade_stats(kInstrument);
    ASSERT_NE(trade_stats, nullptr);
    EXPECT_EQ(trade_stats->trade_count, 1u);
    EXPECT_EQ(trade_stats->traded_quantity, 6u);
    EXPECT_EQ(trade_stats->last_trade_price, 100);
}

TEST(MarketDataE2e, PartiallyFilledRestingOrderSurvivesTheRoundTripAtItsReducedQuantity) {
    TempFile tmp("mdh_test_market_data_e2e_partial_fill.bin");

    MatchingEngine engine{kInstrument};
    MarketDataPublisher publisher;
    {
        EventFileWriter writer(tmp.path());
        const EventSink sink = publisher.sink([&](const Event& wire_event) { writer.write(wire_event); });

        engine.process(new_order(1, kBuyer, /*client_id=*/1, Side::Buy, 100, 10), sink);
        engine.process(new_order(2, kSeller, /*client_id=*/1, Side::Sell, 100, 6), sink);
        // Order (1) is deliberately left resting at 4 remaining -- no cancel this time.
    }

    const EngineStateSnapshot authoritative = engine.snapshot();
    ASSERT_EQ(authoritative.instruments.size(), 1u);
    ASSERT_EQ(authoritative.instruments[0].bids.size(), 1u);
    EXPECT_EQ(authoritative.instruments[0].bids[0].remaining_quantity, 4u);

    const auto outcome = run_replay(tmp.path());
    EXPECT_FALSE(outcome.stopped_early) << outcome.stop_reason;

    const auto* book = outcome.books.find_book(kInstrument);
    ASSERT_NE(book, nullptr);
    const auto bid = book->best_bid();
    ASSERT_TRUE(bid.has_value());
    EXPECT_EQ(bid->price, 100);
    EXPECT_EQ(bid->aggregate_quantity, 4u); // agrees with the authoritative side's remaining_quantity
}
