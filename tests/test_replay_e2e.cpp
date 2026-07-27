#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>

#include "replay/event_file_writer.hpp"
#include "replay/replay_engine.hpp"

using namespace mdh;
using namespace mdh::protocol;
using namespace mdh::replay;

namespace {

class TempFile {
public:
    explicit TempFile(std::string name) : path_(std::move(name)) {}
    ~TempFile() { std::remove(path_.c_str()); }
    [[nodiscard]] const std::string& path() const { return path_; }

private:
    std::string path_;
};

void write_valid_multi_instrument_feed(const std::string& path) {
    EventFileWriter w(path);
    w.write(Event{AddOrder{.sequence_number = 1, .timestamp_ns = 100, .order_id = 1, .instrument_id = 1, .price = 1000, .quantity = 10, .side = Side::Buy}});
    w.write(Event{AddOrder{.sequence_number = 2, .timestamp_ns = 200, .order_id = 2, .instrument_id = 1, .price = 1010, .quantity = 5, .side = Side::Sell}});
    w.write(Event{AddOrder{.sequence_number = 3, .timestamp_ns = 300, .order_id = 3, .instrument_id = 2, .price = 5000, .quantity = 2, .side = Side::Buy}});
    w.write(Event{ModifyOrder{.sequence_number = 4, .timestamp_ns = 400, .order_id = 1, .instrument_id = 1, .new_price = 995, .new_quantity = 8}});
    w.write(Event{Trade{.sequence_number = 5, .timestamp_ns = 500, .instrument_id = 1, .price = 1000, .quantity = 3, .aggressor_side = Side::Buy}});
    w.write(Event{CancelOrder{.sequence_number = 6, .timestamp_ns = 600, .order_id = 2, .instrument_id = 1}});
    w.write(Event{ClearBook{.sequence_number = 7, .timestamp_ns = 700, .instrument_id = 2}});
}

} // namespace

TEST(ReplayEngine, DeterministicAcrossRepeatedRuns) {
    TempFile tmp("mdh_test_replay_deterministic.bin");
    write_valid_multi_instrument_feed(tmp.path());

    auto outcome1 = run_replay(tmp.path());
    auto outcome2 = run_replay(tmp.path());

    EXPECT_EQ(outcome1.stats.messages_processed, outcome2.stats.messages_processed);
    EXPECT_EQ(outcome1.stats.adds, outcome2.stats.adds);
    EXPECT_EQ(outcome1.stats.cancels, outcome2.stats.cancels);
    EXPECT_EQ(outcome1.stats.modifies, outcome2.stats.modifies);
    EXPECT_EQ(outcome1.stats.trades, outcome2.stats.trades);
    EXPECT_EQ(outcome1.stats.clears, outcome2.stats.clears);
    EXPECT_EQ(outcome1.stats.book_errors, outcome2.stats.book_errors);
    EXPECT_FALSE(outcome1.stopped_early);
    EXPECT_FALSE(outcome2.stopped_early);

    auto bid1 = outcome1.books.find_book(1)->best_bid();
    auto bid2 = outcome2.books.find_book(1)->best_bid();
    ASSERT_TRUE(bid1.has_value());
    ASSERT_TRUE(bid2.has_value());
    EXPECT_EQ(bid1->price, bid2->price);
    EXPECT_EQ(bid1->aggregate_quantity, bid2->aggregate_quantity);
}

TEST(ReplayEngine, ValidFeedProducesExpectedCountsAndBookState) {
    TempFile tmp("mdh_test_replay_counts.bin");
    write_valid_multi_instrument_feed(tmp.path());

    auto outcome = run_replay(tmp.path());

    EXPECT_FALSE(outcome.stopped_early);
    EXPECT_EQ(outcome.stats.messages_processed, 7u);
    EXPECT_EQ(outcome.stats.adds, 3u);
    EXPECT_EQ(outcome.stats.modifies, 1u);
    EXPECT_EQ(outcome.stats.trades, 1u);
    EXPECT_EQ(outcome.stats.cancels, 1u);
    EXPECT_EQ(outcome.stats.clears, 1u);
    EXPECT_EQ(outcome.stats.book_errors, 0u);
    EXPECT_EQ(outcome.stats.decode_failures, 0u);
    EXPECT_EQ(outcome.stats.sequence_failures, 0u);

    // Instrument 1: order 1 modified to price 995/qty 8, order 2 cancelled.
    const auto* book1 = outcome.books.find_book(1);
    ASSERT_NE(book1, nullptr);
    auto bid1 = book1->best_bid();
    ASSERT_TRUE(bid1.has_value());
    EXPECT_EQ(bid1->price, 995);
    EXPECT_FALSE(book1->best_ask().has_value()); // order 2 (the only ask) was cancelled

    // Instrument 2: order 3 was added then the book was cleared.
    const auto* book2 = outcome.books.find_book(2);
    ASSERT_NE(book2, nullptr);
    EXPECT_FALSE(book2->best_bid().has_value());

    const auto* trade_stats = outcome.books.trade_stats(1);
    ASSERT_NE(trade_stats, nullptr);
    EXPECT_EQ(trade_stats->trade_count, 1u);
}

TEST(ReplayEngine, StopsOnFirstSequenceGapByDefault) {
    TempFile tmp("mdh_test_replay_gap.bin");
    {
        EventFileWriter w(tmp.path());
        w.write(Event{AddOrder{.sequence_number = 1, .timestamp_ns = 1, .order_id = 1, .instrument_id = 1, .price = 100, .quantity = 1, .side = Side::Buy}});
        w.write(Event{AddOrder{.sequence_number = 3, .timestamp_ns = 2, .order_id = 2, .instrument_id = 1, .price = 100, .quantity = 1, .side = Side::Buy}}); // gap: seq 2 missing
        w.write(Event{AddOrder{.sequence_number = 4, .timestamp_ns = 3, .order_id = 3, .instrument_id = 1, .price = 100, .quantity = 1, .side = Side::Buy}});
    }

    auto outcome = run_replay(tmp.path());

    EXPECT_TRUE(outcome.stopped_early);
    EXPECT_FALSE(outcome.stop_reason.empty());
    EXPECT_EQ(outcome.stats.sequence_failures, 1u);
    // Only the first (in-order) event was applied before the gap halted replay.
    EXPECT_EQ(outcome.stats.messages_processed, 1u);
}

TEST(ReplayEngine, StopsOnFirstDecodeFailureByDefault) {
    TempFile tmp("mdh_test_replay_decode_fail.bin");
    {
        EventFileWriter w(tmp.path());
        w.write(Event{AddOrder{.sequence_number = 1, .timestamp_ns = 1, .order_id = 1, .instrument_id = 1, .price = 100, .quantity = 1, .side = Side::Buy}});
    }
    // Append one corrupt/truncated frame after the valid one.
    {
        std::ofstream out(tmp.path(), std::ios::binary | std::ios::app);
        char garbage[5] = {1, 0, 0, 0, 0}; // type=1 (AddOrder), but far fewer bytes than a full header
        out.write(garbage, sizeof(garbage));
    }

    auto outcome = run_replay(tmp.path());

    EXPECT_TRUE(outcome.stopped_early);
    EXPECT_EQ(outcome.stats.decode_failures, 1u);
    EXPECT_EQ(outcome.stats.messages_processed, 1u);
}
