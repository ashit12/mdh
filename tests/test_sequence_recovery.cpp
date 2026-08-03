#include <gtest/gtest.h>

#include <cstdio>
#include <string>

#include "book/book_manager.hpp"
#include "replay/event_file_writer.hpp"
#include "replay/replay_engine.hpp"
#include "replay/snapshot.hpp"

using namespace mdh;
using namespace mdh::book;
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

} // namespace

TEST(SequenceRecovery, GapWithConfiguredSnapshotRecoversInsteadOfStopping) {
    TempFile snapshot_file("mdh_test_recovery_snapshot.bin");
    TempFile feed_file("mdh_test_recovery_feed.bin");

    // A snapshot representing some earlier point in time, with a resting
    // order (999) that does NOT appear anywhere in the feed file below --
    // so any trace of it in the final book state can only have come from
    // the snapshot, proving recovery actually loaded it.
    {
        BookManager snapshot_books;
        ASSERT_FALSE(snapshot_books.book_for(1).add_order(999, 70, 3, Side::Buy).has_value());
        ASSERT_TRUE(write_snapshot(snapshot_file.path(), 10, snapshot_books));
    }

    // event 1 establishes the baseline (sequence 1); event 2 jumps to
    // sequence 5, skipping 2,3,4 -- a genuine gap (Missing).
    {
        EventFileWriter w(feed_file.path());
        w.write(Event{AddOrder{.sequence_number = 1, .timestamp_ns = 1, .order_id = 1, .instrument_id = 1, .price = 50, .quantity = 1, .side = Side::Buy}});
        w.write(Event{AddOrder{.sequence_number = 5, .timestamp_ns = 2, .order_id = 2, .instrument_id = 1, .price = 60, .quantity = 2, .side = Side::Sell}});
    }

    ReplayOptions options;
    options.recovery_snapshot_path = snapshot_file.path();
    auto outcome = run_replay(feed_file.path(), options);

    EXPECT_FALSE(outcome.stopped_early);
    EXPECT_EQ(outcome.stats.sequence_failures, 1u); // the gap was still counted
    EXPECT_EQ(outcome.stats.recoveries, 1u);
    // Cumulative stats reflect the whole run, including the pre-recovery
    // event whose effect on book state was discarded by the reset.
    EXPECT_EQ(outcome.stats.messages_processed, 2u);
    EXPECT_EQ(outcome.stats.adds, 2u);

    const auto* book = outcome.books.find_book(1);
    ASSERT_NE(book, nullptr);

    // Order 1 (applied before the gap) is gone -- outcome.books was
    // wholesale replaced by the snapshot's state, not merged with it.
    auto bids = book->all_bids();
    ASSERT_EQ(bids.size(), 1u);
    EXPECT_EQ(bids[0].order_id, 999u); // from the snapshot
    EXPECT_EQ(bids[0].price, 70);

    // The event that triggered recovery (order 2) WAS applied, on top of
    // the freshly-loaded snapshot state.
    auto asks = book->all_asks();
    ASSERT_EQ(asks.size(), 1u);
    EXPECT_EQ(asks[0].order_id, 2u);
    EXPECT_EQ(asks[0].price, 60);
}

TEST(SequenceRecovery, RecoveryFailureStopsWithADescriptiveReason) {
    TempFile feed_file("mdh_test_recovery_missing_snapshot_feed.bin");
    {
        EventFileWriter w(feed_file.path());
        w.write(Event{AddOrder{.sequence_number = 1, .timestamp_ns = 1, .order_id = 1, .instrument_id = 1, .price = 50, .quantity = 1, .side = Side::Buy}});
        w.write(Event{AddOrder{.sequence_number = 5, .timestamp_ns = 2, .order_id = 2, .instrument_id = 1, .price = 60, .quantity = 2, .side = Side::Sell}});
    }

    ReplayOptions options;
    options.recovery_snapshot_path = "mdh_test_recovery_snapshot_that_does_not_exist.bin";
    auto outcome = run_replay(feed_file.path(), options);

    EXPECT_TRUE(outcome.stopped_early);
    EXPECT_NE(outcome.stop_reason.find("recovery"), std::string::npos);
    EXPECT_EQ(outcome.stats.recoveries, 0u);
}

TEST(SequenceRecovery, DuplicateAndOutOfOrderDoNotTriggerRecoveryEvenIfConfigured) {
    TempFile snapshot_file("mdh_test_recovery_unused_snapshot.bin");
    TempFile feed_file("mdh_test_recovery_duplicate_feed.bin");

    {
        BookManager snapshot_books;
        ASSERT_TRUE(write_snapshot(snapshot_file.path(), 1, snapshot_books));
    }
    {
        EventFileWriter w(feed_file.path());
        w.write(Event{AddOrder{.sequence_number = 1, .timestamp_ns = 1, .order_id = 1, .instrument_id = 1, .price = 50, .quantity = 1, .side = Side::Buy}});
        w.write(Event{AddOrder{.sequence_number = 1, .timestamp_ns = 2, .order_id = 2, .instrument_id = 1, .price = 60, .quantity = 1, .side = Side::Sell}}); // duplicate of sequence 1, not a gap
    }

    ReplayOptions options;
    options.recovery_snapshot_path = snapshot_file.path(); // configured, but should not fire for a Duplicate
    auto outcome = run_replay(feed_file.path(), options);

    EXPECT_TRUE(outcome.stopped_early); // stop_on_sequence_error still governs Duplicate/OutOfOrder
    EXPECT_EQ(outcome.stats.recoveries, 0u);
    EXPECT_EQ(outcome.stats.sequence_failures, 1u);
}
