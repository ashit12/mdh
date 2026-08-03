#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>

#include "book/book_manager.hpp"
#include "replay/snapshot.hpp"

using namespace mdh;
using namespace mdh::book;
using namespace mdh::replay;

namespace {

// Each TEST gets a unique path so parallel/repeated runs never collide,
// and the file is removed on scope exit regardless of pass/fail -- same
// convention as test_event_file_io.cpp's TempFile.
class TempFile {
public:
    explicit TempFile(std::string name) : path_(std::move(name)) {}
    ~TempFile() { std::remove(path_.c_str()); }
    [[nodiscard]] const std::string& path() const { return path_; }

private:
    std::string path_;
};

} // namespace

TEST(Snapshot, RoundTripPreservesEveryOrderAcrossMultipleInstruments) {
    TempFile tmp("mdh_test_snapshot_roundtrip.bin");

    BookManager books;
    ASSERT_FALSE(books.book_for(1).add_order(1, 100, 10, Side::Buy).has_value());
    ASSERT_FALSE(books.book_for(1).add_order(2, 100, 5, Side::Buy).has_value()); // same price, FIFO
    ASSERT_FALSE(books.book_for(1).add_order(3, 105, 7, Side::Buy).has_value());
    ASSERT_FALSE(books.book_for(1).add_order(4, 200, 3, Side::Sell).has_value());
    ASSERT_FALSE(books.book_for(2).add_order(5, 500, 20, Side::Buy).has_value());
    ASSERT_FALSE(books.book_for(2).add_order(6, 600, 15, Side::Sell).has_value());

    ASSERT_TRUE(write_snapshot(tmp.path(), 42, books));

    auto loaded = read_snapshot(tmp.path());
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->sequence_number, 42u);

    const auto* book1 = loaded->books.find_book(1);
    ASSERT_NE(book1, nullptr);
    auto bids1 = book1->all_bids();
    ASSERT_EQ(bids1.size(), 3u);
    EXPECT_EQ(bids1[0].order_id, 3u); // highest price first
    EXPECT_EQ(bids1[0].price, 105);
    EXPECT_EQ(bids1[1].order_id, 1u); // then FIFO within price 100
    EXPECT_EQ(bids1[1].quantity, 10u);
    EXPECT_EQ(bids1[2].order_id, 2u);
    EXPECT_EQ(bids1[2].quantity, 5u);
    auto asks1 = book1->all_asks();
    ASSERT_EQ(asks1.size(), 1u);
    EXPECT_EQ(asks1[0].order_id, 4u);
    EXPECT_EQ(asks1[0].price, 200);

    const auto* book2 = loaded->books.find_book(2);
    ASSERT_NE(book2, nullptr);
    EXPECT_EQ(book2->all_bids().size(), 1u);
    EXPECT_EQ(book2->all_asks().size(), 1u);

    // Instrument list on the loaded side matches what was written.
    auto instruments = loaded->books.instruments();
    ASSERT_EQ(instruments.size(), 2u);
    EXPECT_EQ(instruments[0], 1u);
    EXPECT_EQ(instruments[1], 2u);
}

TEST(Snapshot, EmptyBookManagerRoundTripsToZeroEntries) {
    TempFile tmp("mdh_test_snapshot_empty.bin");
    BookManager books; // no orders at all

    ASSERT_TRUE(write_snapshot(tmp.path(), 7, books));

    auto loaded = read_snapshot(tmp.path());
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->sequence_number, 7u);
    EXPECT_TRUE(loaded->books.instruments().empty());
}

TEST(Snapshot, ReadNonexistentFileReturnsNullopt) {
    EXPECT_FALSE(read_snapshot("mdh_test_snapshot_does_not_exist.bin").has_value());
}

TEST(Snapshot, BadMagicIsRejected) {
    TempFile tmp("mdh_test_snapshot_bad_magic.bin");
    BookManager books;
    ASSERT_FALSE(books.book_for(1).add_order(1, 100, 1, Side::Buy).has_value());
    ASSERT_TRUE(write_snapshot(tmp.path(), 1, books));

    // Corrupt the first byte of the magic.
    {
        std::fstream f(tmp.path(), std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(f.is_open());
        f.seekp(0);
        char corrupt = '\x00';
        f.write(&corrupt, 1);
    }

    EXPECT_FALSE(read_snapshot(tmp.path()).has_value());
}

TEST(Snapshot, TruncatedHeaderIsRejected) {
    TempFile tmp("mdh_test_snapshot_truncated_header.bin");
    { std::ofstream f(tmp.path(), std::ios::binary); }
    // File exists but is empty -- far short of the 24-byte header.
    EXPECT_FALSE(read_snapshot(tmp.path()).has_value());
}

TEST(Snapshot, TruncatedEntriesAreRejected) {
    TempFile tmp("mdh_test_snapshot_truncated_entries.bin");
    BookManager books;
    ASSERT_FALSE(books.book_for(1).add_order(1, 100, 1, Side::Buy).has_value());
    ASSERT_FALSE(books.book_for(1).add_order(2, 100, 1, Side::Buy).has_value());
    ASSERT_TRUE(write_snapshot(tmp.path(), 1, books)); // header claims 2 entries

    // Chop off the last byte so the second entry is short by one byte.
    std::ifstream in(tmp.path(), std::ios::binary | std::ios::ate);
    auto size = in.tellg();
    in.close();
    ASSERT_GT(size, 0);
    std::filesystem::resize_file(tmp.path(), static_cast<std::uintmax_t>(size) - 1);

    EXPECT_FALSE(read_snapshot(tmp.path()).has_value());
}
