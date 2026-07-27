#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>

#include "replay/event_file_reader.hpp"
#include "replay/event_file_writer.hpp"

using namespace mdh;
using namespace mdh::protocol;
using namespace mdh::replay;

namespace {

// Each TEST gets a unique path (via the GoogleTest test name) so parallel
// runs / repeated runs never collide, and the file is removed on scope
// exit regardless of pass/fail.
class TempFile {
public:
    explicit TempFile(std::string name) : path_(std::move(name)) {}
    ~TempFile() { std::remove(path_.c_str()); }
    [[nodiscard]] const std::string& path() const { return path_; }

private:
    std::string path_;
};

} // namespace

TEST(EventFileIO, WritesAndReadsBackMultipleEventTypes) {
    TempFile tmp("mdh_test_roundtrip.bin");
    {
        EventFileWriter writer(tmp.path());
        ASSERT_TRUE(writer.is_open());
        writer.write(Event{AddOrder{.sequence_number = 1, .timestamp_ns = 10, .order_id = 1, .instrument_id = 1, .price = 100, .quantity = 5, .side = Side::Buy}});
        writer.write(Event{ModifyOrder{.sequence_number = 2, .timestamp_ns = 20, .order_id = 1, .instrument_id = 1, .new_price = 110, .new_quantity = 3}});
        writer.write(Event{CancelOrder{.sequence_number = 3, .timestamp_ns = 30, .order_id = 1, .instrument_id = 1}});
        writer.write(Event{Trade{.sequence_number = 4, .timestamp_ns = 40, .instrument_id = 1, .price = 105, .quantity = 2, .aggressor_side = Side::Sell}});
        writer.write(Event{ClearBook{.sequence_number = 5, .timestamp_ns = 50, .instrument_id = 1}});
    }

    EventFileReader reader(tmp.path());
    ASSERT_TRUE(reader.is_open());

    auto e1 = reader.next();
    ASSERT_TRUE(e1.has_value());
    ASSERT_TRUE(std::holds_alternative<Event>(*e1));
    EXPECT_TRUE(std::holds_alternative<AddOrder>(std::get<Event>(*e1)));

    auto e2 = reader.next();
    ASSERT_TRUE(std::holds_alternative<Event>(*e2));
    EXPECT_TRUE(std::holds_alternative<ModifyOrder>(std::get<Event>(*e2)));

    auto e3 = reader.next();
    ASSERT_TRUE(std::holds_alternative<Event>(*e3));
    EXPECT_TRUE(std::holds_alternative<CancelOrder>(std::get<Event>(*e3)));

    auto e4 = reader.next();
    ASSERT_TRUE(std::holds_alternative<Event>(*e4));
    EXPECT_TRUE(std::holds_alternative<Trade>(std::get<Event>(*e4)));

    auto e5 = reader.next();
    ASSERT_TRUE(std::holds_alternative<Event>(*e5));
    EXPECT_TRUE(std::holds_alternative<ClearBook>(std::get<Event>(*e5)));

    EXPECT_FALSE(reader.next().has_value()); // clean EOF
}

TEST(EventFileIO, TruncatedFileAtPayloadReportsTruncatedPayload) {
    TempFile tmp("mdh_test_truncated_payload.bin");
    {
        EventFileWriter writer(tmp.path());
        writer.write(Event{AddOrder{.sequence_number = 1, .timestamp_ns = 10, .order_id = 1, .instrument_id = 1, .price = 100, .quantity = 5, .side = Side::Buy}});
    }
    // Chop off the last byte so the header claims a payload that isn't
    // fully present.
    {
        std::ifstream in(tmp.path(), std::ios::binary | std::ios::ate);
        auto size = in.tellg();
        in.close();
        ASSERT_GT(size, 0);
        std::filesystem::resize_file(tmp.path(), static_cast<std::uintmax_t>(size) - 1);
    }

    EventFileReader reader(tmp.path());
    auto result = reader.next();
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(std::holds_alternative<DecodeError>(*result));
    EXPECT_EQ(std::get<DecodeError>(*result), DecodeError::TruncatedPayload);
}

TEST(EventFileIO, EmptyFileIsCleanEof) {
    TempFile tmp("mdh_test_empty.bin");
    { std::ofstream create(tmp.path(), std::ios::binary); }

    EventFileReader reader(tmp.path());
    ASSERT_TRUE(reader.is_open());
    EXPECT_FALSE(reader.next().has_value());
}
