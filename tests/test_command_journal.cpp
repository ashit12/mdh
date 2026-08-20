#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>

#include "exchange/persistence/command_journal_reader.hpp"
#include "exchange/persistence/command_journal_writer.hpp"

using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::persistence;

namespace {

// Each TEST gets a unique path so parallel/repeated runs never collide; the
// file is removed on scope exit regardless of pass/fail. Mirrors
// EventFileIO's TempFile helper.
class TempFile {
public:
    explicit TempFile(std::string name) : path_(std::move(name)) {}
    ~TempFile() { std::remove(path_.c_str()); }
    [[nodiscard]] const std::string& path() const { return path_; }

private:
    std::string path_;
};

} // namespace

TEST(CommandJournal, WritesAndReadsBackAllThreeCommandTypes) {
    TempFile tmp("mdh_test_command_journal_roundtrip.bin");
    constexpr std::array<InstrumentId, 1> kInstruments{1};
    {
        CommandJournalWriter writer(tmp.path(), kInstruments);
        ASSERT_TRUE(writer.is_open());
        writer.write(ExchangeCommand{NewOrderCommand{.command_sequence = 1,
                                                      .account_id = 1,
                                                      .client_order_id = 1,
                                                      .instrument_id = 1,
                                                      .side = Side::Buy,
                                                      .price = 100,
                                                      .quantity = 5,
                                                      .order_type = OrderType::Limit,
                                                      .time_in_force = TimeInForce::GTC}});
        writer.write(ExchangeCommand{
            ReplaceOrderCommand{.command_sequence = 2,
                                 .account_id = 1,
                                 .original_client_order_id = 1,
                                 .new_client_order_id = 2,
                                 .instrument_id = 1,
                                 .new_price = 110,
                                 .new_quantity = 3}});
        writer.write(ExchangeCommand{
            CancelOrderCommand{.command_sequence = 3, .account_id = 1, .client_order_id = 2, .instrument_id = 1}});
    }

    CommandJournalReader reader(tmp.path());
    ASSERT_TRUE(reader.is_open());

    // The universe comes first, before any command -- that ordering is what
    // lets a replay register instruments as it reads rather than needing to
    // be told them separately.
    auto registration = reader.next();
    ASSERT_TRUE(registration.has_value());
    ASSERT_TRUE(std::holds_alternative<RegisterInstrumentRecord>(*registration));
    EXPECT_EQ(std::get<RegisterInstrumentRecord>(*registration).instrument_id, 1u);

    auto c1 = reader.next();
    ASSERT_TRUE(c1.has_value());
    ASSERT_TRUE(std::holds_alternative<ExchangeCommand>(*c1));
    EXPECT_TRUE(std::holds_alternative<NewOrderCommand>(std::get<ExchangeCommand>(*c1)));

    auto c2 = reader.next();
    ASSERT_TRUE(std::holds_alternative<ExchangeCommand>(*c2));
    EXPECT_TRUE(std::holds_alternative<ReplaceOrderCommand>(std::get<ExchangeCommand>(*c2)));

    auto c3 = reader.next();
    ASSERT_TRUE(std::holds_alternative<ExchangeCommand>(*c3));
    EXPECT_TRUE(std::holds_alternative<CancelOrderCommand>(std::get<ExchangeCommand>(*c3)));

    EXPECT_FALSE(reader.next().has_value()); // clean EOF
}

TEST(CommandJournal, TruncatedFileAtPayloadReportsTruncatedPayload) {
    TempFile tmp("mdh_test_command_journal_truncated.bin");
    {
        // No instruments: this test is about framing, and a registration
        // frame ahead of the command would just be one more frame to skip.
        CommandJournalWriter writer(tmp.path(), {});
        writer.write(ExchangeCommand{NewOrderCommand{.command_sequence = 1,
                                                      .account_id = 1,
                                                      .client_order_id = 1,
                                                      .instrument_id = 1,
                                                      .side = Side::Buy,
                                                      .price = 100,
                                                      .quantity = 5,
                                                      .order_type = OrderType::Limit,
                                                      .time_in_force = TimeInForce::GTC}});
    }
    {
        std::ifstream in(tmp.path(), std::ios::binary | std::ios::ate);
        auto size = in.tellg();
        in.close();
        ASSERT_GT(size, 0);
        std::filesystem::resize_file(tmp.path(), static_cast<std::uintmax_t>(size) - 1);
    }

    CommandJournalReader reader(tmp.path());
    auto result = reader.next();
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(std::holds_alternative<CommandDecodeError>(*result));
    EXPECT_EQ(std::get<CommandDecodeError>(*result), CommandDecodeError::TruncatedPayload);
}

TEST(CommandJournal, EmptyFileIsCleanEof) {
    TempFile tmp("mdh_test_command_journal_empty.bin");
    { std::ofstream create(tmp.path(), std::ios::binary); }

    CommandJournalReader reader(tmp.path());
    ASSERT_TRUE(reader.is_open());
    EXPECT_FALSE(reader.next().has_value());
}

TEST(CommandJournal, PreservesFieldValuesAcrossRoundTrip) {
    TempFile tmp("mdh_test_command_journal_fields.bin");
    NewOrderCommand original{.command_sequence = 99,
                              .account_id = 12345,
                              .client_order_id = 67890,
                              .instrument_id = 3,
                              .side = Side::Sell,
                              .price = -42,
                              .quantity = 17,
                              .order_type = OrderType::Limit,
                              .time_in_force = TimeInForce::FOK};
    {
        CommandJournalWriter writer(tmp.path(), {});
        writer.write(ExchangeCommand{original});
    }

    CommandJournalReader reader(tmp.path());
    auto result = reader.next();
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(std::holds_alternative<ExchangeCommand>(*result));
    const auto& decoded = std::get<ExchangeCommand>(*result);
    ASSERT_TRUE(std::holds_alternative<NewOrderCommand>(decoded));
    EXPECT_EQ(std::get<NewOrderCommand>(decoded), original);
}
