#include <gtest/gtest.h>

#include <variant>

#include "exchange/sequencing/command_sequencer.hpp"

namespace mdh::exchange::sequencing {
namespace {

constexpr InstrumentId kInstrument = 1;

NewOrderCommand new_order(CommandSequence seq) {
    return NewOrderCommand{
        .command_sequence = seq,
        .account_id = 100,
        .client_order_id = 1,
        .instrument_id = kInstrument,
        .side = Side::Buy,
        .price = 100,
        .quantity = 10,
        .order_type = OrderType::Limit,
        .time_in_force = TimeInForce::GTC,
    };
}

CancelOrderCommand cancel_order(CommandSequence seq) {
    return CancelOrderCommand{.command_sequence = seq, .account_id = 100, .client_order_id = 1, .instrument_id = kInstrument};
}

ReplaceOrderCommand replace_order(CommandSequence seq) {
    return ReplaceOrderCommand{
        .command_sequence = seq,
        .account_id = 100,
        .original_client_order_id = 1,
        .new_client_order_id = 2,
        .instrument_id = kInstrument,
        .new_price = 105,
        .new_quantity = 5,
    };
}

CommandSequence sequence_of(const ExchangeCommand& command) {
    return std::visit([](const auto& cmd) { return cmd.command_sequence; }, command);
}

} // namespace

TEST(CommandSequencer, FirstAssignedSequenceIsOne) {
    CommandSequencer sequencer;
    EXPECT_EQ(sequencer.next_sequence(), 1u);

    const ExchangeCommand sequenced = sequencer.sequence(new_order(0));
    EXPECT_EQ(sequence_of(sequenced), 1u);
    EXPECT_EQ(sequencer.next_sequence(), 2u);
}

TEST(CommandSequencer, SequenceValuesAreStrictlyIncreasing) {
    CommandSequencer sequencer;

    const ExchangeCommand first = sequencer.sequence(new_order(0));
    const ExchangeCommand second = sequencer.sequence(cancel_order(0));
    const ExchangeCommand third = sequencer.sequence(replace_order(0));

    EXPECT_EQ(sequence_of(first), 1u);
    EXPECT_EQ(sequence_of(second), 2u);
    EXPECT_EQ(sequence_of(third), 3u);
}

TEST(CommandSequencer, OverwritesWhateverSequenceTheCallerSupplied) {
    CommandSequencer sequencer;

    // A caller might pass an arbitrary placeholder (here, deliberately a
    // large, out-of-order value) -- the sequencer must ignore it entirely
    // and stamp its own authoritative value instead.
    const ExchangeCommand sequenced = sequencer.sequence(new_order(999));
    EXPECT_EQ(sequence_of(sequenced), 1u);
}

TEST(CommandSequencer, PreservesEveryOtherFieldOnEachCommandVariant) {
    CommandSequencer sequencer;

    const ExchangeCommand new_cmd = sequencer.sequence(new_order(0));
    ASSERT_TRUE(std::holds_alternative<NewOrderCommand>(new_cmd));
    const auto& n = std::get<NewOrderCommand>(new_cmd);
    EXPECT_EQ(n.account_id, 100u);
    EXPECT_EQ(n.client_order_id, 1u);
    EXPECT_EQ(n.instrument_id, kInstrument);
    EXPECT_EQ(n.side, Side::Buy);
    EXPECT_EQ(n.price, 100);
    EXPECT_EQ(n.quantity, 10u);

    const ExchangeCommand cancel_cmd = sequencer.sequence(cancel_order(0));
    ASSERT_TRUE(std::holds_alternative<CancelOrderCommand>(cancel_cmd));
    EXPECT_EQ(std::get<CancelOrderCommand>(cancel_cmd).client_order_id, 1u);

    const ExchangeCommand replace_cmd = sequencer.sequence(replace_order(0));
    ASSERT_TRUE(std::holds_alternative<ReplaceOrderCommand>(replace_cmd));
    const auto& r = std::get<ReplaceOrderCommand>(replace_cmd);
    EXPECT_EQ(r.new_client_order_id, 2u);
    EXPECT_EQ(r.new_price, 105);
    EXPECT_EQ(r.new_quantity, 5u);
}

} // namespace mdh::exchange::sequencing
