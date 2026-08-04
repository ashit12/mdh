#include <gtest/gtest.h>

#include <string>
#include <type_traits>
#include <variant>
#include <vector>

#include "exchange/core/commands.hpp"

namespace mdh::exchange {
namespace {

TEST(ExchangeCommands, NewOrderCommandConstructs) {
    NewOrderCommand cmd{
        .command_sequence = 1,
        .account_id = 100,
        .client_order_id = 42,
        .instrument_id = 7,
        .side = Side::Buy,
        .price = 10000,
        .quantity = 5,
        .order_type = OrderType::Limit,
        .time_in_force = TimeInForce::GTC,
    };
    EXPECT_EQ(cmd.command_sequence, 1u);
    EXPECT_EQ(cmd.account_id, 100u);
    EXPECT_EQ(cmd.client_order_id, 42u);
    EXPECT_EQ(cmd.instrument_id, 7u);
    EXPECT_EQ(cmd.side, Side::Buy);
    EXPECT_EQ(cmd.price, 10000);
    EXPECT_EQ(cmd.quantity, 5u);
    EXPECT_EQ(cmd.order_type, OrderType::Limit);
    EXPECT_EQ(cmd.time_in_force, TimeInForce::GTC);
}

TEST(ExchangeCommands, CancelOrderCommandConstructs) {
    CancelOrderCommand cmd{
        .command_sequence = 2,
        .account_id = 100,
        .client_order_id = 42,
        .instrument_id = 7,
    };
    EXPECT_EQ(cmd.command_sequence, 2u);
    EXPECT_EQ(cmd.account_id, 100u);
    EXPECT_EQ(cmd.client_order_id, 42u);
    EXPECT_EQ(cmd.instrument_id, 7u);
}

TEST(ExchangeCommands, ReplaceOrderCommandConstructs) {
    ReplaceOrderCommand cmd{
        .command_sequence = 3,
        .account_id = 100,
        .original_client_order_id = 42,
        .new_client_order_id = 43,
        .instrument_id = 7,
        .new_price = 10500,
        .new_quantity = 3,
    };
    EXPECT_EQ(cmd.original_client_order_id, 42u);
    EXPECT_EQ(cmd.new_client_order_id, 43u);
    EXPECT_EQ(cmd.new_price, 10500);
    EXPECT_EQ(cmd.new_quantity, 3u);
}

TEST(ExchangeCommands, VariantHoldsEachAlternative) {
    ExchangeCommand new_order = NewOrderCommand{.command_sequence = 1,
                                                 .account_id = 1,
                                                 .client_order_id = 1,
                                                 .instrument_id = 1,
                                                 .side = Side::Buy,
                                                 .price = 100,
                                                 .quantity = 1,
                                                 .order_type = OrderType::Limit,
                                                 .time_in_force = TimeInForce::GTC};
    ExchangeCommand cancel =
        CancelOrderCommand{.command_sequence = 2, .account_id = 1, .client_order_id = 1, .instrument_id = 1};
    ExchangeCommand replace = ReplaceOrderCommand{.command_sequence = 3,
                                                   .account_id = 1,
                                                   .original_client_order_id = 1,
                                                   .new_client_order_id = 2,
                                                   .instrument_id = 1,
                                                   .new_price = 100,
                                                   .new_quantity = 1};

    EXPECT_TRUE(std::holds_alternative<NewOrderCommand>(new_order));
    EXPECT_TRUE(std::holds_alternative<CancelOrderCommand>(cancel));
    EXPECT_TRUE(std::holds_alternative<ReplaceOrderCommand>(replace));
}

TEST(ExchangeCommands, VariantVisitationDispatchesToCorrectAlternative) {
    std::vector<ExchangeCommand> commands;
    commands.push_back(NewOrderCommand{.command_sequence = 1,
                                        .account_id = 1,
                                        .client_order_id = 1,
                                        .instrument_id = 1,
                                        .side = Side::Buy,
                                        .price = 100,
                                        .quantity = 1,
                                        .order_type = OrderType::Limit,
                                        .time_in_force = TimeInForce::GTC});
    commands.push_back(CancelOrderCommand{.command_sequence = 2, .account_id = 1, .client_order_id = 1, .instrument_id = 1});
    commands.push_back(ReplaceOrderCommand{.command_sequence = 3,
                                            .account_id = 1,
                                            .original_client_order_id = 1,
                                            .new_client_order_id = 2,
                                            .instrument_id = 1,
                                            .new_price = 100,
                                            .new_quantity = 1});

    std::vector<std::string> kinds;
    for (const auto& cmd : commands) {
        std::visit(
            [&](const auto& c) {
                using T = std::decay_t<decltype(c)>;
                if constexpr (std::is_same_v<T, NewOrderCommand>) {
                    kinds.emplace_back("New");
                } else if constexpr (std::is_same_v<T, CancelOrderCommand>) {
                    kinds.emplace_back("Cancel");
                } else if constexpr (std::is_same_v<T, ReplaceOrderCommand>) {
                    kinds.emplace_back("Replace");
                }
            },
            cmd);
    }
    EXPECT_EQ(kinds, (std::vector<std::string>{"New", "Cancel", "Replace"}));
}

} // namespace
} // namespace mdh::exchange
