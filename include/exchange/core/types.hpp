#pragma once

#include <cstdint>
#include <string_view>

// Deterministic, transport-independent exchange domain types -- the
// exchange's own vocabulary, defined before any wire bytes, TCP session, or
// matching algorithm exist. Depends only on common/types.hpp (Price/
// Quantity/Side/InstrumentId are reused as-is: an exchange's notion of price,
// quantity, side, and instrument is identical to a market-data feed's, no
// reason to duplicate them). Must never depend on protocol/, replay/, net/,
// or book/ -- those are either wire/transport concerns or the trader-side
// reconstructed book, neither of which the exchange's own domain model
// should know about.
namespace mdh::exchange {

using AccountId = std::uint64_t;
using ClientOrderId = std::uint64_t;
using ExchangeOrderId = std::uint64_t;
using CommandSequence = std::uint64_t;
using EventSequence = std::uint64_t;

enum class OrderType {
    Limit,
};

[[nodiscard]] constexpr std::string_view to_string(OrderType t) {
    switch (t) {
        case OrderType::Limit: return "Limit";
    }
    return "UnknownOrderType";
}

enum class TimeInForce : std::uint8_t {
    GTC, // Good-Til-Cancelled: unmatched remainder rests on the book.
    IOC, // Immediate-Or-Cancel: unmatched remainder is discarded, never rests.
    FOK, // Fill-Or-Kill: fully filled immediately, or not executed at all.
};

[[nodiscard]] constexpr std::string_view to_string(TimeInForce t) {
    switch (t) {
        case TimeInForce::GTC: return "GTC";
        case TimeInForce::IOC: return "IOC";
        case TimeInForce::FOK: return "FOK";
    }
    return "UnknownTimeInForce";
}

// Only the reasons this project actually needs -- not an encyclopaedia of
// every rejection a real exchange might issue.
enum class RejectReason {
    None, // not a rejection; paired with an accepted/successful outcome
    InvalidPrice,
    InvalidQuantity,
    DuplicateOrderId,
    UnknownOrderId,
    InvalidInstrument,
    InsufficientLiquidity, // FOK: could not be fully filled immediately
    InvalidReplacement,
    InternalError,
    // From exchange/risk/: pre-trade checks performed before a
    // command is allowed to reach the matching engine at all -- distinct
    // from InsufficientLiquidity above, which is a book-depth concept
    // (not enough resting quantity to fill against), not an account-balance
    // one (not enough of the account's own funds/holdings to attempt the
    // order in the first place).
    InsufficientFunds,    // buy: account's available (unreserved) cash can't cover price * quantity
    InsufficientPosition, // sell: account's available (unreserved) instrument holdings are short
    OrderTooLarge,        // exceeds RiskLimits::max_order_quantity
    // From exchange/gateway/: the message's account_id is not the
    // one this connection bound to on its first message. Unlike every
    // reason above, this one is produced by the gateway itself and the
    // command never reaches the matching engine at all -- see
    // OrderEntryGateway's own session-binding doc comment. Appended last
    // so every existing reason keeps its on-wire value.
    AccountMismatch,
};

[[nodiscard]] constexpr std::string_view to_string(RejectReason r) {
    switch (r) {
        case RejectReason::None:                 return "None";
        case RejectReason::InvalidPrice:          return "InvalidPrice";
        case RejectReason::InvalidQuantity:       return "InvalidQuantity";
        case RejectReason::DuplicateOrderId:      return "DuplicateOrderId";
        case RejectReason::UnknownOrderId:        return "UnknownOrderId";
        case RejectReason::InvalidInstrument:     return "InvalidInstrument";
        case RejectReason::InsufficientLiquidity: return "InsufficientLiquidity";
        case RejectReason::InvalidReplacement:    return "InvalidReplacement";
        case RejectReason::InternalError:         return "InternalError";
        case RejectReason::InsufficientFunds:     return "InsufficientFunds";
        case RejectReason::InsufficientPosition:  return "InsufficientPosition";
        case RejectReason::OrderTooLarge:         return "OrderTooLarge";
        case RejectReason::AccountMismatch:       return "AccountMismatch";
    }
    return "UnknownRejectReason";
}

} // namespace mdh::exchange
