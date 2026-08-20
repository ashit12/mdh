#pragma once

#include <cstdint>
#include <variant>

#include "common/types.hpp"
#include "exchange/core/types.hpp"

// The order-entry wire format: a two-way TCP stream between one client and
// the gateway, carrying client requests (NewOrder, CancelOrder,
// ReplaceOrder) and gateway responses (Accepted, Rejected, Cancelled,
// Replaced, TradeReport). It resembles what the industry calls OUCH.
//
// protocol/messages.hpp is the other half: one-way, UDP, market data only,
// resembling ITCH. The two share no header and no message type on purpose.
// They run over different transports with different guarantees, and forcing
// one representation would tie a lossy, unordered, public broadcast to a
// reliable, ordered, private session for no benefit.
//
// ── Why there is no sequence number here ──────────────────────────────────
// The market-data header carries one because UDP can drop and reorder
// datagrams, so a receiver needs it to spot gaps and recover. TCP already
// guarantees ordered, lossless, exactly-once delivery of the byte stream, so
// an application-level sequence number would have nothing left to detect.
//
// ── Why the header is 3 bytes rather than 20 ──────────────────────────────
// A UDP receiver gets exactly one whole datagram per recvfrom(), so message
// boundaries come free from the OS. A TCP receiver gets an arbitrary,
// possibly partial slice of the stream per read(), so the header's only
// remaining job is framing: saying how many payload bytes to wait for. Hence
// just a type and a size.
namespace mdh::protocol::order_entry {

inline constexpr std::size_t HEADER_SIZE = 3; // type (u8) + payload_size (u16)

enum class MessageType : std::uint8_t {
    // Client -> gateway.
    NewOrder = 1,
    CancelOrder = 2,
    ReplaceOrder = 3,

    // Gateway -> client.
    Accepted = 10,
    Rejected = 11,
    Cancelled = 12,
    Replaced = 13,
    TradeReport = 14,
};

struct Header {
    MessageType type;
    std::uint16_t payload_size;
};

// ── Client -> gateway ──────────────────────────────────────────────────────

struct NewOrder {
    exchange::AccountId account_id;
    exchange::ClientOrderId client_order_id;
    InstrumentId instrument_id;
    Side side;
    Price price;
    Quantity quantity;
    exchange::OrderType order_type;
    exchange::TimeInForce time_in_force;

    bool operator==(const NewOrder&) const = default;
};

struct CancelOrder {
    exchange::AccountId account_id;
    exchange::ClientOrderId client_order_id;
    InstrumentId instrument_id;

    bool operator==(const CancelOrder&) const = default;
};

struct ReplaceOrder {
    exchange::AccountId account_id;
    // Identifies the resting order to replace -- the client_order_id it was
    // originally submitted under, same convention as exchange::ReplaceOrderCommand.
    exchange::ClientOrderId original_client_order_id;
    exchange::ClientOrderId new_client_order_id;
    InstrumentId instrument_id;
    Price new_price;
    Quantity new_quantity;

    bool operator==(const ReplaceOrder&) const = default;
};

// ── Gateway -> client ───────────────────────────────────────────────────────

struct Accepted {
    exchange::AccountId account_id;
    exchange::ClientOrderId client_order_id;
    exchange::ExchangeOrderId exchange_order_id;
    InstrumentId instrument_id;
    Side side;
    Price price;
    Quantity quantity;
    exchange::OrderType order_type;
    exchange::TimeInForce time_in_force;

    bool operator==(const Accepted&) const = default;
};

struct Rejected {
    exchange::AccountId account_id;
    exchange::ClientOrderId client_order_id;
    InstrumentId instrument_id;
    exchange::RejectReason reason;

    bool operator==(const Rejected&) const = default;
};

struct Cancelled {
    exchange::AccountId account_id;
    exchange::ClientOrderId client_order_id;
    exchange::ExchangeOrderId exchange_order_id;
    InstrumentId instrument_id;

    bool operator==(const Cancelled&) const = default;
};

struct Replaced {
    exchange::AccountId account_id;
    exchange::ClientOrderId original_client_order_id;
    exchange::ClientOrderId new_client_order_id;
    exchange::ExchangeOrderId exchange_order_id;
    InstrumentId instrument_id;
    Price new_price;
    Quantity new_quantity;

    bool operator==(const Replaced&) const = default;
};

// One side of a trade, addressed to a single account -- exchange::TradeExecuted
// carries a buyer and a seller counterparty that can belong to two different
// connections (see exchange/core/events.hpp), so a single trade can produce up
// to two of these, each routed to its own connection independently.
struct TradeReport {
    exchange::AccountId account_id;
    exchange::ClientOrderId client_order_id;
    exchange::ExchangeOrderId exchange_order_id;
    InstrumentId instrument_id;
    Price price;
    Quantity quantity;
    // Remaining quantity on this account's order after this trade (0 if now
    // fully filled) -- mirrors exchange::TradeCounterparty::remaining_quantity.
    Quantity remaining_quantity;

    bool operator==(const TradeReport&) const = default;
};

using Message = std::variant<NewOrder, CancelOrder, ReplaceOrder, Accepted, Rejected, Cancelled, Replaced,
                              TradeReport>;

// Fixed on-wire payload size (bytes, not counting the header) for each known
// message type -- all order-entry message types are fixed-size, same as
// every message type in protocol/messages.hpp.
[[nodiscard]] constexpr std::size_t payload_size_for(MessageType type) {
    switch (type) {
        case MessageType::NewOrder:     return 8 + 8 + 4 + 1 + 8 + 8 + 1 + 1; // 39
        case MessageType::CancelOrder:  return 8 + 8 + 4;                    // 20
        case MessageType::ReplaceOrder: return 8 + 8 + 8 + 4 + 8 + 8;        // 44
        case MessageType::Accepted:     return 8 + 8 + 8 + 4 + 1 + 8 + 8 + 1 + 1; // 47
        case MessageType::Rejected:     return 8 + 8 + 4 + 1;                // 21
        case MessageType::Cancelled:    return 8 + 8 + 8 + 4;                // 28
        case MessageType::Replaced:     return 8 + 8 + 8 + 8 + 4 + 8 + 8;   // 52
        case MessageType::TradeReport:  return 8 + 8 + 8 + 4 + 8 + 8 + 8;   // 52
    }
    return 0;
}

} // namespace mdh::protocol::order_entry
