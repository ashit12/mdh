#pragma once

#include <functional>
#include <utility>

#include "common/types.hpp"
#include "exchange/core/event_sink.hpp"
#include "exchange/core/events.hpp"
#include "protocol/messages.hpp"

// Translates the matching engine's event stream into the same wire format
// the trader side already speaks. This is what lets the decoder, the
// trader's book and the replay engine consume real exchange output rather
// than only a generated feed file.
//
// ── Only public events cross this boundary ────────────────────────────────
// The account-addressed events -- accepted, rejected, cancelled, replaced --
// must never become wire messages here. A market-data feed never reveals
// whose order is whose. Only the anonymous events may go out:
//
//   BookOrderAdded    -> AddOrder
//   BookOrderReduced  -> ModifyOrder
//   BookOrderRemoved  -> CancelOrder
//   TradeExecuted     -> Trade, with the buyer and seller accounts stripped;
//                        only instrument, price, quantity and aggressor side
//                        are public
//
// ── Its own sequence numbers and timestamps ───────────────────────────────
// A wire event carries a sequence number and a timestamp that an exchange
// event deliberately does not. Neither is borrowed from elsewhere: the
// engine's event sequence and the command sequence both exist for other
// purposes, and neither means "position in the outbound feed." This class
// keeps its own counter for exactly that.
//
// Reading the clock here is a narrow, deliberate exception to the rule
// against wall-clock time in deterministic code. That rule protects the
// matching engine; a feed publish is a different boundary, recording the
// moment a fact left the building, exactly as a real exchange's timestamp
// does. The clock is injectable so tests are not at its mercy.
namespace mdh::exchange::market_data {

using MarketDataSink = std::function<void(const protocol::Event&)>;

struct MarketDataPublisherOptions {
    // Null means real wall-clock time. Override with a fixed or incrementing
    // stub in tests that need reproducible timestamps.
    std::function<Timestamp()> clock;
};

class MarketDataPublisher {
public:
    explicit MarketDataPublisher(MarketDataPublisherOptions options = {});

    // Turns `event` into zero or one wire message and, if there is one,
    // calls `sink` with it. Producing nothing is the correct, silent outcome
    // for every private event type, not an error.
    void publish(const ExchangeEvent& event, const MarketDataSink& sink);

    // Wraps a downstream sink into an EventSink-shaped callable, so a
    // publisher can be plugged in anywhere an EventSink is expected --
    // fanned out alongside the ledger's sink, for instance -- without the
    // caller knowing this class is underneath.
    [[nodiscard]] EventSink sink(MarketDataSink downstream) {
        return [this, downstream = std::move(downstream)](const ExchangeEvent& event) { publish(event, downstream); };
    }

    // Introspection only, for a test asserting how many wire messages have
    // gone out so far.
    [[nodiscard]] Sequence next_sequence() const { return next_sequence_; }

private:
    // One helper per publishable event type, each producing exactly one wire
    // message and passing it to the sink.
    void publish_book_order_added(const BookOrderAdded& event, const MarketDataSink& sink);
    void publish_book_order_reduced(const BookOrderReduced& event, const MarketDataSink& sink);
    void publish_book_order_removed(const BookOrderRemoved& event, const MarketDataSink& sink);
    void publish_trade_executed(const TradeExecuted& event, const MarketDataSink& sink);

    // One sequence number per message actually emitted, never one per event
    // seen: a private event that produces no wire message must not consume a
    // number either.
    [[nodiscard]] Sequence next_sequence_number() { return next_sequence_++; }

    Sequence next_sequence_ = 1;
    MarketDataPublisherOptions options_;
};

} // namespace mdh::exchange::market_data
