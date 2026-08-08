#pragma once

#include <functional>
#include <utility>

#include "common/types.hpp"
#include "exchange/core/event_sink.hpp"
#include "exchange/core/events.hpp"
#include "protocol/messages.hpp"

// The market-data publisher (Milestone 6): translates the matching engine's
// own ExchangeEvent stream into the SAME wire format the trader side
// already speaks (protocol::Event -- AddOrder/CancelOrder/ModifyOrder/
// Trade/ClearBook, see include/protocol/messages.hpp). This is the
// component that finally lets everything already built on the trader side
// (decoder, book::BookManager, replay::run_replay(), SequenceValidator)
// consume real matching-engine output instead of only feed_generator's
// synthetic data -- see docs/exchange_flow.md for the worked example this
// milestone is meant to make possible.
//
// ── Only the public events cross this boundary ─────────────────────────────
// OrderAccepted/OrderRejected/OrderCancelled/OrderReplaced are private,
// account-addressed events (see events.hpp's own class comment) and must
// NEVER be translated into a wire message here -- a real market-data feed
// never reveals whose order is whose. Only the four already-anonymous
// events may become wire messages:
//   BookOrderAdded    -> protocol::AddOrder
//   BookOrderReduced  -> protocol::ModifyOrder
//   BookOrderRemoved  -> protocol::CancelOrder
//   TradeExecuted     -> protocol::Trade   (buyer/seller account info is
//                                           stripped -- only instrument,
//                                           price, quantity, and aggressor
//                                           side are public)
//
// ── A new, separate Sequence/Timestamp stream ───────────────────────────────
// protocol::Event carries sequence_number/timestamp_ns fields that
// ExchangeEvent deliberately does not have (see commands.hpp's own comment
// on why a command never carries a timestamp). Those two concepts are not
// reused from anywhere else -- EventSequence (exchange-internal, gapless
// across every event this engine has ever emitted) and CommandSequence
// (assigned by exchange::sequencing::CommandSequencer) are both the wrong
// stream for this: neither is "the position of this fact in the outbound
// market-data feed." MarketDataPublisher owns its own monotonic counter for
// exactly that purpose, the same way CommandSequencer owns its own counter
// for a different purpose.
//
// Capturing wall-clock time here (rather than deriving it from anything
// upstream) is a deliberate, narrow exception to this codebase's
// "no wall-clock time in deterministic code" rule -- that rule protects
// MatchingEngine::process() specifically (see its own class comment); a
// market-data publish is a genuinely different boundary, modeling the
// instant a fact left the building, exactly like a real exchange's
// feed-publish timestamp. MarketDataPublisherOptions::clock is injectable
// specifically so tests aren't at the mercy of real wall-clock time either.
namespace mdh::exchange::market_data {

using MarketDataSink = std::function<void(const protocol::Event&)>;

struct MarketDataPublisherOptions {
    // Defaults to real wall-clock time (std::chrono::system_clock) if left
    // as nullptr -- see MarketDataPublisher's constructor. Override with a
    // fixed or incrementing stub in tests that need deterministic,
    // reproducible timestamps.
    std::function<Timestamp()> clock;
};

class MarketDataPublisher {
public:
    explicit MarketDataPublisher(MarketDataPublisherOptions options = {});

    // Translates `event` into zero or one protocol::Event and, if one was
    // produced, calls `sink` with it. Zero wire messages is the correct,
    // silent outcome for every private event type (see class-level
    // comment) -- not an error case.
    //
    // *** This is the function you need to implement. ***
    // The dispatch to the four publish_*() helpers below is already
    // wired up in the .cpp -- each helper has a TODO comment describing
    // exactly what it needs to do.
    void publish(const ExchangeEvent& event, const MarketDataSink& sink);

    // Binds a downstream MarketDataSink into an EventSink-shaped callable,
    // so a MarketDataPublisher can be plugged in anywhere an EventSink is
    // expected (e.g. fanned out alongside Ledger::sink(), see ledger.hpp)
    // without the caller needing to know this class exists underneath.
    [[nodiscard]] EventSink sink(MarketDataSink downstream) {
        return [this, downstream = std::move(downstream)](const ExchangeEvent& event) { publish(event, downstream); };
    }

    // Introspection only, e.g. for a test asserting how many wire messages
    // a publisher has emitted so far -- mirrors
    // CommandSequencer::next_sequence()'s own doc comment.
    [[nodiscard]] Sequence next_sequence() const { return next_sequence_; }

private:
    // One helper per publishable event type, each producing exactly one
    // protocol::Event and calling sink(...) with it -- fill these in. See
    // the .cpp file for a field-by-field TODO for each one.
    void publish_book_order_added(const BookOrderAdded& event, const MarketDataSink& sink);
    void publish_book_order_reduced(const BookOrderReduced& event, const MarketDataSink& sink);
    void publish_book_order_removed(const BookOrderRemoved& event, const MarketDataSink& sink);
    void publish_trade_executed(const TradeExecuted& event, const MarketDataSink& sink);

    // Structural glue, already implemented in the .cpp -- not something you
    // need to touch. Hands out this publisher's own market-data sequence
    // numbers, one per wire message actually emitted (never one per
    // ExchangeEvent seen -- a private event that produces no wire message
    // must not consume a sequence number either, the same reasoning
    // MatchingPipeline::submit() applies to CommandSequence).
    [[nodiscard]] Sequence next_sequence_number() { return next_sequence_++; }

    Sequence next_sequence_ = 1;
    MarketDataPublisherOptions options_;
};

} // namespace mdh::exchange::market_data
