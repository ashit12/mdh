#pragma once

#include <functional>
#include <unordered_map>

#include "book/book_manager.hpp"
#include "common/types.hpp"
#include "protocol/messages.hpp"

// The trader-side strategy runtime (Milestone 10) -- the piece named
// "Strategy runtime" in docs/end_to_end_architecture.md's system diagram,
// sitting between the existing, untouched market-data reconstruction
// pipeline (book::BookManager/OrderBook, protocol/messages.hpp) and however
// many strategies (MarketMakerStrategy, market_maker_strategy.hpp;
// CrossVenueArbStrategy, cross_venue_arbitrage_strategy.hpp) a caller wants
// to run side by side.
//
// ── Why this is a dispatch table of std::functions, not a Strategy base class ──
// Every other output boundary in this codebase is a plain std::function
// alias rather than a virtual interface -- see exchange/core/event_sink.hpp's
// own comment on why (every current use is satisfied by a callable, and
// std::function avoids imposing vtable/ownership machinery nothing here
// needs). A strategy is just another consumer of "the book changed";
// MarketMakerStrategy and CrossVenueArbStrategy each expose a plain method
// plus a small `..._sink()` convenience method that wraps it in a
// BookUpdateSink, exactly mirroring how exchange::ledger::Ledger::sink()
// and trader::positions::PositionTracker::sink() wrap apply()/apply().
//
// ── Why this is NOT wired into replay::apply_frame_result() itself ───────
// That function (Milestone 1) is heavily tested and reused by both file
// replay and UDP replay; it has no reason to know strategies exist.
// StrategyRuntime::on_event() is meant to be called by whatever already
// drives BookManager (a live UDP listener loop, or a test), immediately
// after apply_frame_result() returns -- one extra call at an existing call
// site, not a change to Milestone 1-4's own code. Per docs/exchange_flow.md's
// own "Integration status" section, no live UDP feed is wired up to a
// running gateway yet (MarketDataPublisher isn't bound into the gateway's
// matching thread), so this class's own tests exercise it against
// synthetic protocol::Event values the same way replay's own tests do,
// not against a live socket -- wiring that end-to-end live path remains
// future, out-of-scope integration work, not something this milestone
// silently expands into.
namespace mdh::trader::strategies {

using BookUpdateSink = std::function<void(InstrumentId, const book::OrderBook&)>;

class StrategyRuntime {
public:
    // Does not own whatever `sink` closes over -- same non-owning
    // convention exchange::risk::RiskGatedEngine documents for the
    // MatchingEngine/Ledger references it holds. Multiple sinks may
    // subscribe to the same instrument_id (e.g. a market maker and an
    // arbitrage strategy both watching the same book); all are called, in
    // subscription order, on every update.
    void subscribe(InstrumentId instrument_id, BookUpdateSink sink);

    // Extracts `event`'s instrument_id (every protocol::Event variant
    // carries one) and calls every sink subscribed to that instrument,
    // passing `books.book_for(instrument_id)` -- the book as it stands
    // *after* `event` has already been applied to `books` by the caller
    // (this function does not itself mutate `books` beyond book_for()'s
    // own lazy-creation side effect for an instrument never seen before).
    void on_event(const protocol::Event& event, book::BookManager& books);

private:
    std::unordered_multimap<InstrumentId, BookUpdateSink> sinks_;
};

} // namespace mdh::trader::strategies
