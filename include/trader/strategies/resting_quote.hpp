#pragma once

#include <optional>

#include "common/types.hpp"
#include "exchange/core/types.hpp"
#include "trader/oms/client_order.hpp"
#include "trader/risk/trader_risk_gated_oms.hpp"

// One resting quote on one side of one instrument, kept at a desired price.
// `update()` is the whole class: given "where I want to be quoting," it
// submits, holds, replaces, cancels or re-submits as needed, and remembers
// which client_order_id is currently believed to be its order.
//
// ── Why this is a value that takes the OMS as a parameter ─────────────────
// A quote ladder needs a container of these, one per level, and a class
// holding a reference member is not assignable -- which makes it awkward to
// put in a vector for no benefit. Passing the TraderRiskGatedOms to
// update() instead leaves this class holding nothing but its own
// bookkeeping, which also means a unit test can drive it against a fake
// sender with no lifetime setup at all.
//
// ── Why "desired price" is an optional ────────────────────────────────────
// std::nullopt means "withdraw this quote and do not replace it" -- what an
// inventory cap needs. Price 0 would be a real, meaningful (if nonsensical)
// price and could not express it, the same distinction MarketMakerStrategy::
// update_side() already draws for its own single quote.
//
// Not thread-safe, and deliberately not: its owner is a strategy, and every
// strategy in this codebase is driven from one thread (see
// LadderMarketMaker and MomentumStrategy on which thread each of theirs is).
namespace mdh::trader::strategies {

class RestingQuote {
public:
    RestingQuote(InstrumentId instrument_id, Side side, Quantity size, Price requote_threshold)
        : instrument_id_(instrument_id), side_(side), size_(size), requote_threshold_(requote_threshold) {}

    // Drives this quote toward `desired_price`, sending at most one wire
    // request. Returns true if one was sent.
    //
    // Deliberately sends nothing while an Accept or a previous
    // cancel/replace is still in flight: piling a second request onto an
    // order whose state is not yet settled is how a strategy ends up with
    // more live orders than it thinks it has.
    bool update(risk::TraderRiskGatedOms& trading, std::optional<Price> desired_price) {
        if (order_id_) {
            const auto order = trading.order(*order_id_);
            if (!order || oms::is_terminal(order->state)) {
                order_id_.reset(); // done with that id -- fall through and quote afresh
            } else if (order->state == oms::ClientOrderState::PendingNew ||
                        order->pending_action != oms::PendingAction::None) {
                return false;
            } else if (!desired_price) {
                return trading.cancel_order(*order_id_);
            } else if (abs_diff(order->price, *desired_price) < requote_threshold_ &&
                        order->remaining_quantity == size_) {
                return false; // close enough, and the right size -- leave it alone and keep its queue position
            } else {
                // A failed replace (nullopt) leaves order_id_ pointing at the
                // still-live original, which is correct: nothing changed at
                // the exchange, so there is nothing to re-point at.
                if (const auto new_id = trading.replace_order(*order_id_, *desired_price, size_)) {
                    order_id_ = new_id;
                    return true;
                }
                return false;
            }
        }

        if (!desired_price) {
            return false;
        }
        const auto outcome = trading.submit_new_order(instrument_id_, side_, *desired_price, size_);
        // nullopt means locally risk-rejected: nothing was sent and nothing
        // is tracked, so this quote simply stays absent and is retried on the
        // next call.
        order_id_ = outcome.client_order_id;
        return outcome.client_order_id.has_value();
    }

    // Cancels this quote and forgets it -- for shutdown, where the caller
    // wants the book clean rather than a quote it will stop maintaining.
    bool withdraw(risk::TraderRiskGatedOms& trading) {
        if (!order_id_) {
            return false;
        }
        const bool sent = trading.cancel_order(*order_id_);
        order_id_.reset();
        return sent;
    }

    [[nodiscard]] std::optional<exchange::ClientOrderId> order_id() const { return order_id_; }
    [[nodiscard]] Side side() const { return side_; }
    [[nodiscard]] Quantity size() const { return size_; }

private:
    [[nodiscard]] static Price abs_diff(Price a, Price b) { return a > b ? a - b : b - a; }

    InstrumentId instrument_id_;
    Side side_;
    Quantity size_;
    Price requote_threshold_;
    std::optional<exchange::ClientOrderId> order_id_;
};

} // namespace mdh::trader::strategies
