#pragma once

#include <optional>
#include <vector>

#include "trader/oms/order_management_system.hpp"
#include "trader/positions/position_tracker.hpp"
#include "trader/risk/trader_risk_engine.hpp"

// Composes TraderRiskEngine + positions::PositionTracker + a real
// OrderManagementSystem behind (almost exactly) OMS's own submit_new_order()/
// cancel_order()/replace_order() surface -- the trader-side
// mirror of exchange::risk::RiskGatedEngine, which composes RiskEngine +
// Ledger + MatchingEngine behind MatchingEngine::process()'s own signature
// the same way. A strategy can hold either a bare
// OrderManagementSystem or this and call the same three methods either way,
// exactly the drop-in-substitution property RiskGatedEngine documents for
// itself.
//
// ── Why submit_new_order() returns something other than a bare ClientOrderId ──
// OrderManagementSystem::submit_new_order() always returns a real,
// tracked id -- even on a wire send failure, because "we tried to reach the
// exchange and failed" is itself a meaningful, trackable outcome (see that
// class's own doc comment: the order is recorded Rejected/InternalError). A
// trader-side risk rejection is a qualitatively earlier, different kind of
// "no": the order never existed at all -- nothing was allocated an id,
// nothing was tracked, nothing was ever going to be sent. Manufacturing a
// fake tracked id for a request the OMS itself never saw would blur that
// distinction for no benefit, so SubmitOutcome makes "never even attempted"
// explicit instead of forcing it through the same channel as "attempted and
// failed."
//
// ── Why cancel_order()/replace_order() are NOT risk-gated ─────────────────
// Exactly exchange::risk::RiskEngine's own documented policy, restated for
// the trader side: a cancel never increases exposure, and a replace either
// shrinks it (safe) or grows it (which the exchange's own RiskGatedEngine
// evaluates when the new size actually reaches it) -- there is nothing this
// class could check ahead of the exchange that would change the outcome, so
// both simply forward to the underlying OMS unchanged.
namespace mdh::trader::risk {

struct SubmitOutcome {
    // nullopt iff rejected locally by this class's own risk check --
    // meaning nothing was sent, and no order was ever tracked by the
    // underlying OrderManagementSystem for this call. See class-level
    // comment for why this is not just an always-present id instead.
    std::optional<exchange::ClientOrderId> client_order_id;
    // Meaningful only when client_order_id is nullopt.
    exchange::RejectReason local_reject_reason = exchange::RejectReason::None;
};

class TraderRiskGatedOms {
public:
    explicit TraderRiskGatedOms(exchange::AccountId account_id, oms::OrderManagementSystem::Sender sender,
                                 oms::OrderManagementSystem::OrderUpdateSink update_sink = nullptr,
                                 TraderRiskLimits limits = {});

    // Checks `risk_` against `positions_`'s current settled exposure first;
    // on failure, sends nothing and returns a SubmitOutcome with
    // client_order_id unset. On success, forwards straight to the
    // underlying OrderManagementSystem exactly as it is.
    [[nodiscard]] SubmitOutcome submit_new_order(InstrumentId instrument_id, Side side, Price price, Quantity quantity,
                                                  exchange::OrderType order_type = exchange::OrderType::Limit,
                                                  exchange::TimeInForce time_in_force = exchange::TimeInForce::GTC);

    // Not risk-gated -- see class-level comment. Thin forwarding to the
    // underlying OMS.
    [[nodiscard]] bool cancel_order(exchange::ClientOrderId client_order_id);
    [[nodiscard]] std::optional<exchange::ClientOrderId> replace_order(exchange::ClientOrderId client_order_id,
                                                                        Price new_price, Quantity new_quantity);

    // Thin forwarding to the underlying OMS's own handle_message() -- the
    // wiring point for a real OrderEntryClient's MessageSink (or a test
    // simulating one), exactly like a bare OrderManagementSystem's. Also
    // what drives positions_ up to date via the OMS's FillSink, invisibly
    // to the caller.
    void handle_message(const protocol::order_entry::Message& message);

    // Read-through introspection -- same shape as OrderManagementSystem's
    // own, plus this class's own positions::PositionTracker view (updated
    // automatically: the underlying OMS's FillSink is wired to `positions_`
    // internally at construction, invisibly to the caller).
    [[nodiscard]] std::optional<oms::ClientOrder> order(exchange::ClientOrderId client_order_id) const;
    [[nodiscard]] std::vector<oms::ClientOrder> orders() const;
    [[nodiscard]] positions::Balance cash() const { return positions_.cash(account_id_); }
    [[nodiscard]] Quantity position(InstrumentId instrument_id) const {
        return positions_.position(account_id_, instrument_id);
    }

    // Seeding only -- forwards to the underlying PositionTracker, see its
    // own doc comment.
    void deposit_cash(positions::Balance amount) { positions_.deposit_cash(account_id_, amount); }
    void deposit_position(InstrumentId instrument_id, Quantity amount) {
        positions_.deposit_position(account_id_, instrument_id, amount);
    }

private:
    exchange::AccountId account_id_;
    TraderRiskEngine risk_;
    positions::PositionTracker positions_;
    // Constructed last: its FillSink constructor argument
    // (`positions_.sink()`) requires `positions_` to already be
    // fully-constructed, and member destruction order (reverse of
    // declaration) must tear oms_ down before positions_ in case any
    // in-flight callback is still touching it -- both reasons this must be
    // declared after positions_, not just constructed after it.
    oms::OrderManagementSystem oms_;
};

} // namespace mdh::trader::risk
