#pragma once

#include <functional>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include "protocol/order_entry/messages.hpp"
#include "trader/oms/client_order.hpp"

// The trader-side order management system: it turns a strategy's
// buy/sell/cancel/replace intent into wire requests, and turns the gateway's
// responses back into a per-order state machine the strategy can query --
// all without knowing anything about sockets. Networking is injected through
// two function-shaped seams rather than owning a client directly:
//
//   - `Sender` (constructor parameter): how a wire Message gets sent.
//   - `handle_message()` (public method): how a wire Message that arrived
//     comes back in.
//
// This is the same pattern MatchingEngine/RiskGatedEngine use for EventSink
// (event_sink.hpp) and MatchingPipeline uses for its Processor seam
// (matching_pipeline.hpp): the logic-heavy class stays fully unit-testable
// with a plain std::function fake standing in for the real transport (see
// tests/test_order_management_system.cpp), and the real transport
// (OrderEntryClient, order_entry_client.hpp) is wired in only at the
// integration/e2e layer (tests/test_oms_gateway_e2e.cpp), exactly mirroring
// how OrderEntryGateway composes RiskGatedEngine into MatchingPipeline.
//
// ── Why a Rejected response needs `pending_action` to interpret ──────────
// protocol::order_entry::Rejected (messages.hpp) carries only account_id,
// client_order_id, instrument_id, and reason -- no "what kind of request
// failed" field, because MatchingEngine emits the exact same OrderRejected
// event shape whether a NewOrderCommand, a CancelOrderCommand, or a
// ReplaceOrderCommand was the one rejected (see matching_engine.cpp's
// process_cancel()/process_replace(), which both emit OrderRejected with
// RejectReason::UnknownOrderId on a bad target order, precisely like a
// failed new order). So a Rejected referencing a client_order_id whose
// tracked state is *not* PendingNew cannot be "the order was rejected" (it
// already wasn't in that state) -- it must mean an in-flight cancel/replace
// attempt on an already-live order failed, leaving that order completely
// unaffected. `pending_action` is what lets handle_message() tell these two
// cases apart instead of misclassifying a failed cancel as the order itself
// having been rejected.
//
// ── Why replace_order() pre-registers the new client_order_id synchronously ──
// A caller needs *some* id to reference the replacement order by
// immediately, before any wire round trip completes -- the same reason
// submit_new_order() itself returns an id synchronously rather than only
// after an Accepted arrives. Mirroring that, replace_order() allocates and
// inserts a new PendingNew-state ClientOrder for the new id up front; a
// later Replaced response (which carries both the original and new
// client_order_id) fills in that pre-registered entry rather than creating
// it from scratch.
namespace mdh::trader::oms {

// One trade fill, addressed to a single tracked order -- OrderManagementSystem's
// own output contract for anything downstream that needs to *settle* a
// trade -- the position tracker -- rather than just observe an order's
// lifecycle state. Deliberately a distinct type from
// protocol::order_entry::TradeReport, which carries only what fit on the
// wire: TradeReport has no `side` field (see messages.hpp's own comment),
// because the gateway already knows which connection it's routing to and
// doesn't need to re-state per-account context in an account-addressed
// message -- but a caller that only sees TradeReport in isolation cannot
// tell a buy fill from a sell fill without separately remembering the
// order's own side (as recorded by an earlier Accepted, and carried forward
// across any Replaced -- exactly what OrderManagementSystem's own orders_
// map already does). Fill carries that side through directly, computed
// once, here, instead of every downstream consumer re-deriving it.
struct Fill {
    exchange::AccountId account_id;
    exchange::ClientOrderId client_order_id;
    InstrumentId instrument_id;
    Side side;
    Price price;      // the trade price for this specific fill
    Quantity quantity; // the incremental amount filled by this specific TradeReport

    bool operator==(const Fill&) const = default;
};

class OrderManagementSystem {
public:
    // Sends `message` over the wire. Returns false if the send itself
    // failed (e.g. the connection has already dropped) -- distinguished
    // from a wire-level Rejected, which is a *successful* send that the
    // exchange then declined; see handle_message()'s own doc comment.
    using Sender = std::function<bool(const protocol::order_entry::Message&)>;

    // Invoked once per observable change to a tracked order -- after every
    // send that changes its state (submit_new_order(), a failed
    // cancel/replace send) and after every handle_message() call that finds
    // a match in orders_. Never invoked while this class's internal mutex
    // is held (see notify()'s own doc comment), so it is always safe for a
    // caller's sink to re-enter this OMS (e.g. call cancel_order() upon
    // seeing a fill).
    using OrderUpdateSink = std::function<void(const ClientOrder&)>;

    // Invoked once per TradeReport, in addition to (and after) the
    // corresponding OrderUpdateSink call for the same event -- the seam the
    // position tracker hooks into via sink() below, mirroring how the
    // exchange's ledger subscribes to events. Optional, like OrderUpdateSink:
    // a caller with no need to settle fills simply never sets it.
    using FillSink = std::function<void(const Fill&)>;

    // `fill_sink` is typically `positions::PositionTracker::sink()`
    // (trader/positions/position_tracker.hpp) -- see FillSink's own comment.
    explicit OrderManagementSystem(exchange::AccountId account_id, Sender sender, OrderUpdateSink update_sink = nullptr,
                                    FillSink fill_sink = nullptr);

    // Assigns a new, never-before-used client_order_id, records the order
    // as PendingNew, and sends the corresponding NewOrder message. Always
    // returns the assigned id, even if the send itself fails -- in that
    // case the order is immediately (and synchronously, before this
    // function returns) transitioned to Rejected with
    // RejectReason::InternalError and reported via `update_sink`, since a
    // send failure means the exchange will never see this order at all and
    // there is no other channel (no bool return here) to surface that.
    [[nodiscard]] exchange::ClientOrderId submit_new_order(InstrumentId instrument_id, Side side, Price price,
                                                            Quantity quantity,
                                                            exchange::OrderType order_type = exchange::OrderType::Limit,
                                                            exchange::TimeInForce time_in_force = exchange::TimeInForce::GTC);

    // Sends a CancelOrder for `client_order_id`. Returns false, sending
    // nothing, if the id is unknown, already terminal, or already has a
    // cancel/replace in flight -- true means the request was sent
    // successfully (not that it will succeed at the exchange; that answer
    // arrives later via handle_message()).
    [[nodiscard]] bool cancel_order(exchange::ClientOrderId client_order_id);

    // Sends a ReplaceOrder for `client_order_id`. Returns the new
    // client_order_id the replacement will be known by if the request was
    // sent successfully (see class-level comment on why this is
    // pre-registered), or std::nullopt under the same conditions
    // cancel_order() returns false for.
    [[nodiscard]] std::optional<exchange::ClientOrderId> replace_order(exchange::ClientOrderId client_order_id,
                                                                        Price new_price, Quantity new_quantity);

    // Feeds one decoded gateway -> client Message into this OMS's state
    // machine -- the counterpart to `Sender` above. Called by
    // OrderEntryClient's reader thread in the real (networked) case, or
    // directly by a test standing in for the wire. A message referencing a
    // client_order_id this OMS never assigned (e.g. a stray/duplicate, or
    // simply a bug elsewhere) is silently ignored -- there is no tracked
    // order to update. NewOrder/CancelOrder/ReplaceOrder (client -> gateway
    // types) arriving here would indicate a transport-layer bug; also
    // ignored, since there is nothing meaningful to do with them.
    void handle_message(const protocol::order_entry::Message& message);

    [[nodiscard]] std::optional<ClientOrder> order(exchange::ClientOrderId client_order_id) const;
    [[nodiscard]] std::vector<ClientOrder> orders() const;

private:
    void on_accepted(const protocol::order_entry::Accepted& event);
    void on_rejected(const protocol::order_entry::Rejected& event);
    void on_cancelled(const protocol::order_entry::Cancelled& event);
    void on_replaced(const protocol::order_entry::Replaced& event);
    void on_trade_report(const protocol::order_entry::TradeReport& event);

    // Transitions `client_order_id` straight to Rejected/InternalError and
    // reports it -- the submit_new_order() send-failure path described in
    // its own doc comment above.
    void mark_send_failed(exchange::ClientOrderId client_order_id);
    // Clears pending_action on `client_order_id` with no other state
    // change -- the cancel_order()/replace_order() send-failure path
    // (their own bool/optional return already tells the caller it failed;
    // no order-visible change actually occurred, so no notify() here).
    void clear_pending_action(exchange::ClientOrderId client_order_id);

    // Invokes update_sink_ (if set) with a copy of `updated_order`. Always
    // called with mutex_ NOT held by the caller -- every call site above
    // copies the post-mutation ClientOrder out from under the lock first,
    // specifically so a caller's sink can safely re-enter this OMS (e.g.
    // call cancel_order() from within it) without deadlocking on mutex_.
    void notify(const ClientOrder& updated_order);

    exchange::AccountId account_id_;
    Sender sender_;
    OrderUpdateSink update_sink_;
    FillSink fill_sink_;

    mutable std::mutex mutex_;
    exchange::ClientOrderId next_client_order_id_ = 1;
    std::unordered_map<exchange::ClientOrderId, ClientOrder> orders_;
};

} // namespace mdh::trader::oms
