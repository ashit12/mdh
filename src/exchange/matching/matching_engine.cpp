#include "exchange/matching/matching_engine.hpp"

#include <algorithm>
#include <cstddef>
#include <type_traits>
#include <variant>

namespace mdh::exchange {
namespace {

// orders_ allocates a single node per resting order, now the wider of the
// two it used to allocate because it carries the location the book's own
// index used to hold. The bound is set well above that node's size for the
// reason spelled out in matching_book.cpp: libc++ only pools blocks up to a
// quarter of this value, and a node that misses the pool lands in an adhoc
// list with a linear-scan deallocate.
constexpr std::size_t kLargestPooledBlock = 1024;
constexpr std::size_t kMaxBlocksPerChunk = 4096;

} // namespace

MatchingEngine::MatchingEngine(std::span<const InstrumentId> universe, std::size_t expected_resting_orders)
    : pool_(std::make_unique<std::pmr::unsynchronized_pool_resource>(
          std::pmr::pool_options{.max_blocks_per_chunk = kMaxBlocksPerChunk,
                                 .largest_required_pool_block = kLargestPooledBlock})),
      orders_(pool_.get()) {
    // Size the id table once from the widest id rather than letting
    // register_instrument() grow it per instrument, and reserve the books so
    // registration does not move them repeatedly. Both are just avoided
    // work; nothing below depends on them having happened.
    InstrumentId widest = 0;
    for (const InstrumentId instrument_id : universe) {
        if (instrument_id <= kMaxInstrumentId) {
            widest = std::max(widest, instrument_id);
        }
    }
    if (!universe.empty()) {
        slot_of_id_.assign(static_cast<std::size_t>(widest) + 1, kNoSlot);
        // expected_resting_orders counts every instrument's orders, so each
        // book gets an equal share of it. Split evenly rather than cleverly:
        // the engine has no way to know which instruments will be busy, and
        // being wrong per book costs only the relocations a wrong figure was
        // always going to cost. An engine constructed with no universe at
        // all -- replay, which learns its instruments from the journal --
        // reserves nothing, since the share would be a division by the
        // number of instruments it does not yet have.
        expected_orders_per_book_ = expected_resting_orders / universe.size();
        // Sized from the whole universe before the first book is built, so
        // that every book in it gets the same band -- registering them one
        // at a time and re-deriving as the count grew would give the first
        // instruments a wider ladder than the last for no reason.
        band_ticks_ = MatchingBook::band_for(universe.size(), kLadderByteBudget);
    }
    books_.reserve(universe.size());
    by_id_.reserve(universe.size());
    for (const InstrumentId instrument_id : universe) {
        register_instrument(instrument_id);
    }

    orders_.reserve(expected_resting_orders);
}

MatchingEngine::MatchingEngine(std::initializer_list<InstrumentId> universe, std::size_t expected_resting_orders)
    : MatchingEngine(std::span<const InstrumentId>(universe.begin(), universe.size()), expected_resting_orders) {}

bool MatchingEngine::register_instrument(InstrumentId instrument_id) {
    if (instrument_id > kMaxInstrumentId || knows_instrument(instrument_id)) {
        return false;
    }
    if (instrument_id >= slot_of_id_.size()) {
        slot_of_id_.resize(static_cast<std::size_t>(instrument_id) + 1, kNoSlot);
    }

    const auto slot = static_cast<std::uint32_t>(books_.size());
    // Never widens: a universe that keeps growing past what it was sized for
    // narrows the band for the books still to come, which is the only part
    // of the budget still unspent.
    band_ticks_ = std::min(band_ticks_, MatchingBook::band_for(books_.size() + 1, kLadderByteBudget));
    books_.emplace_back(expected_orders_per_book_, band_ticks_);
    slot_of_id_[instrument_id] = slot;

    // Kept sorted on insert so snapshot() can walk it directly. Registration
    // happens at startup (or once per instrument during replay), never on
    // the order path, so the shift this costs is not worth avoiding.
    const auto at = std::lower_bound(by_id_.begin(), by_id_.end(), instrument_id,
                                     [](const auto& entry, InstrumentId id) { return entry.first < id; });
    by_id_.insert(at, {instrument_id, slot});
    return true;
}

ExchangeRestingOrder MatchingEngine::compose(const BookOrder& order, InstrumentId instrument_id) const {
    // Every order on a book has an entry here: the two are written and
    // erased together at every mutation point in this engine.
    const OrderRef& ref = orders_.at(LiveKey{order.account_id, order.client_order_id});
    return ExchangeRestingOrder{
        .exchange_order_id = order.exchange_order_id,
        .client_order_id = order.client_order_id,
        .account_id = order.account_id,
        .price = order.price,
        .original_quantity = ref.original_quantity,
        .remaining_quantity = order.remaining_quantity,
        .order_sequence = ref.order_sequence,
        .instrument_id = instrument_id,
        .side = order.side,
        .time_in_force = order.time_in_force,
    };
}

std::size_t MatchingEngine::out_of_band_levels() const {
    std::size_t total = 0;
    for (const MatchingBook& book : books_) {
        total += book.out_of_band_levels();
    }
    return total;
}

EngineStateSnapshot MatchingEngine::snapshot() const {
    EngineStateSnapshot snap;
    snap.instruments.reserve(by_id_.size());
    for (const auto& [instrument_id, slot] : by_id_) {
        const MatchingBook& book = books_[slot];
        const auto compose_all = [&](const std::vector<BookOrder>& orders) {
            std::vector<ExchangeRestingOrder> composed;
            composed.reserve(orders.size());
            for (const BookOrder& order : orders) {
                composed.push_back(compose(order, instrument_id));
            }
            return composed;
        };
        auto bids = compose_all(book.all_bids());
        auto asks = compose_all(book.all_asks());
        if (bids.empty() && asks.empty()) {
            // Every registered instrument has a book from the moment the
            // engine is constructed, whether or not anything ever traded on
            // it, and a book keeps its entry after its last order leaves.
            // Either way an instrument with nothing resting carries no
            // meaningful state, so it is omitted rather than reported as an
            // empty pair of vectors -- which is also what keeps the snapshot
            // comparable between two engines configured differently.
            continue;
        }
        snap.instruments.push_back(InstrumentBookSnapshot{
            .instrument_id = instrument_id,
            .bids = std::move(bids),
            .asks = std::move(asks),
        });
    }
    return snap;
}

void MatchingEngine::process(const ExchangeCommand& command, const EventSink& sink) {
    std::visit(
        [&](const auto& cmd) {
            using T = std::decay_t<decltype(cmd)>;
            if constexpr (std::is_same_v<T, NewOrderCommand>) {
                process_new_order(cmd, sink);
            } else if constexpr (std::is_same_v<T, CancelOrderCommand>) {
                process_cancel(cmd, sink);
            } else if constexpr (std::is_same_v<T, ReplaceOrderCommand>) {
                process_replace(cmd, sink);
            }
        },
        command);
}

void MatchingEngine::reject_new_order(const NewOrderCommand& command, RejectReason reason, const EventSink& sink) {
    sink(OrderRejected{
        .event_sequence = next_event_sequence_++,
        .command_sequence = command.command_sequence,
        .account_id = command.account_id,
        .client_order_id = command.client_order_id,
        .instrument_id = command.instrument_id,
        .reason = reason,
    });
}

void MatchingEngine::reject_replace_order(const ReplaceOrderCommand& command, RejectReason reason,
                                           const EventSink& sink) {
    sink(OrderRejected{
        .event_sequence = next_event_sequence_++,
        .command_sequence = command.command_sequence,
        .account_id = command.account_id,
        .client_order_id = command.original_client_order_id,
        .instrument_id = command.instrument_id,
        .reason = reason,
    });
}

Quantity MatchingEngine::crossable_quantity(InstrumentId instrument_id, Side incoming_side, Price price,
                                             Quantity quantity) const {
    const Side contra_side = incoming_side == Side::Buy ? Side::Sell : Side::Buy;
    return book_for(instrument_id).crossable_quantity(contra_side, price, quantity);
}

void MatchingEngine::match_and_rest(ExchangeRestingOrder& incoming, CommandSequence command_sequence,
                                     const EventSink& sink) {
    MatchingBook& book = book_for(incoming.instrument_id);
    const Side contra_side = incoming.side == Side::Buy ? Side::Sell : Side::Buy;

    while (incoming.remaining_quantity > 0) {
        const BookOrder* contra = book.front_of_best(contra_side);
        if (contra == nullptr) {
            break;
        }
        const bool crosses = incoming.side == Side::Buy ? incoming.price >= contra->price : incoming.price <= contra->price;
        if (!crosses) {
            break;
        }

        // Everything this iteration still needs once the book has been
        // mutated, read out while `contra` is guaranteed to be valid: the
        // full-fill branch below calls remove_front(), which destroys the
        // order this points at.
        const AccountId contra_account_id = contra->account_id;
        const ClientOrderId contra_client_order_id = contra->client_order_id;
        const ExchangeOrderId contra_exchange_order_id = contra->exchange_order_id;
        const Side contra_order_side = contra->side;
        const Price contra_price = contra->price;

        const Quantity trade_qty = std::min(incoming.remaining_quantity, contra->remaining_quantity);
        incoming.remaining_quantity -= trade_qty;
        const Quantity contra_remaining_after = contra->remaining_quantity - trade_qty;

        const TradeCounterparty aggressor_cp{
            .account_id = incoming.account_id,
            .client_order_id = incoming.client_order_id,
            .exchange_order_id = incoming.exchange_order_id,
            .remaining_quantity = incoming.remaining_quantity,
        };
        const TradeCounterparty resting_cp{
            .account_id = contra_account_id,
            .client_order_id = contra_client_order_id,
            .exchange_order_id = contra_exchange_order_id,
            .remaining_quantity = contra_remaining_after,
        };

        // Price-time priority convention: a trade executes at the resting
        // (passive) order's price, never the incoming (aggressive) order's
        // price -- the resting order is the one that already committed to a
        // price by sitting on the book.
        sink(TradeExecuted{
            .event_sequence = next_event_sequence_++,
            .command_sequence = command_sequence,
            .instrument_id = incoming.instrument_id,
            .price = contra_price,
            .quantity = trade_qty,
            .aggressor_side = incoming.side,
            .buyer = incoming.side == Side::Buy ? aggressor_cp : resting_cp,
            .seller = incoming.side == Side::Buy ? resting_cp : aggressor_cp,
        });

        if (contra_remaining_after == 0) {
            book.remove_front(contra_side);
            orders_.erase(LiveKey{contra_account_id, contra_client_order_id});
            sink(BookOrderRemoved{
                .event_sequence = next_event_sequence_++,
                .instrument_id = incoming.instrument_id,
                .exchange_order_id = contra_exchange_order_id,
                .side = contra_order_side,
                .price = contra_price,
            });
        } else {
            book.reduce_front(contra_side, contra_remaining_after);
            sink(BookOrderReduced{
                .event_sequence = next_event_sequence_++,
                .instrument_id = incoming.instrument_id,
                .exchange_order_id = contra_exchange_order_id,
                .side = contra_order_side,
                .price = contra_price,
                .new_remaining_quantity = contra_remaining_after,
            });
        }
    }
}

void MatchingEngine::rest_remainder_if_applicable(const ExchangeRestingOrder& order, const EventSink& sink) {
    if (order.time_in_force != TimeInForce::GTC || order.remaining_quantity == 0) {
        // IOC/FOK: any remainder is discarded silently -- never accepted as
        // resting, so there is nothing to announce.
        return;
    }
    const MatchingBook::Handle handle = book_for(order.instrument_id).add(BookOrder{
        .exchange_order_id = order.exchange_order_id,
        .client_order_id = order.client_order_id,
        .account_id = order.account_id,
        .price = order.price,
        .remaining_quantity = order.remaining_quantity,
        .side = order.side,
        .time_in_force = order.time_in_force,
    });
    orders_.insert_or_assign(LiveKey{order.account_id, order.client_order_id},
                             OrderRef{
                                 .original_quantity = order.original_quantity,
                                 .order_sequence = order.order_sequence,
                                 .handle = handle,
                                 .instrument_id = order.instrument_id,
                             });
    sink(BookOrderAdded{
        .event_sequence = next_event_sequence_++,
        .instrument_id = order.instrument_id,
        .exchange_order_id = order.exchange_order_id,
        .side = order.side,
        .price = order.price,
        .quantity = order.remaining_quantity,
    });
}

void MatchingEngine::process_new_order(const NewOrderCommand& cmd, const EventSink& sink) {
    // Checked before anything else, price and quantity included: an
    // instrument this engine does not trade is not a malformed order, it is
    // an order sent to the wrong venue, and saying so is more useful than
    // reporting whichever other field happens to also be wrong.
    if (!knows_instrument(cmd.instrument_id)) {
        sink(OrderRejected{
            .event_sequence = next_event_sequence_++,
            .command_sequence = cmd.command_sequence,
            .account_id = cmd.account_id,
            .client_order_id = cmd.client_order_id,
            .instrument_id = cmd.instrument_id,
            .reason = RejectReason::InvalidInstrument,
        });
        return;
    }
    if (cmd.price <= 0) {
        sink(OrderRejected{
            .event_sequence = next_event_sequence_++,
            .command_sequence = cmd.command_sequence,
            .account_id = cmd.account_id,
            .client_order_id = cmd.client_order_id,
            .instrument_id = cmd.instrument_id,
            .reason = RejectReason::InvalidPrice,
        });
        return;
    }
    if (cmd.quantity == 0) {
        sink(OrderRejected{
            .event_sequence = next_event_sequence_++,
            .command_sequence = cmd.command_sequence,
            .account_id = cmd.account_id,
            .client_order_id = cmd.client_order_id,
            .instrument_id = cmd.instrument_id,
            .reason = RejectReason::InvalidQuantity,
        });
        return;
    }

    const LiveKey key{cmd.account_id, cmd.client_order_id};
    if (orders_.contains(key)) {
        sink(OrderRejected{
            .event_sequence = next_event_sequence_++,
            .command_sequence = cmd.command_sequence,
            .account_id = cmd.account_id,
            .client_order_id = cmd.client_order_id,
            .instrument_id = cmd.instrument_id,
            .reason = RejectReason::DuplicateOrderId,
        });
        return;
    }

    if (cmd.time_in_force == TimeInForce::FOK) {
        const Quantity available = crossable_quantity(cmd.instrument_id, cmd.side, cmd.price, cmd.quantity);
        if (available < cmd.quantity) {
            // No OrderAccepted, no book mutation: FOK either fills
            // completely or never touches the book at all.
            sink(OrderRejected{
                .event_sequence = next_event_sequence_++,
                .command_sequence = cmd.command_sequence,
                .account_id = cmd.account_id,
                .client_order_id = cmd.client_order_id,
                .instrument_id = cmd.instrument_id,
                .reason = RejectReason::InsufficientLiquidity,
            });
            return;
        }
    }

    const ExchangeOrderId exchange_order_id = next_exchange_order_id_++;
    ExchangeRestingOrder order{
        .exchange_order_id = exchange_order_id,
        .client_order_id = cmd.client_order_id,
        .account_id = cmd.account_id,
        .price = cmd.price,
        .original_quantity = cmd.quantity,
        .remaining_quantity = cmd.quantity,
        .order_sequence = next_priority_++,
        .instrument_id = cmd.instrument_id,
        .side = cmd.side,
        .time_in_force = cmd.time_in_force,
    };

    sink(OrderAccepted{
        .event_sequence = next_event_sequence_++,
        .command_sequence = cmd.command_sequence,
        .account_id = cmd.account_id,
        .client_order_id = cmd.client_order_id,
        .exchange_order_id = exchange_order_id,
        .instrument_id = cmd.instrument_id,
        .side = cmd.side,
        .price = cmd.price,
        .quantity = cmd.quantity,
        .order_type = cmd.order_type,
        .time_in_force = cmd.time_in_force,
    });

    match_and_rest(order, cmd.command_sequence, sink);
    rest_remainder_if_applicable(order, sink);
}

void MatchingEngine::process_cancel(const CancelOrderCommand& cmd, const EventSink& sink) {
    // Ahead of the order-id lookup, so a cancel naming an instrument this
    // engine does not trade reports that rather than UnknownOrderId -- which
    // would be true but would send the client looking in the wrong place.
    if (!knows_instrument(cmd.instrument_id)) {
        sink(OrderRejected{
            .event_sequence = next_event_sequence_++,
            .command_sequence = cmd.command_sequence,
            .account_id = cmd.account_id,
            .client_order_id = cmd.client_order_id,
            .instrument_id = cmd.instrument_id,
            .reason = RejectReason::InvalidInstrument,
        });
        return;
    }

    const LiveKey key{cmd.account_id, cmd.client_order_id};
    auto it = orders_.find(key);
    if (it == orders_.end() || it->second.instrument_id != cmd.instrument_id) {
        sink(OrderRejected{
            .event_sequence = next_event_sequence_++,
            .command_sequence = cmd.command_sequence,
            .account_id = cmd.account_id,
            .client_order_id = cmd.client_order_id,
            .instrument_id = cmd.instrument_id,
            .reason = RejectReason::UnknownOrderId,
        });
        return;
    }

    const MatchingBook::Handle handle = it->second.handle;
    orders_.erase(it);
    const BookOrder removed = book_for(cmd.instrument_id).remove_at(handle);

    sink(OrderCancelled{
        .event_sequence = next_event_sequence_++,
        .command_sequence = cmd.command_sequence,
        .account_id = cmd.account_id,
        .client_order_id = cmd.client_order_id,
        .exchange_order_id = removed.exchange_order_id,
        .instrument_id = cmd.instrument_id,
    });
    sink(BookOrderRemoved{
        .event_sequence = next_event_sequence_++,
        .instrument_id = cmd.instrument_id,
        .exchange_order_id = removed.exchange_order_id,
        .side = removed.side,
        .price = removed.price,
    });
}

void MatchingEngine::process_replace(const ReplaceOrderCommand& cmd, const EventSink& sink) {
    if (!knows_instrument(cmd.instrument_id)) {
        sink(OrderRejected{
            .event_sequence = next_event_sequence_++,
            .command_sequence = cmd.command_sequence,
            .account_id = cmd.account_id,
            .client_order_id = cmd.original_client_order_id,
            .instrument_id = cmd.instrument_id,
            .reason = RejectReason::InvalidInstrument,
        });
        return;
    }
    if (cmd.new_price <= 0 || cmd.new_quantity == 0) {
        sink(OrderRejected{
            .event_sequence = next_event_sequence_++,
            .command_sequence = cmd.command_sequence,
            .account_id = cmd.account_id,
            .client_order_id = cmd.original_client_order_id,
            .instrument_id = cmd.instrument_id,
            .reason = RejectReason::InvalidReplacement,
        });
        return;
    }

    const LiveKey original_key{cmd.account_id, cmd.original_client_order_id};
    auto it = orders_.find(original_key);
    if (it == orders_.end() || it->second.instrument_id != cmd.instrument_id) {
        sink(OrderRejected{
            .event_sequence = next_event_sequence_++,
            .command_sequence = cmd.command_sequence,
            .account_id = cmd.account_id,
            .client_order_id = cmd.original_client_order_id,
            .instrument_id = cmd.instrument_id,
            .reason = RejectReason::UnknownOrderId,
        });
        return;
    }

    const OrderRef ref = it->second;
    MatchingBook& book = book_for(cmd.instrument_id);
    // Read out everything wanted after the book is mutated, while the
    // reference is still known good -- the priority-losing path below
    // destroys the order it points at.
    const BookOrder& resting = book.at(ref.handle);
    const ExchangeOrderId old_exchange_order_id = resting.exchange_order_id;
    const Side old_side = resting.side;
    const Price old_price = resting.price;
    const TimeInForce old_time_in_force = resting.time_in_force;

    // Priority-preserving path: same price, quantity unchanged or decreased.
    // A price change or a quantity *increase* falls through to the
    // cancel-plus-new path below (see the class-level policy comment in
    // matching_engine.hpp).
    const bool preserves_priority = cmd.new_price == old_price && cmd.new_quantity <= resting.remaining_quantity;

    if (preserves_priority) {
        book.reduce_at(ref.handle, cmd.new_quantity);
        book.set_client_order_id_at(ref.handle, cmd.new_client_order_id);
        // Same order, same place in the queue, addressable under its new
        // client order id: the entry is re-keyed, not rebuilt.
        orders_.erase(it);
        orders_.insert_or_assign(LiveKey{cmd.account_id, cmd.new_client_order_id}, ref);

        sink(OrderReplaced{
            .event_sequence = next_event_sequence_++,
            .command_sequence = cmd.command_sequence,
            .account_id = cmd.account_id,
            .original_client_order_id = cmd.original_client_order_id,
            .new_client_order_id = cmd.new_client_order_id,
            .exchange_order_id = old_exchange_order_id,
            .instrument_id = cmd.instrument_id,
            .new_price = cmd.new_price,
            .new_quantity = cmd.new_quantity,
        });
        sink(BookOrderReduced{
            .event_sequence = next_event_sequence_++,
            .instrument_id = cmd.instrument_id,
            .exchange_order_id = old_exchange_order_id,
            .side = old_side,
            .price = old_price,
            .new_remaining_quantity = cmd.new_quantity,
        });
        return;
    }

    // Priority-losing path: cancel-plus-new. The old order vanishes from the
    // book and a freshly-id'd order re-enters the ordinary matching path, so
    // a reprice into a crossing price trades immediately just like any other
    // aggressive new order.
    book.remove_at(ref.handle);
    orders_.erase(it);

    const ExchangeOrderId new_exchange_order_id = next_exchange_order_id_++;
    sink(OrderReplaced{
        .event_sequence = next_event_sequence_++,
        .command_sequence = cmd.command_sequence,
        .account_id = cmd.account_id,
        .original_client_order_id = cmd.original_client_order_id,
        .new_client_order_id = cmd.new_client_order_id,
        .exchange_order_id = new_exchange_order_id,
        .instrument_id = cmd.instrument_id,
        .new_price = cmd.new_price,
        .new_quantity = cmd.new_quantity,
    });
    sink(BookOrderRemoved{
        .event_sequence = next_event_sequence_++,
        .instrument_id = cmd.instrument_id,
        .exchange_order_id = old_exchange_order_id,
        .side = old_side,
        .price = old_price,
    });

    ExchangeRestingOrder new_order{
        .exchange_order_id = new_exchange_order_id,
        .client_order_id = cmd.new_client_order_id,
        .account_id = cmd.account_id,
        .price = cmd.new_price,
        .original_quantity = cmd.new_quantity,
        .remaining_quantity = cmd.new_quantity,
        .order_sequence = next_priority_++,
        .instrument_id = cmd.instrument_id,
        .side = old_side,
        .time_in_force = old_time_in_force,
    };
    match_and_rest(new_order, cmd.command_sequence, sink);
    rest_remainder_if_applicable(new_order, sink);
}

} // namespace mdh::exchange
