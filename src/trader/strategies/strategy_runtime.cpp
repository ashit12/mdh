#include "trader/strategies/strategy_runtime.hpp"

#include <variant>

namespace mdh::trader::strategies {

void StrategyRuntime::subscribe(InstrumentId instrument_id, BookUpdateSink sink) {
    sinks_.emplace(instrument_id, std::move(sink));
}

void StrategyRuntime::on_event(const protocol::Event& event, book::BookManager& books) {
    const InstrumentId instrument_id = std::visit([](const auto& e) { return e.instrument_id; }, event);
    const auto& order_book = books.book_for(instrument_id);
    const auto [begin, end] = sinks_.equal_range(instrument_id);
    for (auto it = begin; it != end; ++it) {
        it->second(instrument_id, order_book);
    }
}

} // namespace mdh::trader::strategies
