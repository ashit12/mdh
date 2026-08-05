#include "exchange/risk/risk_gated_engine.hpp"

#include <variant>

namespace mdh::exchange::risk {

RiskGatedEngine::RiskGatedEngine(MatchingEngine& engine, ledger::Ledger& ledger, RiskLimits limits)
    : engine_(engine), ledger_(ledger), risk_(limits) {}

void RiskGatedEngine::process(const ExchangeCommand& command, const EventSink& sink) {
    if (const auto* new_order = std::get_if<NewOrderCommand>(&command)) {
        const RejectReason reason = risk_.check(*new_order, ledger_);
        if (reason != RejectReason::None) {
            // Rejected before ever reaching process(): no OrderAccepted was
            // emitted, so Ledger has nothing to reserve for this command --
            // reject_new_order() only emits the event, using MatchingEngine's
            // own event_sequence counter so numbering stays globally gapless.
            engine_.reject_new_order(*new_order, reason, sink);
            return;
        }
    }

    engine_.process(command, [this, &sink](const ExchangeEvent& event) {
        ledger_.apply(event);
        sink(event);
    });
}

} // namespace mdh::exchange::risk
