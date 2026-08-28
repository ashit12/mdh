#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>
#include <variant>

#include "common/monotonic_ticks.hpp"
#include "exchange/core/commands.hpp"
#include "exchange/core/types.hpp"
#include "protocol/order_entry/messages.hpp"

// Process-wide, opt-in order-path latency tracing.
//
// Disabled by default: every stamp is a single relaxed atomic load and a
// return. When enabled, timestamps are stored in a pre-allocated slot table
// keyed by (account_id, client_order_id). Stamps are lock-free stores --
// the matching thread never takes a mutex for metrics.
//
// Tracing does not change command sequencing, matching, risk, ledger, or
// wire encoding. A full slot table or a hash collision drops the sample,
// never the order.
namespace mdh::latency {

using exchange::AccountId;
using exchange::ClientOrderId;
using exchange::ExchangeCommand;
using exchange::ReplaceOrderCommand;

inline constexpr std::size_t kDefaultSlotCount = 1 << 16;

enum class Stage : std::uint8_t {
    ClientSubmit = 0,          // T0
    ServerDecoded = 1,         // T1
    ExchangeBegin = 2,         // T2
    FirstExecutionEvent = 3,   // T3a -- first private report produced
    ExchangeEnd = 4,           // T3  -- processor returned
    WriterQueued = 5,          // T4a -- handed to connection outbound queue
    SocketWritten = 6,         // T4  -- write() completed
    ClientDecoded = 7,         // T5
};

struct TraceSnapshot {
    AccountId account_id = 0;
    ClientOrderId client_order_id = 0;
    std::uint64_t t0_client_submit = 0;
    std::uint64_t t1_server_decoded = 0;
    std::uint64_t t2_exchange_begin = 0;
    std::uint64_t t3_first_event = 0;
    std::uint64_t t3_exchange_end = 0;
    std::uint64_t t4_writer_queued = 0;
    std::uint64_t t4_socket_written = 0;
    std::uint64_t t5_client_first = 0;
    std::uint64_t t5_client_last = 0;
    std::uint32_t reports_generated = 0;
    std::uint32_t reports_decoded = 0;
    protocol::order_entry::MessageType first_report_type = protocol::order_entry::MessageType::Accepted;
};

[[nodiscard]] inline bool inbound_key(const protocol::order_entry::Message& message, AccountId& account_id,
                                      ClientOrderId& client_order_id) {
    return std::visit(
        [&](const auto& msg) -> bool {
            using T = std::decay_t<decltype(msg)>;
            if constexpr (std::is_same_v<T, protocol::order_entry::NewOrder> ||
                          std::is_same_v<T, protocol::order_entry::CancelOrder>) {
                account_id = msg.account_id;
                client_order_id = msg.client_order_id;
                return true;
            } else if constexpr (std::is_same_v<T, protocol::order_entry::ReplaceOrder>) {
                account_id = msg.account_id;
                client_order_id = msg.original_client_order_id;
                return true;
            } else {
                return false;
            }
        },
        message);
}

[[nodiscard]] inline bool report_key(const protocol::order_entry::Message& message, AccountId& account_id,
                                     ClientOrderId& client_order_id) {
    return std::visit(
        [&](const auto& msg) -> bool {
            using T = std::decay_t<decltype(msg)>;
            if constexpr (std::is_same_v<T, protocol::order_entry::Accepted> ||
                          std::is_same_v<T, protocol::order_entry::Rejected> ||
                          std::is_same_v<T, protocol::order_entry::Cancelled> ||
                          std::is_same_v<T, protocol::order_entry::TradeReport>) {
                account_id = msg.account_id;
                client_order_id = msg.client_order_id;
                return true;
            } else if constexpr (std::is_same_v<T, protocol::order_entry::Replaced>) {
                account_id = msg.account_id;
                client_order_id = msg.original_client_order_id;
                return true;
            } else {
                return false;
            }
        },
        message);
}

[[nodiscard]] inline bool command_key(const ExchangeCommand& command, AccountId& account_id,
                                      ClientOrderId& client_order_id) {
    return std::visit(
        [&](const auto& cmd) -> bool {
            using T = std::decay_t<decltype(cmd)>;
            account_id = cmd.account_id;
            if constexpr (std::is_same_v<T, ReplaceOrderCommand>) {
                client_order_id = cmd.original_client_order_id;
            } else {
                client_order_id = cmd.client_order_id;
            }
            return true;
        },
        command);
}

[[nodiscard]] inline protocol::order_entry::MessageType report_type(const protocol::order_entry::Message& message) {
    return std::visit(
        [](const auto& msg) {
            using T = std::decay_t<decltype(msg)>;
            if constexpr (std::is_same_v<T, protocol::order_entry::Accepted>) {
                return protocol::order_entry::MessageType::Accepted;
            } else if constexpr (std::is_same_v<T, protocol::order_entry::Rejected>) {
                return protocol::order_entry::MessageType::Rejected;
            } else if constexpr (std::is_same_v<T, protocol::order_entry::Cancelled>) {
                return protocol::order_entry::MessageType::Cancelled;
            } else if constexpr (std::is_same_v<T, protocol::order_entry::Replaced>) {
                return protocol::order_entry::MessageType::Replaced;
            } else if constexpr (std::is_same_v<T, protocol::order_entry::TradeReport>) {
                return protocol::order_entry::MessageType::TradeReport;
            } else {
                return protocol::order_entry::MessageType::NewOrder;
            }
        },
        message);
}

class Tracer {
public:
    static Tracer& instance() {
        static Tracer tracer;
        return tracer;
    }

    // Allocates the slot table on first enable. disable() only clears the
    // enabled flag -- freeing the table while a reader/matching/writer
    // thread can still be inside a stamp() after the enabled_ check is a
    // data race. The table lives until process exit.
    void enable(std::size_t slot_count = kDefaultSlotCount) {
        if (slot_count == 0) {
            slot_count = kDefaultSlotCount;
        }
        std::size_t n = 1;
        while (n < slot_count) {
            n <<= 1;
        }
        if (slots_ == nullptr) {
            slots_ = std::make_unique<Slot[]>(n);
            mask_.store(n - 1, std::memory_order_relaxed);
        }
        enabled_.store(true, std::memory_order_release);
    }

    void disable() { enabled_.store(false, std::memory_order_release); }

    [[nodiscard]] bool enabled() const { return enabled_.load(std::memory_order_relaxed); }

    void stamp_client_submit(const protocol::order_entry::Message& message) {
        AccountId account_id = 0;
        ClientOrderId client_order_id = 0;
        if (!inbound_key(message, account_id, client_order_id)) {
            return;
        }
        Slot* slot = begin_slot(account_id, client_order_id);
        if (slot == nullptr) {
            return;
        }
        slot->t0.store(monotonic_ticks(), std::memory_order_release);
    }

    void stamp_server_decoded(const protocol::order_entry::Message& message) {
        stamp_inbound(message, &Slot::t1);
    }

    void stamp_exchange_begin(const ExchangeCommand& command) {
        stamp_command(command, &Slot::t2);
    }

    void stamp_exchange_end(const ExchangeCommand& command) {
        stamp_command(command, &Slot::t3_end);
    }

    void stamp_first_event_if_unset(const protocol::order_entry::Message& message) {
        AccountId account_id = 0;
        ClientOrderId client_order_id = 0;
        if (!report_key(message, account_id, client_order_id)) {
            return;
        }
        Slot* slot = find_slot(account_id, client_order_id);
        if (slot == nullptr) {
            return;
        }
        const std::uint64_t now = monotonic_ticks();
        std::uint64_t expected = 0;
        if (slot->t3_first.compare_exchange_strong(expected, now, std::memory_order_release,
                                                   std::memory_order_relaxed)) {
            slot->first_report_type.store(static_cast<std::uint8_t>(report_type(message)),
                                          std::memory_order_relaxed);
        }
        slot->reports_generated.fetch_add(1, std::memory_order_relaxed);
    }

    void stamp_writer_queued(const protocol::order_entry::Message& message) {
        Slot* slot = find_from_report(message);
        if (slot == nullptr) {
            return;
        }
        std::uint64_t expected = 0;
        const std::uint64_t now = monotonic_ticks();
        slot->t4_queued.compare_exchange_strong(expected, now, std::memory_order_release,
                                                std::memory_order_relaxed);
    }

    void stamp_socket_written(const protocol::order_entry::Message& message) {
        Slot* slot = find_from_report(message);
        if (slot == nullptr) {
            return;
        }
        std::uint64_t expected = 0;
        const std::uint64_t now = monotonic_ticks();
        slot->t4_written.compare_exchange_strong(expected, now, std::memory_order_release,
                                                 std::memory_order_relaxed);
    }

    void stamp_client_decoded(const protocol::order_entry::Message& message) {
        AccountId account_id = 0;
        ClientOrderId client_order_id = 0;
        if (!report_key(message, account_id, client_order_id)) {
            return;
        }
        Slot* slot = find_slot(account_id, client_order_id);
        if (slot == nullptr) {
            return;
        }
        const std::uint64_t now = monotonic_ticks();
        std::uint64_t expected = 0;
        slot->t5_first.compare_exchange_strong(expected, now, std::memory_order_release,
                                               std::memory_order_relaxed);
        slot->t5_last.store(now, std::memory_order_release);
        slot->reports_decoded.fetch_add(1, std::memory_order_relaxed);
    }

    [[nodiscard]] std::optional<TraceSnapshot> snapshot(AccountId account_id, ClientOrderId client_order_id) const {
        const Slot* slot = find_slot(account_id, client_order_id);
        if (slot == nullptr) {
            return std::nullopt;
        }
        TraceSnapshot out;
        out.account_id = slot->account_id.load(std::memory_order_acquire);
        out.client_order_id = slot->client_order_id.load(std::memory_order_relaxed);
        if (out.account_id != account_id || out.client_order_id != client_order_id) {
            return std::nullopt;
        }
        out.t0_client_submit = slot->t0.load(std::memory_order_relaxed);
        out.t1_server_decoded = slot->t1.load(std::memory_order_relaxed);
        out.t2_exchange_begin = slot->t2.load(std::memory_order_relaxed);
        out.t3_first_event = slot->t3_first.load(std::memory_order_relaxed);
        out.t3_exchange_end = slot->t3_end.load(std::memory_order_relaxed);
        out.t4_writer_queued = slot->t4_queued.load(std::memory_order_relaxed);
        out.t4_socket_written = slot->t4_written.load(std::memory_order_relaxed);
        out.t5_client_first = slot->t5_first.load(std::memory_order_acquire);
        out.t5_client_last = slot->t5_last.load(std::memory_order_relaxed);
        out.reports_generated = slot->reports_generated.load(std::memory_order_relaxed);
        out.reports_decoded = slot->reports_decoded.load(std::memory_order_relaxed);
        out.first_report_type =
            static_cast<protocol::order_entry::MessageType>(slot->first_report_type.load(std::memory_order_relaxed));
        return out;
    }

private:
    struct Slot {
        std::atomic<AccountId> account_id{0};
        std::atomic<ClientOrderId> client_order_id{0};
        std::atomic<std::uint64_t> t0{0};
        std::atomic<std::uint64_t> t1{0};
        std::atomic<std::uint64_t> t2{0};
        std::atomic<std::uint64_t> t3_first{0};
        std::atomic<std::uint64_t> t3_end{0};
        std::atomic<std::uint64_t> t4_queued{0};
        std::atomic<std::uint64_t> t4_written{0};
        std::atomic<std::uint64_t> t5_first{0};
        std::atomic<std::uint64_t> t5_last{0};
        std::atomic<std::uint32_t> reports_generated{0};
        std::atomic<std::uint32_t> reports_decoded{0};
        std::atomic<std::uint8_t> first_report_type{0};
    };

    [[nodiscard]] static std::size_t mix(AccountId account_id, ClientOrderId client_order_id, std::size_t mask) {
        const std::uint64_t mixed =
            (static_cast<std::uint64_t>(account_id) * 0x9e3779b97f4a7c15ULL) ^ static_cast<std::uint64_t>(client_order_id);
        return static_cast<std::size_t>(mixed) & mask;
    }

    [[nodiscard]] bool tracing() const { return enabled_.load(std::memory_order_acquire); }

    Slot* begin_slot(AccountId account_id, ClientOrderId client_order_id) {
        if (!tracing()) {
            return nullptr;
        }
        const std::size_t mask = mask_.load(std::memory_order_relaxed);
        if (slots_ == nullptr) {
            return nullptr;
        }
        Slot& slot = slots_[mix(account_id, client_order_id, mask)];
        slot.account_id.store(account_id, std::memory_order_relaxed);
        slot.client_order_id.store(client_order_id, std::memory_order_relaxed);
        slot.t1.store(0, std::memory_order_relaxed);
        slot.t2.store(0, std::memory_order_relaxed);
        slot.t3_first.store(0, std::memory_order_relaxed);
        slot.t3_end.store(0, std::memory_order_relaxed);
        slot.t4_queued.store(0, std::memory_order_relaxed);
        slot.t4_written.store(0, std::memory_order_relaxed);
        slot.t5_first.store(0, std::memory_order_relaxed);
        slot.t5_last.store(0, std::memory_order_relaxed);
        slot.reports_generated.store(0, std::memory_order_relaxed);
        slot.reports_decoded.store(0, std::memory_order_relaxed);
        slot.first_report_type.store(0, std::memory_order_relaxed);
        return &slot;
    }

    [[nodiscard]] Slot* find_slot(AccountId account_id, ClientOrderId client_order_id) {
        return const_cast<Slot*>(static_cast<const Tracer*>(this)->find_slot(account_id, client_order_id));
    }

    [[nodiscard]] const Slot* find_slot(AccountId account_id, ClientOrderId client_order_id) const {
        if (!tracing()) {
            return nullptr;
        }
        const std::size_t mask = mask_.load(std::memory_order_relaxed);
        if (slots_ == nullptr) {
            return nullptr;
        }
        const Slot& slot = slots_[mix(account_id, client_order_id, mask)];
        if (slot.account_id.load(std::memory_order_acquire) != account_id) {
            return nullptr;
        }
        if (slot.client_order_id.load(std::memory_order_relaxed) != client_order_id) {
            return nullptr;
        }
        return &slot;
    }

    [[nodiscard]] Slot* find_from_report(const protocol::order_entry::Message& message) {
        AccountId account_id = 0;
        ClientOrderId client_order_id = 0;
        if (!report_key(message, account_id, client_order_id)) {
            return nullptr;
        }
        return find_slot(account_id, client_order_id);
    }

    void stamp_inbound(const protocol::order_entry::Message& message, std::atomic<std::uint64_t> Slot::* field) {
        AccountId account_id = 0;
        ClientOrderId client_order_id = 0;
        if (!inbound_key(message, account_id, client_order_id)) {
            return;
        }
        Slot* slot = find_slot(account_id, client_order_id);
        if (slot == nullptr) {
            return;
        }
        (slot->*field).store(monotonic_ticks(), std::memory_order_release);
    }

    void stamp_command(const ExchangeCommand& command, std::atomic<std::uint64_t> Slot::* field) {
        AccountId account_id = 0;
        ClientOrderId client_order_id = 0;
        if (!command_key(command, account_id, client_order_id)) {
            return;
        }
        Slot* slot = find_slot(account_id, client_order_id);
        if (slot == nullptr) {
            return;
        }
        (slot->*field).store(monotonic_ticks(), std::memory_order_release);
    }

    std::atomic<bool> enabled_{false};
    std::atomic<std::size_t> mask_{0};
    std::unique_ptr<Slot[]> slots_;
};

struct ScopedEnable {
    explicit ScopedEnable(std::size_t slot_count = kDefaultSlotCount) { Tracer::instance().enable(slot_count); }
    ~ScopedEnable() { Tracer::instance().disable(); }

    ScopedEnable(const ScopedEnable&) = delete;
    ScopedEnable& operator=(const ScopedEnable&) = delete;
};

[[nodiscard]] inline Tracer& tracer() { return Tracer::instance(); }

} // namespace mdh::latency
