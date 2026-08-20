// Milestone 15: heap allocation behaviour and memory footprint of the
// matching hot path.
//
// The question this answers is not "is the engine fast" but "what does it
// ask the allocator for". It began as the measurement that justified the
// pool, the slab, and the intrusive FIFO levels, and it stays as the guard
// on all three: an allocation per resting order reappearing here means some
// structure has quietly gone back to allocating one node per element.
//
// ── Method ─────────────────────────────────────────────────────────────────
// Every global operator new/delete overload is replaced in this translation
// unit, so the counters below see all C++ heap traffic in the process, not a
// sampled or estimated subset. Block sizes come from the platform allocator
// itself (malloc_size / malloc_usable_size) rather than from the requested
// size, which means "bytes" here includes real allocator rounding instead of
// pretending it away.
//
// This is a separate executable from the other benchmarks on purpose:
// replacing operator new globally would otherwise perturb every timing
// measurement in the same binary.
//
// Nothing about the engine's behaviour is altered to obtain these numbers.
// The commands are ordinary commands and go through the ordinary public
// process() entry point.
//
// Run from a Release build only -- a Debug build's container code allocates
// differently (libc++ debug iterators, no inlining).
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <new>
#include <string>
#include <vector>

#if defined(__APPLE__)
#include <mach/mach.h>
#include <malloc/malloc.h>
#elif defined(__linux__)
#include <malloc.h>
#endif

#include "exchange/matching/matching_engine.hpp"
#include "exchange/testing/matching_scenarios.hpp"
#include "exchange/testing/matching_workload.hpp"

using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::testing;

// ── Global allocation counters ─────────────────────────────────────────────
//
// Plain namespace-scope integers with static initialisation only: operator
// new runs before any dynamic initialiser in the program, so anything
// requiring construction would be a use-before-init hazard. This binary is
// single-threaded, which is what makes non-atomic counters safe here.
namespace {

std::uint64_t g_allocation_count = 0;
std::uint64_t g_deallocation_count = 0;
std::uint64_t g_bytes_allocated = 0;
std::uint64_t g_bytes_freed = 0;

// Direct-indexed histogram of allocation block sizes. Small sizes are what
// container nodes look like, and naming which node type dominates is the
// whole point of the footprint section -- so the distribution is recorded
// rather than inferred. A fixed array keeps operator new allocation-free.
constexpr std::size_t kHistogramBuckets = 1024;
std::uint64_t g_size_histogram[kHistogramBuckets] = {};
std::uint64_t g_large_allocation_count = 0;
std::uint64_t g_large_bytes = 0;

[[nodiscard]] std::size_t block_size(void* pointer, std::size_t requested) {
#if defined(__APPLE__)
    (void)requested;
    return malloc_size(pointer);
#elif defined(__linux__)
    (void)requested;
    return malloc_usable_size(pointer);
#else
    (void)pointer;
    return requested;
#endif
}

void record_allocation(void* pointer, std::size_t requested) {
    const std::size_t size = block_size(pointer, requested);
    ++g_allocation_count;
    g_bytes_allocated += size;
    if (size < kHistogramBuckets) {
        ++g_size_histogram[size];
    } else {
        ++g_large_allocation_count;
        g_large_bytes += size;
    }
}

void record_deallocation(void* pointer) {
    ++g_deallocation_count;
    g_bytes_freed += block_size(pointer, 0);
}

[[nodiscard]] void* allocate(std::size_t size) {
    void* pointer = std::malloc(size == 0 ? 1 : size);
    if (pointer != nullptr) {
        record_allocation(pointer, size);
    }
    return pointer;
}

[[nodiscard]] void* allocate_aligned(std::size_t size, std::size_t alignment) {
    void* pointer = nullptr;
    // posix_memalign requires a power-of-two alignment that is also a
    // multiple of sizeof(void*); every over-aligned new satisfies the first
    // and this rounds up to satisfy the second.
    const std::size_t effective = std::max(alignment, sizeof(void*));
    if (::posix_memalign(&pointer, effective, size == 0 ? 1 : size) != 0) {
        return nullptr;
    }
    record_allocation(pointer, size);
    return pointer;
}

void release(void* pointer) {
    if (pointer == nullptr) {
        return;
    }
    record_deallocation(pointer);
    std::free(pointer);
}

} // namespace

void* operator new(std::size_t size) {
    void* pointer = allocate(size);
    if (pointer == nullptr) {
        throw std::bad_alloc();
    }
    return pointer;
}
void* operator new[](std::size_t size) { return ::operator new(size); }
void* operator new(std::size_t size, const std::nothrow_t&) noexcept { return allocate(size); }
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept { return allocate(size); }
void* operator new(std::size_t size, std::align_val_t alignment) {
    void* pointer = allocate_aligned(size, static_cast<std::size_t>(alignment));
    if (pointer == nullptr) {
        throw std::bad_alloc();
    }
    return pointer;
}
void* operator new[](std::size_t size, std::align_val_t alignment) { return ::operator new(size, alignment); }
void* operator new(std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept {
    return allocate_aligned(size, static_cast<std::size_t>(alignment));
}
void* operator new[](std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept {
    return allocate_aligned(size, static_cast<std::size_t>(alignment));
}

void operator delete(void* pointer) noexcept { release(pointer); }
void operator delete[](void* pointer) noexcept { release(pointer); }
void operator delete(void* pointer, std::size_t) noexcept { release(pointer); }
void operator delete[](void* pointer, std::size_t) noexcept { release(pointer); }
void operator delete(void* pointer, std::align_val_t) noexcept { release(pointer); }
void operator delete[](void* pointer, std::align_val_t) noexcept { release(pointer); }
void operator delete(void* pointer, std::size_t, std::align_val_t) noexcept { release(pointer); }
void operator delete[](void* pointer, std::size_t, std::align_val_t) noexcept { release(pointer); }
void operator delete(void* pointer, const std::nothrow_t&) noexcept { release(pointer); }
void operator delete[](void* pointer, const std::nothrow_t&) noexcept { release(pointer); }
void operator delete(void* pointer, std::align_val_t, const std::nothrow_t&) noexcept { release(pointer); }
void operator delete[](void* pointer, std::align_val_t, const std::nothrow_t&) noexcept { release(pointer); }

namespace {

constexpr InstrumentId kInstrument = 1;
constexpr AccountId kMaker = 1;
constexpr AccountId kTaker = 2;
constexpr Price kBase = 10'000'000;

struct AllocationSnapshot {
    std::uint64_t allocations = 0;
    std::uint64_t deallocations = 0;
    std::uint64_t bytes_allocated = 0;
    std::uint64_t bytes_freed = 0;

    [[nodiscard]] std::uint64_t live_bytes() const { return bytes_allocated - bytes_freed; }
};

[[nodiscard]] AllocationSnapshot snapshot_allocations() {
    return AllocationSnapshot{g_allocation_count, g_deallocation_count, g_bytes_allocated, g_bytes_freed};
}

[[nodiscard]] AllocationSnapshot since(const AllocationSnapshot& before) {
    const AllocationSnapshot now = snapshot_allocations();
    return AllocationSnapshot{
        now.allocations - before.allocations,
        now.deallocations - before.deallocations,
        now.bytes_allocated - before.bytes_allocated,
        now.bytes_freed - before.bytes_freed,
    };
}

void reset_histogram() {
    for (auto& bucket : g_size_histogram) {
        bucket = 0;
    }
    g_large_allocation_count = 0;
    g_large_bytes = 0;
}

[[nodiscard]] std::size_t resident_bytes() {
#if defined(__APPLE__)
    mach_task_basic_info info{};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count) ==
        KERN_SUCCESS) {
        return static_cast<std::size_t>(info.resident_size);
    }
    return 0;
#elif defined(__linux__)
    std::FILE* statm = std::fopen("/proc/self/statm", "r");
    if (statm == nullptr) {
        return 0;
    }
    long total_pages = 0;
    long resident_pages = 0;
    const int matched = std::fscanf(statm, "%ld %ld", &total_pages, &resident_pages);
    std::fclose(statm);
    return matched == 2 ? static_cast<std::size_t>(resident_pages) * 4096U : 0;
#else
    return 0;
#endif
}

void rule(const char* title) {
    std::printf("\n=== %s %s\n", title, std::string(std::max<std::size_t>(4, 74 - std::strlen(title)), '=').c_str());
}

// ── Per-operation allocation profile ───────────────────────────────────────

// Builds the engine state a scenario needs and returns the commands whose
// allocation behaviour is to be measured. Everything the builder itself
// allocates happens before the snapshot is taken.
using ScenarioBuilder = std::function<std::vector<ExchangeCommand>(MatchingEngine&, SequentialIds&)>;

void profile_operation(const char* name, const ScenarioBuilder& build) {
    MatchingEngine engine{kInstrument};
    SequentialIds ids;
    const std::vector<ExchangeCommand> commands = build(engine, ids);
    const EventSink& sink = discard_events();

    const AllocationSnapshot before = snapshot_allocations();
    for (const auto& command : commands) {
        engine.process(command, sink);
    }
    const AllocationSnapshot delta = since(before);

    const auto operations = static_cast<double>(commands.size());
    std::printf("%-44s %10.3f %10.3f %12.1f %14.1f\n", name, static_cast<double>(delta.allocations) / operations,
                static_cast<double>(delta.deallocations) / operations,
                static_cast<double>(delta.bytes_allocated) / operations,
                (static_cast<double>(delta.bytes_allocated) - static_cast<double>(delta.bytes_freed)) / operations);
}

void report_operation_allocations() {
    rule("Allocations per matching-engine operation");
    std::printf("Averages over a batch, so a fractional count is real (amortised container growth),\n"
                "not a rounding artefact. \"net bytes\" is allocated minus freed: it is what the\n"
                "operation leaves behind on the heap, and should be ~0 for anything that does not\n"
                "grow the book.\n\n");
    std::printf("%-44s %10s %10s %12s %14s\n", "operation", "allocs/op", "frees/op", "bytes/op", "net bytes/op");
    std::printf("%s\n", std::string(94, '-').c_str());

    constexpr std::size_t kOps = 20'000;

    profile_operation("new resting order, existing price level",
                      [](MatchingEngine& engine, SequentialIds& ids) {
                          seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -1, 1'024, 1, 100);
                          std::vector<ExchangeCommand> commands;
                          for (std::size_t i = 0; i < kOps; ++i) {
                              commands.push_back(ExchangeCommand{new_order(
                                  ids.take_command_sequence(), kMaker, ids.take_client_order_id(), kInstrument,
                                  Side::Buy, kBase - static_cast<Price>(i % 1'024), 100)});
                          }
                          return commands;
                      });

    profile_operation("new resting order, new price level",
                      [](MatchingEngine& engine, SequentialIds& ids) {
                          seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -2, 1'024, 1, 100);
                          std::vector<ExchangeCommand> commands;
                          for (std::size_t i = 0; i < kOps; ++i) {
                              commands.push_back(ExchangeCommand{new_order(
                                  ids.take_command_sequence(), kMaker, ids.take_client_order_id(), kInstrument,
                                  Side::Buy, kBase - 2 * static_cast<Price>(i) - 1, 100)});
                          }
                          return commands;
                      });

    profile_operation("cancel", [](MatchingEngine& engine, SequentialIds& ids) {
        std::vector<ClientOrderId> seeded;
        seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -1, 1'024, kOps / 1'024 + 2, 100,
                            &seeded);
        std::vector<ExchangeCommand> commands;
        for (std::size_t i = 0; i < kOps; ++i) {
            commands.push_back(
                ExchangeCommand{cancel_order(ids.take_command_sequence(), kMaker, seeded[i], kInstrument)});
        }
        return commands;
    });

    profile_operation("match, one fill (IOC vs one resting order)",
                      [](MatchingEngine& engine, SequentialIds& ids) {
                          seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Sell, kBase, 1, 1, kOps, 10);
                          std::vector<ExchangeCommand> commands;
                          for (std::size_t i = 0; i < kOps; ++i) {
                              commands.push_back(ExchangeCommand{new_order(ids.take_command_sequence(), kTaker,
                                                                            ids.take_client_order_id(), kInstrument,
                                                                            Side::Buy, kBase, 10, TimeInForce::IOC)});
                          }
                          return commands;
                      });

    profile_operation("match, 16-level sweep (per operation)",
                      [](MatchingEngine& engine, SequentialIds& ids) {
                          constexpr std::size_t kLevels = 16;
                          constexpr std::size_t kCases = 2'000;
                          seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Sell, kBase, 1,
                                              kCases * kLevels, 1, 10);
                          std::vector<ExchangeCommand> commands;
                          for (std::size_t i = 0; i < kCases; ++i) {
                              const Price worst = kBase + static_cast<Price>((i + 1) * kLevels - 1);
                              commands.push_back(ExchangeCommand{new_order(
                                  ids.take_command_sequence(), kTaker, ids.take_client_order_id(), kInstrument,
                                  Side::Buy, worst, 10 * kLevels, TimeInForce::IOC)});
                          }
                          return commands;
                      });

    profile_operation("replace, priority preserved (qty decrease)",
                      [](MatchingEngine& engine, SequentialIds& ids) {
                          std::vector<ClientOrderId> current;
                          seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -1, 1'024, 1,
                                              kOps + 16, &current);
                          std::vector<Quantity> remaining(1'024, kOps + 16);
                          std::vector<ExchangeCommand> commands;
                          for (std::size_t i = 0; i < kOps; ++i) {
                              const std::size_t target = i % 1'024;
                              const ClientOrderId fresh = ids.take_client_order_id();
                              --remaining[target];
                              commands.push_back(ExchangeCommand{replace_order(
                                  ids.take_command_sequence(), kMaker, current[target], fresh, kInstrument,
                                  kBase - static_cast<Price>(target), remaining[target])});
                              current[target] = fresh;
                          }
                          return commands;
                      });

    profile_operation("replace, priority lost (qty increase)",
                      [](MatchingEngine& engine, SequentialIds& ids) {
                          std::vector<ClientOrderId> current;
                          seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -1, 1'024, 1, 1,
                                              &current);
                          std::vector<Quantity> remaining(1'024, 1);
                          std::vector<ExchangeCommand> commands;
                          for (std::size_t i = 0; i < kOps; ++i) {
                              const std::size_t target = i % 1'024;
                              const ClientOrderId fresh = ids.take_client_order_id();
                              ++remaining[target];
                              commands.push_back(ExchangeCommand{replace_order(
                                  ids.take_command_sequence(), kMaker, current[target], fresh, kInstrument,
                                  kBase - static_cast<Price>(target), remaining[target])});
                              current[target] = fresh;
                          }
                          return commands;
                      });

    profile_operation("replace, priority lost (reprice)", [](MatchingEngine& engine, SequentialIds& ids) {
        std::vector<ClientOrderId> current;
        seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -2, 1'024, 1, 100, &current);
        std::vector<bool> shifted(1'024, false);
        std::vector<ExchangeCommand> commands;
        for (std::size_t i = 0; i < kOps; ++i) {
            const std::size_t target = i % 1'024;
            const ClientOrderId fresh = ids.take_client_order_id();
            shifted[target] = !shifted[target];
            const Price home = kBase - 2 * static_cast<Price>(target);
            commands.push_back(ExchangeCommand{replace_order(ids.take_command_sequence(), kMaker, current[target],
                                                              fresh, kInstrument, shifted[target] ? home - 1 : home,
                                                              100)});
            current[target] = fresh;
        }
        return commands;
    });

    profile_operation("FOK rejected in preflight, 1 contra level",
                      [](MatchingEngine& engine, SequentialIds& ids) {
                          seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Sell, kBase, 1, 1, 1, 10);
                          std::vector<ExchangeCommand> commands;
                          for (std::size_t i = 0; i < kOps; ++i) {
                              commands.push_back(ExchangeCommand{new_order(ids.take_command_sequence(), kTaker,
                                                                            ids.take_client_order_id(), kInstrument,
                                                                            Side::Buy, kBase, 11, TimeInForce::FOK)});
                          }
                          return commands;
                      });

    profile_operation("FOK rejected in preflight, 4096 contra levels",
                      [](MatchingEngine& engine, SequentialIds& ids) {
                          seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Sell, kBase, 1, 1, 1, 10);
                          seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Sell, kBase + 1'000'000, 1,
                                              4'096, 1, 10);
                          std::vector<ExchangeCommand> commands;
                          for (std::size_t i = 0; i < 2'000; ++i) {
                              commands.push_back(ExchangeCommand{new_order(ids.take_command_sequence(), kTaker,
                                                                            ids.take_client_order_id(), kInstrument,
                                                                            Side::Buy, kBase, 11, TimeInForce::FOK)});
                          }
                          return commands;
                      });

    // The mixed stream's average, which is the figure that matters for a
    // sustained-load allocation budget.
    {
        WorkloadConfig config;
        config.operation_count = 200'000;
        config.initial_orders_per_side = 1'000;
        const Workload workload = generate_workload(config);
        profile_operation("mixed realistic workload (average)",
                          [&workload](MatchingEngine& engine, SequentialIds&) {
                              replay(engine, workload.seed, discard_events());
                              return workload.operations;
                          });
    }
}

// ── Memory footprint of a resting book ─────────────────────────────────────

void report_footprint() {
    rule("Memory footprint of a resting book");
    std::printf("Directly measured, not estimated: live bytes is (allocated - freed) across building\n"
                "the book, using the platform allocator's own reported block sizes, so allocator\n"
                "rounding is included. RSS delta is a coarse cross-check that includes page-level\n"
                "granularity and the process's other state.\n\n");
    std::printf("%12s %14s | %14s %12s %12s %14s\n", "orders", "shape", "live bytes", "bytes/order", "allocs/order",
                "RSS delta MB");
    std::printf("%s\n", std::string(88, '-').c_str());

    struct Shape {
        const char* name;
        std::size_t levels_divisor; // orders per price level
    };
    const Shape shapes[] = {{"1 order/level", 1}, {"100 orders/level", 100}};

    for (const std::size_t orders : {std::size_t{1'000}, std::size_t{10'000}, std::size_t{100'000},
                                      std::size_t{1'000'000}}) {
        for (const Shape& shape : shapes) {
            const std::size_t per_level = std::min(shape.levels_divisor, orders);
            const std::size_t levels = orders / per_level;

            const std::size_t rss_before = resident_bytes();
            const AllocationSnapshot before = snapshot_allocations();
            {
                MatchingEngine engine{kInstrument};
                SequentialIds ids;
                seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -1, levels, per_level, 100);
                const AllocationSnapshot delta = since(before);
                const std::size_t rss_after = resident_bytes();
                const auto live = static_cast<double>(delta.bytes_allocated) - static_cast<double>(delta.bytes_freed);
                const auto count = static_cast<double>(orders);
                std::printf("%12zu %14s | %14.0f %12.1f %12.3f %14.1f\n", orders, shape.name, live, live / count,
                            static_cast<double>(delta.allocations - delta.deallocations) / count,
                            (static_cast<double>(rss_after) - static_cast<double>(rss_before)) / (1024.0 * 1024.0));
            }
        }
    }
}

void report_allocation_shape() {
    rule("Which structures the bytes go to");
    std::printf("Allocation-size histogram for building a 1,000,000-order book (1 order per price\n"
                "level, so every per-order structure is exercised once). Sizes are the allocator's\n"
                "block sizes. No per-order structure is visible here at all: the orders come from\n"
                "one slab vector and the price-index nodes from a pool, so what reaches the\n"
                "allocator is a few dozen bulk buffers rather than a few million nodes -- which is\n"
                "why nearly all the bytes land in the >=1024 row.\n\n");
    std::printf("Type sizes on this build:\n");
    std::printf("  sizeof(BookOrder)                 %3zu  (what a slab entry actually holds)\n", sizeof(BookOrder));
    std::printf("  slab entry                        %3zu  (BookOrder plus two 32-bit FIFO links)\n",
                sizeof(BookOrder) + 2 * sizeof(std::uint32_t));
    std::printf("  sizeof(ExchangeRestingOrder)      %3zu  (the reassembled form: commands, snapshots)\n",
                sizeof(ExchangeRestingOrder));
    std::printf("  sizeof(Price)                     %3zu\n", sizeof(Price));
    std::printf("  sizeof(ExchangeOrderId)           %3zu\n", sizeof(ExchangeOrderId));
    std::printf("\n");

    reset_histogram();
    const AllocationSnapshot before = snapshot_allocations();
    {
        MatchingEngine engine{kInstrument};
        SequentialIds ids;
        seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -1, 1'000'000, 1, 100);

        const AllocationSnapshot delta = since(before);
        std::printf("%10s %14s %14s %10s\n", "block size", "count", "total bytes", "share");
        std::printf("%s\n", std::string(52, '-').c_str());

        struct Bucket {
            std::size_t size;
            std::uint64_t count;
        };
        std::vector<Bucket> buckets;
        for (std::size_t size = 0; size < kHistogramBuckets; ++size) {
            if (g_size_histogram[size] > 0) {
                buckets.push_back(Bucket{size, g_size_histogram[size]});
            }
        }
        std::sort(buckets.begin(), buckets.end(), [](const Bucket& a, const Bucket& b) {
            return a.size * a.count > b.size * b.count;
        });

        const auto total_bytes = static_cast<double>(delta.bytes_allocated);
        for (std::size_t i = 0; i < buckets.size() && i < 8; ++i) {
            const auto bytes = static_cast<double>(buckets[i].size * buckets[i].count);
            std::printf("%10zu %14llu %14.0f %9.1f%%\n", buckets[i].size,
                        static_cast<unsigned long long>(buckets[i].count), bytes, 100.0 * bytes / total_bytes);
        }
        if (g_large_allocation_count > 0) {
            std::printf("%10s %14llu %14llu %9.1f%%  (pool chunks / bucket arrays)\n", ">=1024",
                        static_cast<unsigned long long>(g_large_allocation_count),
                        static_cast<unsigned long long>(g_large_bytes),
                        100.0 * static_cast<double>(g_large_bytes) / total_bytes);
        }
        std::printf("\ntotal: %llu allocations, %.0f bytes for 1,000,000 resting orders\n",
                    static_cast<unsigned long long>(delta.allocations), total_bytes);
    }
}

} // namespace

int main() {
    std::printf("mdh matching-engine baseline: allocation behaviour and memory footprint\n");
#if !defined(NDEBUG)
    std::printf("WARNING: NDEBUG is not defined -- this is a debug build and these numbers are not\n"
                "representative. Rebuild with -DCMAKE_BUILD_TYPE=Release.\n");
#endif
    // Force the shared sink's one-time construction outside every measured
    // region, so its std::function initialisation is never charged to an
    // operation.
    (void)discard_events();

    report_operation_allocations();
    report_footprint();
    report_allocation_shape();
    std::printf("\n");
    return EXIT_SUCCESS;
}
