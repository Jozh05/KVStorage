#pragma once
#include "../../src/KVStorage.hpp"

struct ManualClock {
    using rep        = int64_t;
    using period     = std::nano;
    using duration   = std::chrono::nanoseconds;
    using time_point = std::chrono::time_point<ManualClock>;
    static constexpr bool is_steady = true;

    inline static std::atomic<int64_t> now_ns{0};

    static time_point now() noexcept {
        return time_point(duration(now_ns.load(std::memory_order_relaxed)));
    }
    static void reset() noexcept { now_ns.store(0, std::memory_order_relaxed); }
    static void advance(duration d) noexcept {
        now_ns.fetch_add(d.count(), std::memory_order_relaxed);
    }
    static void advance_sec(uint32_t s) noexcept {
        advance(std::chrono::seconds{s});
    }
};

using Storage = KVStorage<ManualClock>;
using Tup = std::tuple<std::string, std::string, uint32_t>;

using namespace std::chrono_literals;