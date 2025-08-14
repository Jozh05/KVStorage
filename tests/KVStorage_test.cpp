#include <gtest/gtest.h>
#include "../src/KVStorage.hpp"

struct ManualClock {
    using rep        = int64_t;
    using period     = std::nano;
    using duration   = std::chrono::nanoseconds;
    using time_point = std::chrono::time_point<ManualClock>;
    static constexpr bool is_steady = true;

    static std::atomic<int64_t> now_ns;

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

std::atomic<int64_t> ManualClock::now_ns{0};

using Storage = KVStorage<ManualClock>;
using Tup = std::tuple<std::string, std::string, uint32_t>;

using namespace std::chrono_literals;

TEST(KVStorage, BasicGetRemove) {
    ManualClock::reset();
    std::vector<Tup> init {
        {"k1", "v1", 0},
        {"k2", "v2", 10},
        {"k1", "v1-last", 0}
    };

    Storage stor{std::span<Tup>(init)};
    auto v1 = stor.get("k1");
    ASSERT_TRUE(v1.has_value());
    EXPECT_EQ(*v1, "v1-last");

    // k2 — есть и не истёк
    auto v2 = stor.get("k2");
    ASSERT_TRUE(v2.has_value());
    EXPECT_EQ(*v2, "v2");

    // remove: нет ключа
    EXPECT_FALSE(stor.remove("no-such"));

    // remove: есть ключ
    EXPECT_TRUE(stor.remove("k2"));
    EXPECT_FALSE(stor.get("k2").has_value());
}

TEST(KVStorage, TTL_Expiry_And_RemoveOne) {
    ManualClock::reset();
    Storage s(std::span<Tup>{});

    s.set("t1", "a", 5);   // истечёт через 5с
    s.set("t2", "b", 10);  // истечёт через 10с
    s.set("t3", "c", 0);   // вечный

    // До истечения всё читается
    EXPECT_EQ(s.get("t1").value(), "a");
    EXPECT_EQ(s.get("t2").value(), "b");
    EXPECT_EQ(s.get("t3").value(), "c");

    // Промотали 6с — истёк только t1
    ManualClock::advance_sec(6);
    EXPECT_FALSE(s.get("t1").has_value());
    EXPECT_TRUE(s.get("t2").has_value());
    EXPECT_TRUE(s.get("t3").has_value());
    EXPECT_EQ(s.getManySorted("a", 10).size(), 2);

    // removeOneExpiredEntry должен снять ровно t1
    auto removed1 = s.removeOneExpiredEntry();
    ASSERT_TRUE(removed1.has_value());
    EXPECT_EQ(removed1->first, "t1");
    EXPECT_EQ(removed1->second, "a");

    // Повторно — ничего не истекло ДО 10с
    EXPECT_FALSE(s.removeOneExpiredEntry().has_value());

    // Дотягиваем до истечения t2
    ManualClock::advance_sec(5);
    EXPECT_FALSE(s.get("t2").has_value());
    auto removed2 = s.removeOneExpiredEntry();
    ASSERT_TRUE(removed2.has_value());
    EXPECT_EQ(removed2->first, "t2");
    EXPECT_EQ(removed2->second, "b");

    // Вечный жив
    EXPECT_EQ(s.get("t3").value(), "c");
}

TEST(KVStorage, GetManySorted_SkipsExpired) {
    ManualClock::reset();
    Storage s(std::span<Tup>{});

    s.set("a",  "1", 0);
    s.set("aa", "2", 0);
    s.set("ab", "3", 2);
    s.set("b",  "4", 0);

    // До истечения
    {
        auto out = s.getManySorted("aa", 3);
        ASSERT_EQ(out.size(), 3u);
        EXPECT_EQ(out[0], std::make_pair(std::string("aa"), std::string("2")));
        EXPECT_EQ(out[1], std::make_pair(std::string("ab"), std::string("3")));
        EXPECT_EQ(out[2], std::make_pair(std::string("b"),  std::string("4")));
    }

    // истек ab
    ManualClock::advance_sec(3);
    {
        auto out = s.getManySorted("a", 10);
        std::vector<std::pair<std::string,std::string>> exp = {
            {"a","1"}, {"aa","2"}, {"b","4"}
        };
        EXPECT_EQ(out, exp);
    }
}

TEST(KVStorage, UpdateExistingValueAndTTL) {
    ManualClock::reset();
    Storage s(std::span<Tup>{});

    s.set("x", "v1", 5); 
    ManualClock::advance_sec(3);

    s.set("x", "v2", 10); 

    // Ещё должен жить
    ManualClock::advance_sec(6); 
    auto vx = s.get("x");
    ASSERT_TRUE(vx.has_value());
    EXPECT_EQ(*vx, "v2");

    // Должен протухнуть
    ManualClock::advance_sec(5);
    EXPECT_FALSE(s.get("x").has_value());
    auto removed = s.removeOneExpiredEntry();
    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(removed->first, "x");
    EXPECT_EQ(removed->second, "v2");
}