#include <gtest/gtest.h>
#include "../src/KVStorage.hpp"
#include <future>

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

TEST(KVStorage, ConcurrentReadersAndWriter_NoDeadlock) {
    ManualClock::reset();
    Storage s(std::span<Tup>{});
    s.set("hot", "val", 0);

    std::atomic<bool> go{false};
    std::atomic<bool> stop{false};

    // Несколько читателей, постоянно дергают get / getManySorted
    const int R = 8;
    std::vector<std::thread> readers;
    readers.reserve(R);
    for (int i = 0; i < R; ++i) {
        readers.emplace_back([&]{
            while (!go.load(std::memory_order_acquire)) { std::this_thread::yield(); }
            while (!stop.load(std::memory_order_acquire)) {
                auto v = s.get("hot");
                auto vec = s.getManySorted("h", 4);
            }
        });
    }

    // Запускаем читателей
    go.store(true, std::memory_order_release);
    std::this_thread::sleep_for(50ms);

    // Писатель должен завершиться без дедлока
    std::promise<void> wrote;
    std::thread writer([&]{
        s.set("other", "val2", 0);
        wrote.set_value();
    });

    auto fut = wrote.get_future();
    // Ожидаем завершения писателя в разумные сроки
    auto status = fut.wait_for(2s);
    EXPECT_EQ(status, std::future_status::ready) << "writer likely starved or deadlocked";

    stop.store(true, std::memory_order_release);
    for (auto& t : readers) t.join();
    writer.join();

    // Проверим, что запись успешна
    EXPECT_EQ(s.get("other").value(), "val2");
}

TEST(KVStorage, WriterPriority_GatePreventsStarvation) {
    ManualClock::reset();
    Storage s(std::span<Tup>{});
    s.set("key", "v", 0);

    std::atomic<bool> go{false};
    std::atomic<bool> stop{false};

    // Поток читателей, которые постоянно пытаются читать
    const int R = 8;
    std::vector<std::thread> readers;
    readers.reserve(R);
    for (int i = 0; i < R; ++i) {
        readers.emplace_back([&]{
            while (!go.load(std::memory_order_acquire)) { std::this_thread::yield(); }
            while (!stop.load(std::memory_order_acquire)) {
                (void)s.get("key");
            }
        });
    }

    go.store(true, std::memory_order_release);
    std::this_thread::sleep_for(50ms); 

    // Писатель должен быстро пройти
    auto start = std::chrono::steady_clock::now();
    s.set("gate-test", "ok", 0);
    auto dur = std::chrono::steady_clock::now() - start;

    stop.store(true, std::memory_order_release);
    for (auto& t : readers) t.join();

    EXPECT_EQ(s.get("gate-test").value(), "ok");

    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(dur).count(), 1500)
        << "Writer took too long under reader load";
}