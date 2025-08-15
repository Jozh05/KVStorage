#include <gtest/gtest.h>
#include "common/kv_test_utils.hpp"
#include "../src/KVStorage.hpp"
#include <future>

TEST(KVStorage_Concurrency, ReadersVsWriter_NoDeadlock) {
    ManualClock::reset();
    Storage s(std::span<Tup>{});
    s.set("hot","val",0);

    StartStopGate gate;
    auto readers = spawn_readers(8, gate, [&]{ (void)s.get("hot"); (void)s.getManySorted("h",4); });
    gate.start();

    std::promise<void> wrote;
    std::thread writer([&]{ s.set("other","val2",0); wrote.set_value(); });

    auto status = wrote.get_future().wait_for(std::chrono::seconds(2));
    EXPECT_EQ(status, std::future_status::ready) << "writer likely starved or deadlocked";

    gate.request_stop();
    for (auto& t: readers) t.join();
    writer.join();
    EXPECT_EQ(s.get("other").value(),"val2");
}



TEST(KVStorage_Concurrency, WriterPriority_GatePreventsStarvation) {
    ManualClock::reset();
    Storage s(std::span<Tup>{}); s.set("key","v",0);

    StartStopGate gate;
    auto readers = spawn_readers(8, gate, [&]{ (void)s.get("key"); });
    gate.start();

    auto t0 = std::chrono::steady_clock::now();
    s.set("gate-test","ok",0);
    auto dur = std::chrono::steady_clock::now() - t0;

    gate.request_stop();
    for (auto& t: readers) t.join();

    EXPECT_EQ(s.get("gate-test").value(),"ok");
    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(dur).count(), 1500);
}




TEST(KVStorage_Concurrency, TwoWritersBackToBack_UnderReaderLoad) {
    ManualClock::reset();
    Storage s(std::span<Tup>{}); s.set("w","init",0);

    StartStopGate gate;
    auto readers = spawn_readers(6, gate, [&]{ (void)s.get("w"); });
    gate.start();

    auto t0 = std::chrono::steady_clock::now();
    std::thread w1([&]{ s.set("w","v1",0); });
    std::thread w2([&]{ s.set("w","v2",0); });
    w1.join(); w2.join();
    auto dur = std::chrono::steady_clock::now() - t0;

    gate.request_stop();
    for (auto& t: readers) t.join();

    auto vw = s.get("w");
    ASSERT_TRUE(vw.has_value());
    EXPECT_TRUE(*vw == "v1" || *vw == "v2");
    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(dur).count(), 1500);
}
