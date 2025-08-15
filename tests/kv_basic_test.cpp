#include "common/kv_test_utils.hpp"
#include <gtest/gtest.h>

TEST(KVStorage_Basic, BasicGetRemove) {
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

    // k2 есть и не истёк
    auto v2 = stor.get("k2");
    ASSERT_TRUE(v2.has_value());
    EXPECT_EQ(*v2, "v2");

    // remove: нет ключа
    EXPECT_FALSE(stor.remove("no-such"));

    // remove: есть ключ
    EXPECT_TRUE(stor.remove("k2"));
    EXPECT_FALSE(stor.get("k2").has_value());
}

TEST(KVStorage_Basic, Remove_Idempotent) {
    ManualClock::reset();
    Storage s(std::span<Tup>{});

    EXPECT_FALSE(s.remove("nope")); // нет ключа

    s.set("r", "x", 0);
    EXPECT_TRUE(s.remove("r"));
    EXPECT_FALSE(s.remove("r"));
    EXPECT_FALSE(s.get("r").has_value());
    EXPECT_FALSE(s.removeOneExpiredEntry().has_value());
}

TEST(KVStorage_Basic, Constructor_DuplicatesAndTTL_LastWriteWins) {
    ManualClock::reset();
    std::vector<Tup> init = {
        {"dup", "v1", 5},
        {"k",   "a",  0},
        {"k",   "b",  0},
        {"dup", "v2", 0},
    };
    Storage s{std::span<Tup>(init)};

    // dup не должен истекать
    ManualClock::advance_sec(10);
    EXPECT_TRUE(s.get("dup").has_value());
}

TEST(KVStorage_Basic, DrainManySameExpiry) {
    ManualClock::reset();
    Storage s(std::span<Tup>{});

    // партия ключей с общим временем истечения
    const int N = 200;
    for (int i = 0; i < N; ++i) {
        s.set("p_" + std::to_string(i), "v", 5);
    }
    ManualClock::advance_sec(6);

    int removed = 0;
    while (auto r = s.removeOneExpiredEntry()) {
        ++removed;
    }
    EXPECT_EQ(removed, N); // все протухшие удалены
}