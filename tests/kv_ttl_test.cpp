#include "common/kv_test_utils.hpp"
#include <gtest/gtest.h>

TEST(KVStorage_TTL, TTL_Expiry_And_RemoveOne) {
    ManualClock::reset();
    Storage s(std::span<Tup>{});

    s.set("t1", "a", 5);   // истечёт через 5с
    s.set("t2", "b", 10);  // истечёт через 10с
    s.set("t3", "c", 0);   // вечный

    // До истечения всё читается
    EXPECT_EQ(s.get("t1").value(), "a");
    EXPECT_EQ(s.get("t2").value(), "b");
    EXPECT_EQ(s.get("t3").value(), "c");

    // Истёк только t1
    ManualClock::advance_sec(6);
    EXPECT_FALSE(s.get("t1").has_value());
    EXPECT_TRUE(s.get("t2").has_value());
    EXPECT_TRUE(s.get("t3").has_value());
    EXPECT_EQ(s.getManySorted("a", 10).size(), 2);

    // removeOneExpiredEntry должен снять только t1
    auto removed1 = s.removeOneExpiredEntry();
    ASSERT_TRUE(removed1.has_value());
    EXPECT_EQ(removed1->first, "t1");
    EXPECT_EQ(removed1->second, "a");

    // Ничего не истекло до 10с
    EXPECT_FALSE(s.removeOneExpiredEntry().has_value());

    // Истек t2
    ManualClock::advance_sec(5);
    EXPECT_FALSE(s.get("t2").has_value());
    auto removed2 = s.removeOneExpiredEntry();
    ASSERT_TRUE(removed2.has_value());
    EXPECT_EQ(removed2->first, "t2");
    EXPECT_EQ(removed2->second, "b");

    // Вечный жив
    EXPECT_EQ(s.get("t3").value(), "c");
}




TEST(KVStorage_TTL, ExpiryExactlyNow) {
    ManualClock::reset();
    Storage s(std::span<Tup>{});
    s.set("b", "val", 5);

    
    ManualClock::advance_sec(5);
    EXPECT_FALSE(s.get("b").has_value());

    auto rem = s.removeOneExpiredEntry();
    ASSERT_TRUE(rem.has_value());
    EXPECT_EQ(rem->first, "b");
}




TEST(KVStorage_TTL, UpdateTTL_EternalToFiniteAndBack) {
    ManualClock::reset();
    Storage s(std::span<Tup>{});

    s.set("u", "v0", 0);            // вечный
    ManualClock::advance_sec(3);
    EXPECT_EQ(s.get("u").value(), "v0");

    s.set("u", "v1", 5);            // стал конечным
    ManualClock::advance_sec(4);
    EXPECT_EQ(s.get("u").value(), "v1"); // ещё жив

    s.set("u", "v2", 0);            // снова вечный
    ManualClock::advance_sec(10);
    EXPECT_EQ(s.get("u").value(), "v2"); // жив
    EXPECT_FALSE(s.removeOneExpiredEntry().has_value()); // ничего не протухло
}




TEST(KVStorage_TTL, Expiry_TiesAndDrainingOrder) {
    ManualClock::reset();
    Storage s(std::span<Tup>{});

    s.set("a", "1", 5);
    s.set("b", "2", 5);
    s.set("c", "3", 5);

    ManualClock::advance_sec(6); // все три истекли

    std::set<std::string> seen;
    for (int i = 0; i < 3; ++i) {
        auto rem = s.removeOneExpiredEntry();
        ASSERT_TRUE(rem.has_value());
        seen.insert(rem->first);
    }
    EXPECT_EQ(seen.size(), 3u);
    EXPECT_TRUE(seen.count("a"));
    EXPECT_TRUE(seen.count("b"));
    EXPECT_TRUE(seen.count("c"));
    EXPECT_FALSE(s.removeOneExpiredEntry().has_value()); // больше нечего удалять
}



TEST(KVStorage_TTL, UpdateExistingValueAndTTL) {
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