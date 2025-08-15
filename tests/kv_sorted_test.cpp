#include "common/kv_test_utils.hpp"
#include <gtest/gtest.h>

TEST(KVStorage_Sorted, GetManySorted_SkipsExpired) {
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



TEST(KVStorage_Sorted, GetManySorted_EdgeCases) {
    ManualClock::reset();
    Storage s(std::span<Tup>{});

    s.set("a", "1", 0);
    s.set("b", "2", 1);
    s.set("c", "3", 0);

    // count == 0
    {
        auto out = s.getManySorted("a", 0);
        EXPECT_TRUE(out.empty());
    }

    // Стартовая точка после последнего ключа
    {
        auto out = s.getManySorted("z", 10);
        EXPECT_TRUE(out.empty());
    }

    // После истечения b
    ManualClock::advance_sec(1);
    {
        auto out = s.getManySorted("", 10);
        std::vector<std::pair<std::string,std::string>> exp = {{"a","1"}, {"c","3"}};
        EXPECT_EQ(out, exp);
    }
}

