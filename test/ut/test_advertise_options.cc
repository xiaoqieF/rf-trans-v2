#include <gtest/gtest.h>

#include "trans/advertise_options.hpp"

TEST(advertise_options, construct)
{
    rf::trans::AdvertiseMessageOptions ops;
    EXPECT_EQ(ops.getMsgsPerSec(), rf::trans::kUnthrottled);
    EXPECT_EQ(ops.getScope(), rf::trans::Scope::ALL);

    rf::trans::AdvertiseServiceOptions ops1;
    EXPECT_EQ(ops1.getScope(), rf::trans::Scope::ALL);
}

TEST(advertise_options, equality)
{
    rf::trans::AdvertiseMessageOptions ops1;
    rf::trans::AdvertiseMessageOptions ops2;
    EXPECT_TRUE(ops1 == ops2);
    EXPECT_FALSE(ops1 != ops2);

    ops1.setScope(rf::trans::Scope::HOST);
    EXPECT_FALSE(ops1 == ops2);
    EXPECT_TRUE(ops1 != ops2);
    ops2.setScope(rf::trans::Scope::HOST);
    EXPECT_TRUE(ops1 == ops2);
    EXPECT_FALSE(ops1 != ops2);

    ops1.setMsgsPerSec(10);
    EXPECT_FALSE(ops1 == ops2);
    EXPECT_TRUE(ops1 != ops2);
    ops2.setMsgsPerSec(10);
    EXPECT_TRUE(ops1 == ops2);
    EXPECT_FALSE(ops1 != ops2);
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}