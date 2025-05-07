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

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}