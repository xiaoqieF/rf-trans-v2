#include <stdexcept>

#include <gtest/gtest.h>

#include "trans/discovery.hpp"

namespace rf
{
namespace trans
{
TEST(discovery_init, rejectsInvalidPort)
{
    EXPECT_THROW(
        (Discovery<MessagePublisherInfo>("process", "239.255.0.7", 0)),
        std::invalid_argument);
}

TEST(discovery_init, rejectsNonMulticastAddress)
{
    EXPECT_THROW(
        (Discovery<MessagePublisherInfo>("process", "127.0.0.1", 10317)),
        std::invalid_argument);
}
} // namespace trans
} // namespace rf
