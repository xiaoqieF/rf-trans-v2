#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>

#include <gtest/gtest.h>

#include "msgs/example.pb.h"
#include "shm_transport_test_payload.hpp"
#include "trans/advertise_options.hpp"
#include "trans/node.hpp"

using namespace std::chrono_literals;
using rf::msgs::ExampleMsg;
using rf::trans::AdvertiseMessageOptions;
using rf::trans::Node;
using rf::trans::Publisher;
using rf::trans::Scope;
using rf::trans::test::buildShmTestPayload;

namespace
{
constexpr auto kWaitTimeout = 5s;

std::atomic<uint64_t> next_topic_id{0};

std::string makeTopic()
{
    return "/test/shm_transport/" + std::to_string(next_topic_id.fetch_add(1));
}

struct ReceivedState
{
    std::mutex mutex;
    std::condition_variable cv;
    std::string payload;
    std::atomic<uint64_t> received{0};
};

bool waitForConnections(const Publisher& publisher)
{
    const auto deadline = std::chrono::steady_clock::now() + kWaitTimeout;
    while (!publisher.hasConnections() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    return publisher.hasConnections();
}

bool waitForMessage(const std::shared_ptr<ReceivedState>& state, const uint64_t expected)
{
    std::unique_lock lock(state->mutex);
    return state->cv.wait_for(lock, kWaitTimeout, [&state, expected] {
        return state->received.load(std::memory_order_acquire) >= expected;
    });
}
} // namespace

TEST(shm_transport, largeMessageRoundTripsThroughShm)
{
    const std::string topic = makeTopic();
    const std::string payload = buildShmTestPayload(2 * 1024 * 1024);

    Node subscriber_node;
    auto state = std::make_shared<ReceivedState>();
    ASSERT_TRUE(subscriber_node.subscribe<ExampleMsg>(topic,
        [state](const std::shared_ptr<const ExampleMsg>& msg) {
            std::lock_guard lock(state->mutex);
            state->payload = msg->payload();
            state->received.fetch_add(1, std::memory_order_release);
            state->cv.notify_one();
        }));

    Node publisher_node;
    AdvertiseMessageOptions options;
    options.setScope(Scope::HOST);
    options.setUseShm(true);
    auto publisher = publisher_node.advertise<ExampleMsg>(topic, options);
    ASSERT_TRUE(publisher.valid());
    ASSERT_TRUE(waitForConnections(publisher));

    auto msg = std::make_unique<ExampleMsg>();
    msg->set_name("shm-roundtrip");
    msg->set_payload(payload);
    ASSERT_TRUE(publisher.publish(std::move(msg)));

    ASSERT_TRUE(waitForMessage(state, 1));
    std::lock_guard lock(state->mutex);
    EXPECT_EQ(state->payload, payload);
}

TEST(shm_transport, messageLargerThanExistingRingFallsBackToRegularTransport)
{
    const std::string topic = makeTopic();
    const std::string small_payload = buildShmTestPayload(1024);
    const std::string large_payload = buildShmTestPayload(1024 * 1024);

    Node subscriber_node;
    auto state = std::make_shared<ReceivedState>();
    ASSERT_TRUE(subscriber_node.subscribe<ExampleMsg>(topic,
        [state](const std::shared_ptr<const ExampleMsg>& msg) {
            std::lock_guard lock(state->mutex);
            state->payload = msg->payload();
            state->received.fetch_add(1, std::memory_order_release);
            state->cv.notify_one();
        }));

    Node publisher_node;
    AdvertiseMessageOptions options;
    options.setScope(Scope::HOST);
    options.setUseShm(true);
    auto publisher = publisher_node.advertise<ExampleMsg>(topic, options);
    ASSERT_TRUE(publisher.valid());
    ASSERT_TRUE(waitForConnections(publisher));

    // First publish fixes the shm ring's slot size to `small_payload`'s size.
    auto first = std::make_unique<ExampleMsg>();
    first->set_name("small");
    first->set_payload(small_payload);
    ASSERT_TRUE(publisher.publish(std::move(first)));
    ASSERT_TRUE(waitForMessage(state, 1));
    {
        std::lock_guard lock(state->mutex);
        EXPECT_EQ(state->payload, small_payload);
    }

    // Second publish no longer fits the ring; MessageRuntime must fall back
    // to the regular (non-shm) transport rather than drop the message.
    auto second = std::make_unique<ExampleMsg>();
    second->set_name("large");
    second->set_payload(large_payload);
    ASSERT_TRUE(publisher.publish(std::move(second)));
    ASSERT_TRUE(waitForMessage(state, 2));
    std::lock_guard lock(state->mutex);
    EXPECT_EQ(state->payload, large_payload);
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
