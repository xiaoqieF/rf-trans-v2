#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "msgs/example.pb.h"
#include "trans/advertise_options.hpp"
#include "trans/node.hpp"

namespace
{
std::atomic<unsigned int> next_topic_id{0};

std::string makeTopic()
{
    return "/test/node_lifecycle/" + std::to_string(next_topic_id.fetch_add(1));
}

bool containsTopic(const std::vector<std::string>& topics, const std::string& topic)
{
    return std::find(topics.begin(), topics.end(), topic) != topics.end();
}

struct BatchState
{
    std::atomic<uint64_t> received{0};
    std::atomic<bool> valid{true};
    std::mutex mutex;
    std::condition_variable callback_cv;
};

bool waitFor(const std::shared_ptr<BatchState>& state, const uint64_t expected)
{
    std::unique_lock lock(state->mutex);
    return state->callback_cv.wait_for(lock, std::chrono::seconds(1), [state, expected] {
        return state->received.load(std::memory_order_acquire) >= expected;
    });
}
} // namespace

TEST(node_lifecycle, explicitMessageUnadvertiseRemovesTopic)
{
    rf::trans::Node node;
    rf::trans::AdvertiseMessageOptions options;
    options.setScope(rf::trans::Scope::PROCESS);
    const std::string topic = makeTopic();

    auto publisher = node.advertise<rf::msgs::ExampleMsg>(topic, options);
    ASSERT_TRUE(publisher.valid());
    EXPECT_TRUE(containsTopic(node.getTopicList(), topic));

    EXPECT_TRUE(node.unadvertise(topic));
    EXPECT_FALSE(containsTopic(node.getTopicList(), topic));
}

TEST(node_lifecycle, destructorRemovesMessageAdvertisement)
{
    rf::trans::AdvertiseMessageOptions options;
    options.setScope(rf::trans::Scope::PROCESS);
    const std::string topic = makeTopic();

    {
        rf::trans::Node node;
        auto publisher = node.advertise<rf::msgs::ExampleMsg>(topic, options);
        ASSERT_TRUE(publisher.valid());
        EXPECT_TRUE(containsTopic(node.getTopicList(), topic));
    }

    rf::trans::Node observer;
    EXPECT_FALSE(containsTopic(observer.getTopicList(), topic));
}

TEST(node_lifecycle, localPublishDeliversRepeatedBatchesInOrder)
{
    constexpr int kBatchSize = 128;
    constexpr int kBatchCount = 16;
    const std::string topic = makeTopic();
    const auto state = std::make_shared<BatchState>();

    rf::trans::Node publisher_node;
    rf::trans::Node subscriber_node;
    ASSERT_TRUE(subscriber_node.subscribe<rf::msgs::ExampleMsg>(topic,
        [state](const std::shared_ptr<const rf::msgs::ExampleMsg>& message) {
            std::lock_guard lock(state->mutex);
            const uint64_t sequence = state->received.fetch_add(1, std::memory_order_acq_rel) + 1;
            if (!message || message->age() != static_cast<int>(sequence)) {
                state->valid.store(false, std::memory_order_release);
            }
            state->callback_cv.notify_one();
        }));

    rf::trans::AdvertiseMessageOptions options;
    options.setScope(rf::trans::Scope::PROCESS);
    auto publisher = publisher_node.advertise<rf::msgs::ExampleMsg>(topic, options);
    ASSERT_TRUE(publisher.valid());
    ASSERT_TRUE(publisher.hasConnections());

    for (int batch = 0; batch < kBatchCount; ++batch) {
        const uint64_t first_sequence = state->received.load(std::memory_order_acquire) + 1;
        for (int index = 0; index < kBatchSize; ++index) {
            auto message = std::make_unique<rf::msgs::ExampleMsg>();
            message->set_age(static_cast<int>(first_sequence + index));
            ASSERT_TRUE(publisher.publish(std::move(message)));
        }
        ASSERT_TRUE(waitFor(state, first_sequence + kBatchSize - 1));
    }

    EXPECT_EQ(state->received.load(std::memory_order_acquire), kBatchSize * kBatchCount);
    EXPECT_TRUE(state->valid.load(std::memory_order_acquire));
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
