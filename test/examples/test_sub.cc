#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "msgs/example.pb.h"
#include "trans/node.hpp"

using namespace std::chrono_literals;
using namespace rf::trans;

namespace
{
constexpr std::array<const char*, 4> kTopics{
    "/examples/multi_topic/temperature",
    "/examples/multi_topic/pressure",
    "/examples/multi_topic/heartbeat",
    "/examples/multi_topic/diagnostics",
};
constexpr int kMaxFirstSequence = 40;
constexpr int kMinReceivedPerTopic = 20;
constexpr auto kFirstMessageTimeout = 5s;
constexpr auto kCollectionTime = 1s;
constexpr auto kStatusPeriod = 1s;

volatile std::sig_atomic_t stop_requested = 0;

void requestStop(int)
{
    stop_requested = 1;
}

struct TopicStats
{
    std::string topic;
    std::atomic<int> first_sequence{-1};
    std::atomic<int> last_sequence{-1};
    std::atomic<int> received{0};
    std::atomic<bool> out_of_order{false};
    std::atomic<bool> wrong_topic{false};
};

bool allTopicsReceived(const std::vector<std::shared_ptr<TopicStats>>& stats)
{
    for (const auto& state : stats) {
        if (state->first_sequence.load() < 0) {
            return false;
        }
    }
    return true;
}

bool passed(const TopicStats& state)
{
    return state.first_sequence.load() >= 0 &&
        state.first_sequence.load() <= kMaxFirstSequence &&
        state.received.load() >= kMinReceivedPerTopic &&
        !state.out_of_order.load() &&
        !state.wrong_topic.load();
}

void printStatus(const TopicStats& state, const char* phase)
{
    std::cout << "SUB_STATUS phase=" << phase
              << " topic=" << state.topic
              << " first=" << state.first_sequence.load()
              << " last=" << state.last_sequence.load()
              << " received=" << state.received.load()
              << " ordered=" << std::boolalpha << !state.out_of_order.load()
              << " topic_matches=" << !state.wrong_topic.load() << '\n';
}
} // namespace

int main()
{
    std::signal(SIGINT, requestStop);
    std::signal(SIGTERM, requestStop);

    Node subscriber_node;
    std::vector<std::shared_ptr<TopicStats>> stats;
    stats.reserve(kTopics.size());

    std::cout << "SUB_READY node=" << subscriber_node.getNodeUuid()
              << " topics=" << kTopics.size() << '\n';

    for (const char* topic : kTopics) {
        auto state = std::make_shared<TopicStats>();
        state->topic = topic;
        if (!subscriber_node.subscribe<rf::msgs::ExampleMsg>(topic,
                [state](const std::shared_ptr<const rf::msgs::ExampleMsg>& message) {
                    const int sequence = message->age();
                    int expected = -1;
                    state->first_sequence.compare_exchange_strong(expected, sequence);

                    const int previous = state->last_sequence.exchange(sequence);
                    if (previous >= sequence) {
                        state->out_of_order = true;
                    }
                    if (message->name() != state->topic) {
                        state->wrong_topic = true;
                    }
                    ++state->received;
                })) {
            std::cerr << "SUB_RESULT status=failed reason=subscribe topic=" << topic << '\n';
            return 1;
        }
        stats.push_back(std::move(state));
        std::cout << "SUB_TOPIC topic=" << topic << '\n';
    }

    const auto first_message_deadline = std::chrono::steady_clock::now() + kFirstMessageTimeout;
    while (!allTopicsReceived(stats) && std::chrono::steady_clock::now() < first_message_deadline) {
        std::this_thread::sleep_for(5ms);
    }
    if (allTopicsReceived(stats)) {
        std::this_thread::sleep_for(kCollectionTime);
    }

    for (const auto& state : stats) {
        printStatus(*state, "initial");
    }

    bool valid = allTopicsReceived(stats);
    for (const auto& state : stats) {
        if (!passed(*state)) {
            valid = false;
            std::cerr << "SUB_FAILURE topic=" << state->topic
                      << " first_sequence_limit=" << kMaxFirstSequence
                      << " minimum_received=" << kMinReceivedPerTopic << '\n';
        }
    }

    std::cout << "SUB_CHECK status=" << (valid ? "passed" : "failed") << '\n';
    std::cout << "Running continuously; press Ctrl+C to stop.\n";

    auto next_status = std::chrono::steady_clock::now() + kStatusPeriod;
    while (stop_requested == 0) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= next_status) {
            for (const auto& state : stats) {
                printStatus(*state, "running");
            }
            next_status = now + kStatusPeriod;
        }
        std::this_thread::sleep_for(20ms);
    }

    std::cout << "SUB_RESULT status=stopped\n";
    return 0;
}
