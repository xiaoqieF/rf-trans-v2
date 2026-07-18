#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "msgs/example.pb.h"
#include "trans/node.hpp"

using namespace std::chrono_literals;
using namespace rf::trans;

namespace
{
constexpr int kRoundCount = 8;
constexpr int kMaxFirstSequence = 40;
constexpr int kMinReceivedPerRound = 20;
constexpr auto kFirstMessageTimeout = 3s;
constexpr auto kCollectionTime = 300ms;
constexpr auto kUnsubscribeSettleTime = 300ms;
constexpr char kTopicPrefix[] = "/test/pub_sub_stress/";

struct RoundStats
{
    std::atomic<int> first_sequence{-1};
    std::atomic<int> last_sequence{-1};
    std::atomic<int> received{0};
    std::atomic<int> callbacks_after_unsubscribe{0};
    std::atomic<bool> active{true};
    std::atomic<bool> out_of_order{false};
    std::atomic<bool> wrong_round{false};
};

std::string topicForRound(const int round)
{
    return std::string{kTopicPrefix} + std::to_string(round);
}
} // namespace

int main()
{
    Node subscriber_node;

    for (int round = 0; round < kRoundCount; ++round) {
        const std::string topic = topicForRound(round);
        const std::string expected_name = "round=" + std::to_string(round);
        auto stats = std::make_shared<RoundStats>();

        if (!subscriber_node.subscribe<rf::msgs::ExampleMsg>(topic,
                [stats, expected_name](const std::shared_ptr<const rf::msgs::ExampleMsg>& message) {
                    const int sequence = message->age();
                    int expected = -1;
                    stats->first_sequence.compare_exchange_strong(expected, sequence);

                    const int previous = stats->last_sequence.exchange(sequence);
                    if (previous >= sequence) {
                        stats->out_of_order = true;
                    }
                    if (message->name() != expected_name) {
                        stats->wrong_round = true;
                    }
                    if (!stats->active.load()) {
                        ++stats->callbacks_after_unsubscribe;
                    }
                    ++stats->received;
                })) {
            std::cerr << "failed to subscribe to round " << round << '\n';
            return 1;
        }

        const auto first_message_deadline = std::chrono::steady_clock::now() + kFirstMessageTimeout;
        while (stats->first_sequence.load() < 0 && std::chrono::steady_clock::now() < first_message_deadline) {
            std::this_thread::sleep_for(5ms);
        }

        std::this_thread::sleep_for(kCollectionTime);
        if (!subscriber_node.unsubscribe(topic)) {
            std::cerr << "failed to unsubscribe from round " << round << '\n';
            return 1;
        }
        stats->active = false;
        std::this_thread::sleep_for(kUnsubscribeSettleTime);

        const int first_sequence = stats->first_sequence.load();
        const int received = stats->received.load();
        const int callbacks_after_unsubscribe = stats->callbacks_after_unsubscribe.load();
        std::cout << "round " << round
                  << ": first=" << first_sequence
                  << ", received=" << received
                  << ", post_unsubscribe=" << callbacks_after_unsubscribe << '\n';

        if (first_sequence < 0 || first_sequence > kMaxFirstSequence ||
            received < kMinReceivedPerRound || stats->out_of_order || stats->wrong_round) {
            std::cerr << "round " << round << " failed: "
                      << "first message must arrive by sequence " << kMaxFirstSequence
                      << " and messages must be ordered\n";
            return 1;
        }
    }

    std::cout << "pub/sub stress scenario passed\n";
    return 0;
}
