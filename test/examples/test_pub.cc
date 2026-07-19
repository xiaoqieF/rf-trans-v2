#include <chrono>
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
constexpr int kRoundCount = 8;
constexpr int kMessagesPerRound = 160;
constexpr auto kMessagePeriod = 5ms;
constexpr auto kOverallTimeout = 45s;
constexpr char kTopicPrefix[] = "/test/pub_sub_stress/";

struct PublisherRound
{
    std::unique_ptr<Publisher> publisher;
    int next_sequence{0};
    bool started{false};
    std::chrono::steady_clock::time_point next_publish{};
};

std::string topicForRound(const int round)
{
    return std::string{kTopicPrefix} + std::to_string(round);
}
} // namespace

int main()
{
    Node publisher_node;
    std::vector<PublisherRound> rounds;
    rounds.reserve(kRoundCount);

    for (int round = 0; round < kRoundCount; ++round) {
        auto publisher = publisher_node.advertise<rf::msgs::ExampleMsg>(topicForRound(round));
        if (!publisher.valid()) {
            std::cerr << "failed to advertise round " << round << '\n';
            return 1;
        }
        rounds.push_back({std::make_unique<Publisher>(publisher)});
    }

    std::cout << "publisher ready: start test_sub within 45 seconds\n";
    const auto deadline = std::chrono::steady_clock::now() + kOverallTimeout;

    while (std::chrono::steady_clock::now() < deadline) {
        const auto now = std::chrono::steady_clock::now();
        bool complete = true;

        for (int round = 0; round < kRoundCount; ++round) {
            auto& state = rounds[round];
            if (state.next_sequence == kMessagesPerRound) {
                continue;
            }
            complete = false;

            if (!state.publisher->hasConnections()) {
                if (state.started) {
                    const int sent = state.next_sequence;
                    state.next_sequence = kMessagesPerRound;
                    std::cout << "round " << round << " unsubscribed after "
                              << sent << " messages\n";
                }
                continue;
            }

            if (now < state.next_publish) {
                continue;
            }

            if (!state.started) {
                state.started = true;
                std::cout << "round " << round << " registered; starting burst\n";
            }

            auto message = std::make_unique<rf::msgs::ExampleMsg>();
            message->set_name("round=" + std::to_string(round));
            message->set_age(state.next_sequence++);
            if (!state.publisher->publish(std::move(message))) {
                std::cerr << "failed to publish round " << round << '\n';
                return 1;
            }
            state.next_publish = now + kMessagePeriod;
        }

        if (complete) {
            std::cout << "all rounds completed\n";
            std::this_thread::sleep_for(500ms);
            return 0;
        }
        std::this_thread::sleep_for(1ms);
    }

    std::cerr << "timed out waiting for all rounds to register\n";
    return 1;
}
