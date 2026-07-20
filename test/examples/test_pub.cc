#include <array>
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
constexpr auto kPublishPeriod = 10ms;
constexpr auto kStatusPeriod = 1s;

volatile std::sig_atomic_t stop_requested = 0;

void requestStop(int)
{
    stop_requested = 1;
}

struct TopicPublisher
{
    std::string topic;
    std::unique_ptr<Publisher> publisher;
    int next_sequence{0};
    bool connected{false};
    std::chrono::steady_clock::time_point next_publish{};
};

void printStatus(const TopicPublisher& state)
{
    std::cout << "PUB_STATUS topic=" << state.topic
              << " connected=" << std::boolalpha << state.connected
              << " sent=" << state.next_sequence << '\n';
}
} // namespace

int main()
{
    std::signal(SIGINT, requestStop);
    std::signal(SIGTERM, requestStop);

    Node publisher_node;
    std::vector<TopicPublisher> publishers;
    publishers.reserve(kTopics.size());

    for (const char* topic : kTopics) {
        auto publisher = publisher_node.advertise<rf::msgs::ExampleMsg>(topic);
        if (!publisher.valid()) {
            std::cerr << "PUB_RESULT status=failed reason=advertise topic=" << topic << '\n';
            return 1;
        }
        publishers.push_back({topic, std::make_unique<Publisher>(publisher)});
    }

    std::cout << "PUB_READY node=" << publisher_node.getNodeUuid()
              << " topics=" << publishers.size() << '\n';
    for (const auto& state : publishers) {
        std::cout << "PUB_TOPIC topic=" << state.topic << '\n';
    }
    std::cout << "Running continuously; press Ctrl+C to stop.\n";
    auto next_status = std::chrono::steady_clock::now();

    while (stop_requested == 0) {
        const auto now = std::chrono::steady_clock::now();

        for (auto& state : publishers) {
            const bool has_connections = state.publisher->hasConnections();
            if (has_connections && !state.connected) {
                state.connected = true;
                std::cout << "PUB_CONNECTED topic=" << state.topic << '\n';
            }
            if (!has_connections && state.connected) {
                state.connected = false;
                std::cout << "PUB_DISCONNECTED topic=" << state.topic
                          << " sent=" << state.next_sequence << '\n';
            }
            if (!state.connected) {
                continue;
            }
            if (now < state.next_publish) {
                continue;
            }

            auto message = std::make_unique<rf::msgs::ExampleMsg>();
            message->set_name(state.topic);
            message->set_age(state.next_sequence++);
            if (!state.publisher->publish(std::move(message))) {
                std::cerr << "PUB_RESULT status=failed reason=publish topic=" << state.topic << '\n';
                return 1;
            }
            state.next_publish = now + kPublishPeriod;
        }

        if (now >= next_status) {
            for (const auto& state : publishers) {
                printStatus(state);
            }
            next_status = now + kStatusPeriod;
        }
        std::this_thread::sleep_for(1ms);
    }

    for (const auto& state : publishers) {
        printStatus(state);
    }
    std::cout << "PUB_RESULT status=stopped\n";
    return 0;
}
