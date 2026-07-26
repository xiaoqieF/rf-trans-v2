#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

#include "msgs/example.pb.h"
#include "trans/node.hpp"

using namespace std::chrono_literals;
using namespace rf::trans;

namespace
{
constexpr const char* kTopic = "/examples/shm/large_payload";
constexpr size_t kExpectedPayloadSize = 1024 * 1024;
constexpr int kMinReceived = 3;
constexpr auto kFirstMessageTimeout = 5s;
constexpr auto kCollectionTime = 2s;
constexpr auto kStatusPeriod = 1s;

volatile std::sig_atomic_t stop_requested = 0;

void requestStop(int)
{
    stop_requested = 1;
}

// Must match test_shm_pub.cc's buildPayload() so mismatches are caught.
std::string buildPayload(const size_t size)
{
    std::string payload(size, '\0');
    for (size_t i = 0; i < size; ++i) {
        payload[i] = static_cast<char>(static_cast<unsigned char>((i * 131 + 7) % 251));
    }
    return payload;
}

struct Stats
{
    std::mutex mutex;
    std::atomic<int> first_sequence{-1};
    std::atomic<int> last_sequence{-1};
    std::atomic<int> received{0};
    std::atomic<uint64_t> bytes_received{0};
    std::atomic<bool> out_of_order{false};
    std::atomic<bool> size_mismatch{false};
    std::atomic<bool> content_mismatch{false};
};

void printStatus(const Stats& stats, const char* phase)
{
    std::cout << "SHM_SUB_STATUS phase=" << phase
              << " topic=" << kTopic
              << " first=" << stats.first_sequence.load()
              << " last=" << stats.last_sequence.load()
              << " received=" << stats.received.load()
              << " bytes=" << stats.bytes_received.load()
              << " ordered=" << std::boolalpha << !stats.out_of_order.load()
              << " size_ok=" << !stats.size_mismatch.load()
              << " content_ok=" << !stats.content_mismatch.load() << '\n';
}

bool passed(const Stats& stats)
{
    return stats.first_sequence.load() >= 0 &&
        stats.received.load() >= kMinReceived &&
        !stats.out_of_order.load() &&
        !stats.size_mismatch.load() &&
        !stats.content_mismatch.load();
}
} // namespace

int main()
{
    std::signal(SIGINT, requestStop);
    std::signal(SIGTERM, requestStop);

    const std::string expected_payload = buildPayload(kExpectedPayloadSize);

    Node subscriber_node;
    Stats stats;

    std::cout << "SHM_SUB_READY node=" << subscriber_node.getNodeUuid() << " topic=" << kTopic << '\n';

    const bool subscribed = subscriber_node.subscribe<rf::msgs::ExampleMsg>(kTopic,
        [&stats, &expected_payload](const std::shared_ptr<const rf::msgs::ExampleMsg>& message) {
            std::lock_guard lock(stats.mutex);
            const int sequence = message->age();

            int expected = -1;
            stats.first_sequence.compare_exchange_strong(expected, sequence);
            const int previous = stats.last_sequence.exchange(sequence);
            if (previous >= sequence) {
                stats.out_of_order = true;
            }
            if (message->payload().size() != expected_payload.size()) {
                stats.size_mismatch = true;
            } else if (message->payload() != expected_payload) {
                stats.content_mismatch = true;
            }
            stats.bytes_received.fetch_add(message->payload().size());
            ++stats.received;
        });
    if (!subscribed) {
        std::cerr << "SHM_SUB_RESULT status=failed reason=subscribe topic=" << kTopic << '\n';
        return 1;
    }

    const auto first_message_deadline = std::chrono::steady_clock::now() + kFirstMessageTimeout;
    while (stats.first_sequence.load() < 0 && std::chrono::steady_clock::now() < first_message_deadline) {
        std::this_thread::sleep_for(20ms);
    }
    if (stats.first_sequence.load() >= 0) {
        std::this_thread::sleep_for(kCollectionTime);
    }

    printStatus(stats, "initial");

    const bool valid = passed(stats);
    if (!valid) {
        std::cerr << "SHM_SUB_FAILURE minimum_received=" << kMinReceived
                  << " expected_payload_bytes=" << expected_payload.size() << '\n';
    }
    std::cout << "SHM_SUB_CHECK status=" << (valid ? "passed" : "failed") << '\n';
    std::cout << "Running continuously; press Ctrl+C to stop.\n";

    auto next_status = std::chrono::steady_clock::now() + kStatusPeriod;
    while (stop_requested == 0) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= next_status) {
            printStatus(stats, "running");
            next_status = now + kStatusPeriod;
        }
        std::this_thread::sleep_for(20ms);
    }

    std::cout << "SHM_SUB_RESULT status=stopped\n";
    return 0;
}
