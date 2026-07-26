#include <chrono>
#include <csignal>
#include <cstddef>
#include <iostream>
#include <string>
#include <thread>

#include "msgs/example.pb.h"
#include "trans/advertise_options.hpp"
#include "trans/node.hpp"

using namespace std::chrono_literals;
using namespace rf::trans;

namespace
{
constexpr const char* kTopic = "/examples/shm/large_payload";
constexpr size_t kPayloadSize = 1024 * 1024; // 1 MiB; large enough to exercise the shm ring.
constexpr auto kPublishPeriod = 200ms;
constexpr auto kStatusPeriod = 1s;

volatile std::sig_atomic_t stop_requested = 0;

void requestStop(int)
{
    stop_requested = 1;
}

// Deterministic non-uniform content so the subscriber can verify the payload
// survived the shm round trip byte-for-byte without any out-of-band channel.
std::string buildPayload(const size_t size)
{
    std::string payload(size, '\0');
    for (size_t i = 0; i < size; ++i) {
        payload[i] = static_cast<char>(static_cast<unsigned char>((i * 131 + 7) % 251));
    }
    return payload;
}
} // namespace

int main()
{
    std::signal(SIGINT, requestStop);
    std::signal(SIGTERM, requestStop);

    Node publisher_node;
    AdvertiseMessageOptions options;
    // Shm transport only applies to Scope::HOST (or narrower): it hands a
    // pointer to shared memory to same-host subscribers, so a subscriber
    // over the network could never dereference it.
    options.setScope(Scope::HOST);
    options.setUseShm(true);

    auto publisher = publisher_node.advertise<rf::msgs::ExampleMsg>(kTopic, options);
    if (!publisher.valid()) {
        std::cerr << "SHM_PUB_RESULT status=failed reason=advertise topic=" << kTopic << '\n';
        return 1;
    }

    const std::string payload = buildPayload(kPayloadSize);
    std::cout << "SHM_PUB_READY node=" << publisher_node.getNodeUuid()
              << " topic=" << kTopic << " payload_bytes=" << kPayloadSize << '\n';
    std::cout << "Running continuously; press Ctrl+C to stop.\n";

    bool connected = false;
    int sequence = 0;
    auto next_publish = std::chrono::steady_clock::now();
    auto next_status = next_publish + kStatusPeriod;

    while (stop_requested == 0) {
        const auto now = std::chrono::steady_clock::now();

        if (publisher.hasConnections() && !connected) {
            connected = true;
            next_publish = now;
            std::cout << "SHM_PUB_CONNECTED topic=" << kTopic << '\n';
        }
        if (!publisher.hasConnections() && connected) {
            connected = false;
            std::cout << "SHM_PUB_DISCONNECTED topic=" << kTopic << " sent=" << sequence << '\n';
        }

        if (connected && now >= next_publish) {
            auto message = std::make_unique<rf::msgs::ExampleMsg>();
            message->set_name(kTopic);
            message->set_age(sequence++);
            message->set_payload(payload);
            if (!publisher.publish(std::move(message))) {
                std::cerr << "SHM_PUB_RESULT status=failed reason=publish topic=" << kTopic << '\n';
                return 1;
            }
            next_publish += kPublishPeriod;
        }

        if (now >= next_status) {
            std::cout << "SHM_PUB_STATUS topic=" << kTopic
                      << " connected=" << std::boolalpha << connected
                      << " sent=" << sequence << '\n';
            next_status = now + kStatusPeriod;
        }
        std::this_thread::sleep_for(5ms);
    }

    std::cout << "SHM_PUB_RESULT status=stopped sent=" << sequence << '\n';
    return 0;
}
