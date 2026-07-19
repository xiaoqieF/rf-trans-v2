#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
#include <string>

#include <unistd.h>

#include "msgs/example.pb.h"
#include "trans/node.hpp"

namespace
{
using rf::msgs::ExampleMsg;
using rf::trans::Node;

volatile std::sig_atomic_t keep_running = 1;

void stopSubscriber(int)
{
    keep_running = 0;
}

bool writeStatus(const int fd, const char status)
{
    while (true) {
        const ssize_t written = write(fd, &status, sizeof(status));
        if (written == sizeof(status)) {
            return true;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
}

bool parseArguments(const int argc, char* argv[], std::string& topic, int& ready_fd, size_t& payload_size)
{
    bool payload_size_supplied = false;
    for (int index = 1; index < argc; ++index) {
        const std::string argument(argv[index]);
        if (argument == "--topic" && index + 1 < argc) {
            topic = argv[++index];
        } else if (argument == "--ready-fd" && index + 1 < argc) {
            try {
                ready_fd = std::stoi(argv[++index]);
            } catch (const std::exception&) {
                return false;
            }
        } else if (argument == "--payload-size" && index + 1 < argc) {
            try {
                payload_size = std::stoull(argv[++index]);
                payload_size_supplied = true;
            } catch (const std::exception&) {
                return false;
            }
        } else {
            return false;
        }
    }
    return !topic.empty() && ready_fd >= 0 && payload_size_supplied;
}
} // namespace

int main(int argc, char* argv[])
{
    std::string topic;
    int ready_fd = -1;
    size_t payload_size = 0;
    if (!parseArguments(argc, argv, topic, ready_fd, payload_size)) {
        std::cerr << "usage: benchmark_pubsub_subscriber --topic <topic> --ready-fd <fd> --payload-size <bytes>\n";
        return EXIT_FAILURE;
    }

    const std::string expected_payload(payload_size, 'x');
    std::atomic<uint64_t> received{0};
    Node subscriber;
    const bool subscribed = subscriber.subscribe<ExampleMsg>(topic,
        [&expected_payload, &received, ready_fd](const std::shared_ptr<const ExampleMsg>& message) {
            const uint64_t expected_sequence = received.load(std::memory_order_acquire) + 1;
            const bool payload_matches = message && message->name() == expected_payload;
            if (payload_matches && message->age() == static_cast<int>(expected_sequence)) {
                received.fetch_add(1, std::memory_order_acq_rel);
                writeStatus(ready_fd, '1');
            } else if (!payload_matches || !message || message->age() > static_cast<int>(expected_sequence)) {
                writeStatus(ready_fd, '0');
            }
        });

    const bool ready = writeStatus(ready_fd, subscribed ? '1' : '0');
    if (!subscribed || !ready) {
        close(ready_fd);
        return EXIT_FAILURE;
    }

    std::signal(SIGTERM, stopSubscriber);
    std::signal(SIGINT, stopSubscriber);
    while (keep_running) {
        pause();
    }
    close(ready_fd);
    return EXIT_SUCCESS;
}
