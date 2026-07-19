#include <cerrno>
#include <csignal>
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
using rf::trans::AdvertiseServiceOptions;
using rf::trans::Node;
using rf::trans::Scope;

volatile std::sig_atomic_t keep_running = 1;

void stopServer(int)
{
    keep_running = 0;
}

bool writeReadyStatus(const int fd, const char status)
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

bool parseArguments(const int argc, char* argv[], std::string& topic, int& ready_fd)
{
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
        } else {
            return false;
        }
    }
    return !topic.empty() && ready_fd >= 0;
}
} // namespace

int main(int argc, char* argv[])
{
    std::string topic;
    int ready_fd = -1;
    if (!parseArguments(argc, argv, topic, ready_fd)) {
        std::cerr << "usage: benchmark_service_server --topic <topic> --ready-fd <fd>\n";
        return EXIT_FAILURE;
    }

    Node server;
    AdvertiseServiceOptions options;
    options.setScope(Scope::HOST);
    const bool advertised = server.advertise<ExampleMsg, ExampleMsg>(topic,
        [](const std::shared_ptr<const ExampleMsg>& request, std::shared_ptr<ExampleMsg> reply) {
            if (!request) {
                return false;
            }
            reply->set_name(request->name());
            reply->set_age(request->age() + 1);
            return true;
        }, options);

    const bool ready = writeReadyStatus(ready_fd, advertised ? '1' : '0');
    close(ready_fd);
    if (!advertised || !ready) {
        return EXIT_FAILURE;
    }

    std::signal(SIGTERM, stopServer);
    std::signal(SIGINT, stopServer);
    while (keep_running) {
        pause();
    }
    return EXIT_SUCCESS;
}
