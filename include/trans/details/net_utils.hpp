#pragma once

#include <cerrno>
#include <string>
#include <cstring>
#include <stdexcept>
#include <vector>

#include <arpa/inet.h>
#include <unistd.h>
#include <net/if.h>
#include <ifaddrs.h>
#include <sys/ioctl.h>
#include <netdb.h>

#include "elog/elog.h"
#include "trans/details/helpers.hpp"

namespace rf
{
namespace trans
{

// find all non-loopback interfaces on host.
// return loopback interface if there are no other available interfaces
inline std::vector<std::string> determineInterfaces()
{
    std::vector<std::string> result;
    struct ifaddrs* ifa = nullptr, *ifp = nullptr;
    int rc = getifaddrs(&ifp);
    if (rc < 0) {
        const int error = errno;
        const std::string message = "getifaddrs failed: " + std::string(strerror(error));
        elog::error("{}.", message);
        throw std::runtime_error(message);
    }

    for (ifa = ifp; ifa; ifa = ifa->ifa_next) {
        socklen_t salen;
        if (!ifa->ifa_addr) {
            continue;
        }
        if (ifa->ifa_addr->sa_family == AF_INET) {
            salen = sizeof(sockaddr_in);
        } else if (ifa->ifa_addr->sa_family == AF_INET6) {
            salen = sizeof(sockaddr_in6);
        } else {
            continue;
        }

        char ip[200];
        std::string interface;
        if (getnameinfo(ifa->ifa_addr, salen, ip, sizeof(ip), nullptr, 0, NI_NUMERICHOST) < 0) {
            elog::error("getnameinfo could not get the ip of interface: {}", ifa->ifa_name);
            continue;
        }
        // prefer non-loopback IPs
        if (!strcmp("127.0.0.1", ip) || strchr(ip, ':')) {
            continue;
        }
        // Dose not support multicast
        if (!(ifa->ifa_flags & IFF_MULTICAST)) {
            continue;
        }
        // Is not running.
        if (!(ifa->ifa_flags & IFF_UP)) {
            continue;
        }
        interface = std::string(ip);

        if (!interface.empty() && std::find(result.begin(), result.end(), interface) == result.end()) {
            result.push_back(interface);
        }
    }
    freeifaddrs(ifp);
    if (result.empty()) {
        elog::error("Couldn't find a preferred IP via the getifaddrs() call; "
            "I'm assuming that your IP "
            "address is 127.0.0.1.  This should work for local processes, "
            "but will almost certainly not work if you have remote processes.");
        return {"127.0.0.1"};
    }
    return result;
}

inline std::string determineHost()
{
    std::string host_ip;
    if (getEnv("RF_HOST_IP", host_ip) && !host_ip.empty()) {
        return host_ip;
    }
    auto interfaces = determineInterfaces();
    for (const auto& ip : interfaces) {
        // Prefer non-loopback ip
        if (ip != "127.0.0.1") {
            return ip;
        }
    }
    return interfaces.front();
}

} // namespace trans
} // namespace rf
