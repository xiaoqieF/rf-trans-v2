#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "trans/advertise_options.hpp"

namespace rf
{
namespace trans
{

/** Settings used by a read-only discovery observer. */
struct DiscoveryObserverOptions
{
    std::string multicast_group{"239.255.0.7"};
    std::uint16_t message_port{10319};
    std::uint16_t service_port{10320};

    // Empty uses the same interface selection as Node, including RF_HOST_IP.
    std::string host_address;
};

struct MessageEndpointInfo
{
    std::string topic;
    std::string address;
    std::string control_id;
    std::string process_uuid;
    std::string process_name;
    std::string node_uuid;
    std::string message_type;
    Scope scope{Scope::ALL};
    bool throttled{false};
    std::uint64_t messages_per_second{0};
};

struct ServiceEndpointInfo
{
    std::string topic;
    std::string address;
    std::string socket_id;
    std::string process_uuid;
    std::string process_name;
    std::string node_uuid;
    std::string request_type;
    std::string response_type;
    Scope scope{Scope::ALL};
};

/**
 * Observes endpoints announced through rf_trans discovery without registering
 * subscriptions or advertising a user-visible endpoint.
 */
class DiscoveryObserver
{
public:
    explicit DiscoveryObserver(const DiscoveryObserverOptions& options = {});
    ~DiscoveryObserver();

    DiscoveryObserver(const DiscoveryObserver&) = delete;
    DiscoveryObserver& operator=(const DiscoveryObserver&) = delete;
    DiscoveryObserver(DiscoveryObserver&&) noexcept;
    DiscoveryObserver& operator=(DiscoveryObserver&&) noexcept;

    std::vector<MessageEndpointInfo> messageEndpoints() const;
    std::vector<ServiceEndpointInfo> serviceEndpoints() const;
    std::string hostAddress() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace trans
} // namespace rf
