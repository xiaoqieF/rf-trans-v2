#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "trans/details/trans_types.hpp"

namespace rf
{
namespace trans
{

/** Settings used by a read-only topic sniffer. */
struct TopicSnifferOptions
{
    std::string multicast_group{"239.255.0.7"};
    std::uint16_t message_port{10319};

    // Empty uses the same interface selection as Node, including RF_HOST_IP.
    std::string host_address;
};

struct SniffedMessage
{
    std::string topic;
    std::string sender;
    std::string raw_data;
    std::string msg_type;
    Timestamp received_at;
};

/**
 * Subscribes to a message topic without knowing its protobuf type, exposing
 * the raw serialized bytes of every message received. Intended for CLI
 * diagnostics (rate/content monitoring), not for regular application use.
 */
class TopicSniffer
{
public:
    explicit TopicSniffer(std::string topic, TopicSnifferOptions options = {});
    ~TopicSniffer();

    TopicSniffer(const TopicSniffer&) = delete;
    TopicSniffer& operator=(const TopicSniffer&) = delete;

    void setMessageCallback(std::function<void(const SniffedMessage&)> callback);

    void start();
    void stop();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace trans
} // namespace rf
