#pragma once

#include "trans/node_shared.hpp"

namespace rf
{
namespace trans
{
class Publisher
{
public:
    Publisher() : node_shared_(NodeShared::getInstance()) {}
    Publisher(const MessagePublisherInfo& publisher);
    virtual ~Publisher() = default;

    operator bool() { return valid(); }
    operator bool() const { return valid(); }

    bool valid() const { return !message_publisher_.getTopic().empty(); }
    bool publish(const ProtoMsg& msg);
    // Zero copy when pub msg to local subscribers
    bool publish(std::unique_ptr<ProtoMsg> msg);
    bool hasConnections() const;
    bool throttledUpdateReady() const;

private:
    bool updateThrottling();
    bool publishImpl(std::unique_ptr<ProtoMsg> msg);

    NodeShared& node_shared_;
    MessagePublisherInfo message_publisher_;
    Timestamp last_pub_time_;
    double period_ns_;
};

} // namespace trans
} // namespace rf