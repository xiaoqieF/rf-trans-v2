#include "trans/publisher.hpp"

namespace rf
{
namespace trans
{
Publisher::Publisher(const MessagePublisherInfo& publisher)
    : node_shared_(NodeShared::getInstance()),
      message_publisher_(publisher)
{
    if (message_publisher_.getOptions().throttled()) {
        period_ns_ = 1e9 / publisher.getOptions().getMsgsPerSec();
    }
}

bool Publisher::publish(const ProtoMsg& msg)
{
    std::unique_ptr<ProtoMsg> msg_ptr{nullptr};
    msg_ptr.reset(msg.New());
    msg_ptr->CopyFrom(msg);

    return publishImpl(std::move(msg_ptr));
}

bool Publisher::publish(std::unique_ptr<ProtoMsg> msg)
{
    return publishImpl(std::move(msg));
}

bool Publisher::publishImpl(std::unique_ptr<ProtoMsg> msg)
{
    if (!valid()) {
        return false;
    }

    if (message_publisher_.getMsgType() != msg->GetTypeName()) {
        elog::error("Publisher::publish() type mismatch. Type advertise: {}"
            ", Type published: {}.", message_publisher_.getMsgType(), msg->GetTypeName());
        return false;
    }

    if (!updateThrottling()) {
        return true;
    }

    return node_shared_.publishMessage(message_publisher_, std::move(msg));
}

bool Publisher::hasConnections() const
{
    const std::string& topic = message_publisher_.getTopic();
    const std::string& msg_type = message_publisher_.getMsgType();

    return valid() && node_shared_.hasSubscribers(topic, msg_type);
}

bool Publisher::throttledUpdateReady() const
{
    if (!message_publisher_.getOptions().throttled()) {
        return true;
    }

    auto now = std::chrono::steady_clock::now();

    auto elapsed = now - last_pub_time_;
    return std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count() >= period_ns_;
}

bool Publisher::updateThrottling()
{
    if (!message_publisher_.getOptions().throttled()) {
        return true;
    }

    if (!throttledUpdateReady()) {
        return false;
    }

    last_pub_time_ = std::chrono::steady_clock::now();
    return true;
}

} // namespace trans
} // namespace rf
