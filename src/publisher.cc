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

    const std::string& topic = message_publisher_.getTopic();
    const std::string msg_type = message_publisher_.getMsgType();

    auto subscribers = node_shared_.checkSubscriberInfo(topic, msg_type);

    // Pub to remote subscribers
    // To use zero-copy of zmq, we use new/delete
    char* serialize_buffer = nullptr;
    if (subscribers.have_remote) {
        auto msg_size = msg->ByteSizeLong();
        serialize_buffer = new char[msg_size];

        if (!msg->SerializeToArray(serialize_buffer, msg_size)) {
            elog::error("Publisher::publish() error serializing data");
            delete [] serialize_buffer;
            return false;
        }

        auto deallocator = [] (void* buffer, void*) {
            delete [] reinterpret_cast<char*>(buffer);
        };

        // zmq::publish will free the mem, don't double free
        if (!node_shared_.publish(topic, serialize_buffer, msg_size, deallocator, msg_type)) {
            return false;
        }
    }

    // Pub to local subscribers
    if (subscribers.local_handler_info.have_local) {
        auto pub_msg_details = std::make_unique<PublishMsgDetails>();

        pub_msg_details->info.setTopic(topic);
        pub_msg_details->info.setMsgType(msg_type);
        pub_msg_details->msg_copy = std::move(msg);

        pub_msg_details->publisher_node_uuid = message_publisher_.getNodeUuid();

        for (const auto& [n_uuid, mp] : subscribers.local_handler_info.local_handlers) {
            for (const auto& [h_uuid, handler] : mp) {
                if (!handler) {
                    elog::error("Publisher::publish() error, null local subscription handler");
                    continue;
                }

                if (handler->getMsgType() != msg_type) {
                    elog::error("Publisher::publish() error, msg type mismatch");
                    continue;
                }

                pub_msg_details->local_handlers.push_back(handler);
            }
        }

        {
            std::lock_guard lock(node_shared_.local_pub_mutex_);
            node_shared_.local_pub_queue_.push_back(std::move(pub_msg_details));
            node_shared_.local_pub_cv_.notify_one();
        }
    }

    return true;
}

bool Publisher::hasConnections() const
{
    const std::string& topic = message_publisher_.getTopic();
    const std::string& msg_type = message_publisher_.getMsgType();

    return valid() &&
        (node_shared_.local_subscribers_.hasSubscriber(topic, msg_type) ||
         node_shared_.remote_subscribers_.hasTopic(topic, msg_type));
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