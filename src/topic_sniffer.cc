#include "trans/topic_sniffer.hpp"

#include <atomic>
#include <mutex>
#include <set>
#include <thread>
#include <utility>

#include "zmq.hpp"

#include "elog/elog.h"
#include "trans/details/discovery.hpp"
#include "trans/details/helpers.hpp"

namespace rf
{
namespace trans
{
namespace
{
constexpr int kPollTimeoutMs = 250;
}

class TopicSniffer::Impl
{
public:
    Impl(std::string topic, const TopicSnifferOptions& options)
        : topic_(std::move(topic)),
          process_uuid_(generateUuidV4()),
          node_uuid_(generateUuidV4()),
          context_(1),
          subscriber_(context_, ZMQ_SUB),
          discovery_(process_uuid_, options.multicast_group, options.message_port, options.host_address)
    {
    }

    ~Impl()
    {
        stop();
    }

    void setMessageCallback(std::function<void(const SniffedMessage&)> callback)
    {
        callback_ = std::move(callback);
    }

    void start()
    {
        if (started_.exchange(true)) {
            return;
        }

        discovery_.setConnectionCb([this](const MessagePublisherInfo& pub) { onPublisherFound(pub); });
        discovery_.start();
        discovery_.discover(topic_);

        receive_thread_ = std::thread(&Impl::receiveLoop, this);
    }

    void stop()
    {
        if (!started_.exchange(false)) {
            return;
        }

        exit_ = true;
        if (receive_thread_.joinable()) {
            receive_thread_.join();
        }

        discovery_.setConnectionCb(nullptr);

        std::lock_guard<std::mutex> lock(publishers_mutex_);
        for (const auto& registration : registrations_) {
            discovery_.unRegisterNode(registration);
        }
        registrations_.clear();
        connected_addresses_.clear();

        discovery_.stop();
    }

private:
    void onPublisherFound(const MessagePublisherInfo& pub)
    {
        if (pub.getTopic() != topic_) {
            return;
        }

        const std::string addr = pub.getAddr();
        {
            std::lock_guard<std::mutex> lock(publishers_mutex_);
            if (connected_addresses_.count(addr) != 0) {
                return;
            }
            connected_addresses_.insert(addr);
        }

        try {
            subscriber_.connect(addr);
            subscriber_.set(zmq::sockopt::subscribe, topic_);
        } catch (const zmq::error_t& e) {
            elog::error("TopicSniffer failed to connect to [{}]: {}", addr, e.what());
            return;
        }

        MessagePublisherInfo registration(topic_, "", pub.getProcessUuid(), process_uuid_, node_uuid_,
            kGenericMessageType, AdvertiseMessageOptions{});
        discovery_.registerNode(registration);

        std::lock_guard<std::mutex> lock(publishers_mutex_);
        registrations_.push_back(registration);
    }

    void receiveLoop()
    {
        while (!exit_) {
            zmq::pollitem_t items[] = {
                {subscriber_, 0, ZMQ_POLLIN, 0},
            };

            try {
                zmq::poll(items, 1, std::chrono::milliseconds(kPollTimeoutMs));
            } catch (...) {
                continue;
            }

            if (items[0].revents & ZMQ_POLLIN) {
                receiveMessage();
            }
        }
    }

    void receiveMessage()
    {
        SniffedMessage sniffed;
        zmq::message_t msg(0);
        try {
            if (!subscriber_.recv(msg)) {
                return;
            }
            sniffed.topic = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

            if (!subscriber_.recv(msg)) {
                return;
            }
            sniffed.sender = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

            if (!subscriber_.recv(msg)) {
                return;
            }
            sniffed.raw_data = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

            if (!subscriber_.recv(msg)) {
                return;
            }
            sniffed.msg_type = std::string(reinterpret_cast<char*>(msg.data()), msg.size());
        } catch (const zmq::error_t& e) {
            elog::error("TopicSniffer receive error: {}", e.what());
            return;
        }

        sniffed.received_at = std::chrono::steady_clock::now();

        if (callback_) {
            callback_(sniffed);
        }
    }

    const std::string topic_;
    const std::string process_uuid_;
    const std::string node_uuid_;

    zmq::context_t context_;
    zmq::socket_t subscriber_;
    MsgDiscovery discovery_;

    std::function<void(const SniffedMessage&)> callback_;

    std::atomic_bool started_{false};
    std::atomic_bool exit_{false};
    std::thread receive_thread_;

    std::mutex publishers_mutex_;
    std::set<std::string> connected_addresses_;
    std::vector<MessagePublisherInfo> registrations_;
};

TopicSniffer::TopicSniffer(std::string topic, TopicSnifferOptions options)
    : impl_(std::make_unique<Impl>(std::move(topic), options))
{
}

TopicSniffer::~TopicSniffer() = default;

void TopicSniffer::setMessageCallback(std::function<void(const SniffedMessage&)> callback)
{
    impl_->setMessageCallback(std::move(callback));
}

void TopicSniffer::start()
{
    impl_->start();
}

void TopicSniffer::stop()
{
    impl_->stop();
}

} // namespace trans
} // namespace rf
