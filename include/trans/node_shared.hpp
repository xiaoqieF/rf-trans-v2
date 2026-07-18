#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "zmq.hpp"

#include "trans/trans_types.hpp"
#include "trans/discovery.hpp"
#include "trans/handler_wrapper.hpp"
#include "trans/rep_handler.hpp"
#include "trans/req_handler.hpp"
#include "trans/publish_msg_details.hpp"


namespace rf
{
namespace trans
{
class NodeShared
{
public:
    static NodeShared& getInstance();
    NodeShared(const NodeShared&) = delete;
    NodeShared& operator=(const NodeShared&) = delete;

    bool publish(const std::string& topic, char* data, const size_t data_size,
        DeallocFunc* ffn, const std::string& msg_type);
    void sendPendingRemoteReqs(const std::string& topic,
        const std::string& req_type, const std::string& rep_type);
    bool advertisePublisher(const ServicePublisherInfo& pub);
    bool unsubscribe(const std::string& topic, const std::string& node_uuid);
    bool getServicePublishers(const std::string& topic, AddressMap<ServicePublisherInfo>& publishers) const;

private:
    NodeShared();
    virtual ~NodeShared();

    bool initializeSockets();
    void receiveMsgLoop();
    // todo: use thread pool to handle local pub
    void localPubLoop();
    void remotePubLoop();
    void serviceHandleLoop();

    void handleRequest(const std::deque<std::unique_ptr<RemoteRequest>>&);
    void handleResponse(const std::deque<std::unique_ptr<RemoteResponse>>&);

    void recvMsgUpdate();
    void recvSrvRequest();
    void recvSrvResponse();

    void onNewConnection(const MessagePublisherInfo& pub);
    void onNewDisconnection(const MessagePublisherInfo& pub);
    void onNewSrvConnection(const ServicePublisherInfo& pub);
    void onNewSrvDisconnection(const ServicePublisherInfo& pub);
    void onNewRegistration(const MessagePublisherInfo& pub);
    void onEndRegistration(const MessagePublisherInfo& pub);

    enum class SubscriberCommandType
    {
        Connect,
        Subscribe,
        Unsubscribe,
    };

    struct SubscriberCommand
    {
        SubscriberCommandType type;
        std::string value;
        std::vector<MessagePublisherInfo> registrations;
    };

    void enqueueSubscriberCommand(SubscriberCommandType type, std::string value,
        std::vector<MessagePublisherInfo> registrations = {});
    void applySubscriberCommands();

    HandlerInfo checkHandlerInfo(const std::string& topic) const;
    SubscriberInfo checkSubscriberInfo(const std::string& topic, const std::string& msg_type) const;

private:
    static constexpr size_t kMsgQueueLimit = 1000;
    static constexpr int kDefaultMsgDiscoveryPort = 10319;
    static constexpr int kDefaultSrvDiscoveryPort = 10320;

    std::string response_receiver_uuid_{generateUuidV4()};
    std::string replier_uuid_{generateUuidV4()};
    std::string process_uuid_{generateUuidV4()};

    const std::string discovery_ip_{"239.255.0.7"};
    int msg_discovery_port_{kDefaultMsgDiscoveryPort};
    int srv_discovery_port_{kDefaultSrvDiscoveryPort};

    // Serializes a complete multipart PUB message.
    mutable std::mutex publisher_mutex_;
    std::thread receive_msg_thread_;

    TopicStorage<MessagePublisherInfo> remote_publishers_;        // keep all remote publishers
    TopicStorage<MessagePublisherInfo> remote_subscribers_;       // keep all remote subscribers
    HandlerWrapper local_subscribers_;

    mutable std::recursive_mutex service_mutex_;
    /// TODO: make this a multiset.
    std::vector<std::string> service_connections_;                // keep remote service server addr
    HandlerStorage<IRepHandler> response_handlers_;               // local repliers
    HandlerStorage<IReqHandler> request_handlers_;                // store pending requests

    std::string host_address_;

    std::unique_ptr<zmq::context_t> context_;

    std::unique_ptr<zmq::socket_t> subscriber_;
    std::unique_ptr<zmq::socket_t> publisher_;
    std::string my_address_;

    // Discovery callbacks enqueue changes; receive_msg_thread_ owns subscriber_.
    std::deque<SubscriberCommand> subscriber_commands_;
    mutable std::mutex subscriber_commands_mutex_;

    std::unique_ptr<zmq::socket_t> requester_;
    std::unique_ptr<zmq::socket_t> response_receiver_;
    std::string my_requester_address_;

    std::unique_ptr<zmq::socket_t> replier_;
    std::string my_replier_address_;

    std::unique_ptr<MsgDiscovery> msg_discovery_;
    std::unique_ptr<SrvDiscovery> srv_discovery_;

    std::atomic_bool exit_{false};

    // For local publish, handle it in local_pub_thread
    std::thread local_pub_thread_;
    std::deque<std::unique_ptr<PublishMsgDetails>> local_pub_queue_;
    mutable std::mutex local_pub_mutex_;
    std::condition_variable local_pub_cv_;

    // For remote msg, handle it in single thread
    std::thread remote_msg_handle_thread_;
    std::deque<std::unique_ptr<RemoteMsg>> remote_msg_queue_;
    mutable std::mutex remote_msg_mutex_;
    std::condition_variable remote_msg_cv_;

    // For remote Service request and response, handle it in single thread
    std::thread service_handle_thread_;
    std::deque<std::unique_ptr<RemoteRequest>> remote_request_queue_;
    std::deque<std::unique_ptr<RemoteResponse>> remote_response_queue_;
    mutable std::mutex remote_service_msg_mutex_;
    std::condition_variable remote_service_msg_cv_;

    friend class Node;
    friend class Publisher;
};

} // namespace trans
} // namespace rf
