#include "trans/node_shared.hpp"

namespace rf
{
namespace trans
{

NodeShared& NodeShared::getInstance()
{
    static NodeShared instance;
    return instance;
}

NodeShared::~NodeShared()
{
    elog::info("NodeShared destroyed.");
    exit_ = true;

    local_pub_cv_.notify_all();
    if (local_pub_thread_.joinable()) {
        local_pub_thread_.join();
    }

    if (receive_msg_thread_.joinable()) {
        receive_msg_thread_.join();
    }

    if (remote_msg_handle_thread_.joinable()) {
        remote_msg_handle_thread_.join();
    }

    if (service_handle_thread_.joinable()) {
        service_handle_thread_.join();
    }
}

NodeShared::NodeShared()
    : context_(new zmq::context_t(1)),
      subscriber_(new zmq::socket_t(*context_, ZMQ_SUB)),
      publisher_(new zmq::socket_t(*context_, ZMQ_PUB)),
      requester_(new zmq::socket_t(*context_, ZMQ_ROUTER)),
      response_receiver_(new zmq::socket_t(*context_, ZMQ_ROUTER)),
      replier_(new zmq::socket_t(*context_, ZMQ_ROUTER))
{
    msg_discovery_ = std::make_unique<MsgDiscovery>(process_uuid_, discovery_ip_, msg_discovery_port_);
    srv_discovery_ = std::make_unique<SrvDiscovery>(process_uuid_, discovery_ip_, srv_discovery_port_);

    if (!initializeSockets()) {
        return;
    }

    std::stringstream ss;
    ss << "Current host address: " << host_address_ << std::endl;
    ss << "Process UUID: " << process_uuid_ << std::endl;
    ss << "Bind at: [udp://" << discovery_ip_ << ":"
              << msg_discovery_port_ << "] for msg discovery\n";
    ss << "Bind at: [udp://" << discovery_ip_ << ":"
              << srv_discovery_port_ << "] for srv discovery\n";
    ss << "Bind at: [" << this->my_address_ << "] for pub/sub\n";
    ss << "Bind at: [" << this->my_replier_address_ << "] for srv. calls\n";
    ss << "Identity for receiving srv. requests: ["
              << replier_uuid_ << "]" << std::endl;
    ss << "Identity for receiving srv. responses: ["
              << response_receiver_uuid_ << "]" << std::endl;

    elog::debug(ss.str());

    receive_msg_thread_ = std::thread(&NodeShared::receiveMsgLoop, this);
    pthread_setname_np(receive_msg_thread_.native_handle(), "rf_recv_loop");

    // Triggered when a new publisher is discovered, as follows:
    // 1. Local publisher advertise a new topic
    // 2. Remote publisher advertise a new topic (discovery receives a new ADVERTISE message)
    msg_discovery_->setConnectionCb([this] (const MessagePublisherInfo& pub) {
        this->onNewConnection(pub);
    });

    // Triggered when a publisher is disconnected, as follows:
    // 1. Remote publisher unadvertises a topic (discovery receives a new UNADVERTISE message)
    // 2. Remote publisher process exits (discovery receives a new BYE message)
    // 3. Remote publisher heartbeat timeout
    msg_discovery_->setDisconnectionCb([this] (const MessagePublisherInfo& pub) {
        this->onNewDisconnection(pub);
    });

    // Triggered when remote subscriber connects to a local publisher successfully
    // In this case, discovery receives a NEW_CONNECTION message from the remote subscriber
    msg_discovery_->setRegistrationCb([this] (const MessagePublisherInfo& pub) {
        this->onNewRegistration(pub);
    });

    // Triggered when remote subscriber disconnects from a local publisher
    // In this case, discovery receives a END_CONNECTION message from the remote subscriber
    msg_discovery_->setUnregistrationCb([this] (const MessagePublisherInfo& pub) {
        this->onEndRegistration(pub);
    });

    srv_discovery_->setConnectionCb([this] (const ServicePublisherInfo& pub) {
        this->onNewSrvConnection(pub);
    });

    srv_discovery_->setDisconnectionCb([this] (const ServicePublisherInfo& pub) {
        this->onNewSrvDisconnection(pub);
    });

    msg_discovery_->start();
    srv_discovery_->start();

    local_pub_thread_ = std::thread(&NodeShared::localPubLoop, this);
    pthread_setname_np(local_pub_thread_.native_handle(), "rf_local_pub");

    remote_msg_handle_thread_ = std::thread(&NodeShared::remotePubLoop, this);
    pthread_setname_np(remote_msg_handle_thread_.native_handle(), "rf_remote_pub");

    service_handle_thread_ = std::thread(&NodeShared::serviceHandleLoop, this);
    pthread_setname_np(service_handle_thread_.native_handle(), "rf_srv_loop");

}

bool NodeShared::initializeSockets()
{
    try {
        host_address_ = msg_discovery_->getHostAddr();

        std::string any_tcp_ep = "tcp://" + host_address_ + ":*";

        int linger_val = 0;
        publisher_->set(zmq::sockopt::linger, linger_val);
        publisher_->set(zmq::sockopt::sndhwm, kDefaultSndHwm);
        publisher_->bind(any_tcp_ep.c_str());
        my_address_ = publisher_->get(zmq::sockopt::last_endpoint);

        subscriber_->set(zmq::sockopt::rcvhwm, kDefaultRcvHwm);

        response_receiver_->set(zmq::sockopt::routing_id, response_receiver_uuid_);
        response_receiver_->bind(any_tcp_ep.c_str());
        my_requester_address_ = response_receiver_->get(zmq::sockopt::last_endpoint);

        int route_on = 1;
        replier_->set(zmq::sockopt::routing_id, replier_uuid_);
        replier_->set(zmq::sockopt::linger, linger_val);
        replier_->set(zmq::sockopt::router_mandatory, route_on);
        replier_->bind(any_tcp_ep.c_str());
        my_replier_address_ = replier_->get(zmq::sockopt::last_endpoint);

        requester_->set(zmq::sockopt::linger, linger_val);
        requester_->set(zmq::sockopt::router_mandatory, route_on);
    } catch (const zmq::error_t& e) {
        elog::error("NodeShared::initializeSockets error: {}", e.what());
        return false;
    }

    return true;
}

void NodeShared::onNewConnection(const MessagePublisherInfo& pub)
{
    std::string topic = pub.getTopic();
    std::string addr = pub.getAddr();
    std::string remote_proc_uuid = pub.getProcessUuid();

    // elog::debug("Connection Callback");
    // std::stringstream ss;
    // ss << pub;
    // elog::debug(ss.str());

    std::lock_guard lock(pub_sub_mutex_);
    // Do not handle connection msg from the same process
    if (!local_subscribers_.hasSubscriber(topic) || remote_proc_uuid == process_uuid_) {
        return;
    }

    if (!connections_.hasPublisher(addr)) {
        elog::debug("Connect to pub.");
        subscriber_->connect(addr.c_str());
    }

    // Add a new subscribe filter for the topic
    subscriber_->set(zmq::sockopt::subscribe, topic);

    connections_.addPublisher(pub);
    elog::debug("Connected to [{}] for data.", addr);

    MessagePublisherInfo pub1(pub);
    pub1.setProcessUuid(process_uuid_);
    pub1.setCtrl(pub.getProcessUuid());

    auto handler_node_uuids = local_subscribers_.getNodeUuids(topic, pub.getMsgType());

    for (const auto& node_uuid : handler_node_uuids) {
        pub1.setNodeUuid(node_uuid);
        msg_discovery_->registerNode(pub1);
    }
}

void NodeShared::onNewDisconnection(const MessagePublisherInfo& pub)
{
    std::string topic = pub.getTopic();
    std::string remote_proc_uuid = pub.getProcessUuid();
    std::string node_uuid = pub.getNodeUuid();

    elog::debug("New disconnection detected, process_uuid: {}", remote_proc_uuid);

    std::lock_guard lock(pub_sub_mutex_);
    // Discovery reports a process-level disconnect without a topic or node UUID.
    // Registration teardown is handled exclusively by onEndRegistration().
    if (topic.empty() && node_uuid.empty()) {
        remote_subscribers_.delPublishersByProc(remote_proc_uuid);
        connections_.delPublishersByProc(remote_proc_uuid);
        return;
    }

    connections_.delPublishersByNode(topic, remote_proc_uuid, node_uuid);
}

bool NodeShared::unsubscribe(const std::string& topic, const std::string& node_uuid)
{
    elog::trace("unsubscribe topic[{}]", topic);

    {
        std::lock_guard lock(pub_sub_mutex_);
        // A queued local message may still invoke its callback after this removal.
        local_subscribers_.removeHandlersForNode(topic, node_uuid);

        if (!local_subscribers_.hasSubscriber(topic)) {
            subscriber_->set(zmq::sockopt::unsubscribe, topic);
        }
    }

    AddressMap<MessagePublisherInfo> addresses;
    msg_discovery_->getPublishers(topic, addresses);

    for (const auto& [publisher_process_uuid, publishers] : addresses) {
        MessagePublisherInfo pub(topic, my_address_, publisher_process_uuid, process_uuid_, node_uuid,
            kGenericMessageType, AdvertiseMessageOptions{});
        msg_discovery_->unRegisterNode(pub);
    }

    return true;
}

void NodeShared::onNewSrvConnection(const ServicePublisherInfo& pub)
{
    std::string topic = pub.getTopic();
    std::string addr = pub.getAddr();
    std::string req_type = pub.getReqTypeName();
    std::string rep_type = pub.getRepTypeName();

    /// todo: use a different mutex
    std::lock_guard lock(service_mutex_);
    elog::debug("Service call connection callback.");

    if (std::find(service_connections_.begin(), service_connections_.end(), addr) == service_connections_.end()) {
        requester_->connect(addr.c_str());
        service_connections_.push_back(addr);
        /// todo: why sleep for 100ms
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        elog::debug("Connect to [{}] for service requests", addr);
    }

    IReqHandlerPtr handler;
    if (request_handlers_.getFirstHandler(topic, req_type, rep_type, handler)) {
        sendPendingRemoteReqs(topic, req_type, rep_type);
    }
}

void NodeShared::onNewSrvDisconnection(const ServicePublisherInfo& pub)
{
    std::string addr = pub.getAddr();

    std::lock_guard lock(service_mutex_);
    service_connections_.erase(std::remove(service_connections_.begin(),
    service_connections_.end(), addr), service_connections_.end());

    elog::debug("Service call disconnection callback");
}

void NodeShared::onNewRegistration(const MessagePublisherInfo& pub)
{
    if (pub.getCtrl() != process_uuid_) {
        return;
    }

    std::string proc_uuid = pub.getProcessUuid();
    std::string node_uuid = pub.getNodeUuid();

    elog::debug("Registering a new remote connection.\n"
        "\t Proc uuid: {}\n\t Node uuid: {}", proc_uuid, node_uuid);

    std::lock_guard lock(pub_sub_mutex_);
    remote_subscribers_.addPublisher(pub);
}

void NodeShared::onEndRegistration(const MessagePublisherInfo& pub)
{
    if (!pub.getCtrl().empty() && pub.getCtrl() != process_uuid_) {
        return;
    }

    std::string topic = pub.getTopic();
    std::string remote_proc_uuid = pub.getProcessUuid();
    std::string node_uuid = pub.getNodeUuid();

    elog::debug("EndRegistering a remote connection.\n"
        "\t Proc uuid: {}\n\t Node uuid: {}", remote_proc_uuid, node_uuid);

    std::lock_guard lock(pub_sub_mutex_);
    remote_subscribers_.delPublishersByNode(topic, remote_proc_uuid, node_uuid);
}

bool NodeShared::publish(const std::string& topic, char* data, const size_t data_size,
    DeallocFunc* ffn, const std::string& msg_type)
{
    // Create the messages.
    // Note that we use zero copy for passing the message data (msg2).
    zmq::message_t msg0(topic.data(), topic.size()),
                   msg1(my_address_.data(), my_address_.size()),
                   msg2(data, data_size, ffn, nullptr),
                   msg3(msg_type.data(), msg_type.size());

    std::lock_guard lock(pub_sub_mutex_);
    try {
        publisher_->send(msg0, zmq::send_flags::sndmore);
        publisher_->send(msg1, zmq::send_flags::sndmore);
        publisher_->send(msg2, zmq::send_flags::sndmore);
        publisher_->send(msg3, zmq::send_flags::none);
    } catch (const zmq::error_t& e) {
        elog::error("NodeShared::publish error: {}", e.what());
        return false;
    }
    return true;
}

void NodeShared::sendPendingRemoteReqs(const std::string& topic,
    const std::string& req_type, const std::string& rep_type)
{
    std::string responder_addr;
    std::string responder_id;

    std::map<std::string, std::vector<ServicePublisherInfo>> addresses;

    srv_discovery_->getPublishers(topic, addresses);
    if (addresses.empty()) {
        elog::warn("No service publishers found in topic[{}]", topic);
        return;
    }

    bool found = false;
    for (auto& [process_uuid, publishers] : addresses) {
        for (auto& pub : publishers) {
            if (pub.getReqTypeName() == req_type && pub.getRepTypeName() == rep_type) {
                found = true;
                responder_addr = pub.getAddr();
                responder_id = pub.getSocketId();
                break;
            }
        }
        if (found) {
            break;
        }
    }

    if (!found) {
        return;
    }

    elog::debug("Found a service call responder at [{}], responder_id[{}]", responder_addr, responder_id);

    std::lock_guard lock(service_mutex_);
    if (std::find(service_connections_.begin(), service_connections_.end(), responder_addr) == service_connections_.end()) {
        requester_->connect(responder_addr.c_str());
        service_connections_.push_back(responder_addr);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        elog::debug("Connected to [{}] for service requests.", responder_addr);
    }

    std::map<std::string, std::map<std::string, IReqHandlerPtr>> req_handlers;
    if (!request_handlers_.getHandlers(topic, req_handlers)) {
        return;
    }

    for (auto& [node_uuid, mp] : req_handlers) {
        for (auto& [req_uuid, req_handler] : mp) {
            if (req_handler->hasRequested()) {
                continue;
            }

            if (req_handler->getReqTypeName() != req_type ||
                req_handler->getRepTypeName() != rep_type) {
                continue;
            }

            std::string data;
            if (!req_handler->serialize(data)) {
                continue;
            }

            try {
                zmq::message_t msg;

                // Send responder id
                msg.rebuild(responder_id.size());
                memcpy(msg.data(), responder_id.data(), responder_id.size());
                requester_->send(msg, zmq::send_flags::sndmore);

                // Send topic
                msg.rebuild(topic.size());
                memcpy(msg.data(), topic.data(), topic.size());
                requester_->send(msg, zmq::send_flags::sndmore);

                // Send requester_addr
                msg.rebuild(my_requester_address_.size());
                memcpy(msg.data(), my_requester_address_.data(), my_requester_address_.size());
                requester_->send(msg, zmq::send_flags::sndmore);

                // Send response receiver uuid
                msg.rebuild(response_receiver_uuid_.size());
                memcpy(msg.data(), response_receiver_uuid_.data(), response_receiver_uuid_.size());
                requester_->send(msg, zmq::send_flags::sndmore);

                // Send node_uuid
                msg.rebuild(node_uuid.size());
                memcpy(msg.data(), node_uuid.data(), node_uuid.size());
                requester_->send(msg, zmq::send_flags::sndmore);

                // Send req_uuid
                msg.rebuild(req_uuid.size());
                memcpy(msg.data(), req_uuid.data(), req_uuid.size());
                requester_->send(msg, zmq::send_flags::sndmore);

                // Send req_data
                msg.rebuild(data.size());
                memcpy(msg.data(), data.data(), data.size());
                requester_->send(msg, zmq::send_flags::sndmore);

                // Send req_type
                msg.rebuild(req_type.size());
                memcpy(msg.data(), req_type.data(), req_type.size());
                requester_->send(msg, zmq::send_flags::sndmore);

                // Send rep_type
                msg.rebuild(rep_type.size());
                memcpy(msg.data(), rep_type.data(), rep_type.size());
                requester_->send(msg, zmq::send_flags::none);

                req_handler->setRequested(true);

            } catch (const zmq::error_t& e) {
                elog::error("NodeShared::sendPendingRemoteReqs request send failed: {}", e.what());
            }
        }
    }
}
bool NodeShared::advertisePublisher(const ServicePublisherInfo& pub)
{
    return srv_discovery_->advertise(pub);
}

bool NodeShared::getServicePublishers(const std::string& topic, AddressMap<ServicePublisherInfo>& publishers) const
{
    return srv_discovery_->getPublishers(topic, publishers);
}

void NodeShared::localPubLoop()
{
    using namespace std::chrono_literals;

    while (!exit_) {
        std::deque<std::unique_ptr<PublishMsgDetails>> msg_details_tmp;

        {
            std::unique_lock lock(local_pub_mutex_);
            if (local_pub_queue_.empty()) {
                local_pub_cv_.wait_for(lock, 500ms,
                    [this] {return !local_pub_queue_.empty() || exit_; });
            }

            if (exit_) {
                return;
            }

            if (local_pub_queue_.empty()) {
                continue;
            }
            msg_details_tmp.swap(local_pub_queue_);
        }

        for (auto& msg_detail : msg_details_tmp) {
            std::shared_ptr<ProtoMsg> shared_msg = std::move(msg_detail->msg_copy);
            for (auto& handler : msg_detail->local_handlers) {
                // For efficiency, do not use try/catch.
                handler->runLocalCallback(shared_msg, msg_detail->info);
            }
        }
    }
}

void NodeShared::receiveMsgLoop()
{
    while (!exit_) {
        zmq::pollitem_t items[] = {
            {*subscriber_, 0, ZMQ_POLLIN, 0},
            {*replier_, 0, ZMQ_POLLIN, 0},
            {*response_receiver_, 0, ZMQ_POLLIN, 0},
        };

        try {
            zmq::poll(&items[0], sizeof(items) / sizeof(items[0]), std::chrono::milliseconds(250));
        } catch (...) {
            continue;
        }

        if (items[0].revents & ZMQ_POLLIN) {
            recvMsgUpdate();
        }
        if (items[1].revents & ZMQ_POLLIN) {
            recvSrvRequest();
        }
        if (items[2].revents & ZMQ_POLLIN) {
            recvSrvResponse();
        }
    }
}

void NodeShared::recvMsgUpdate()
{
    auto remote_msg = std::make_unique<RemoteMsg>();
    zmq::message_t msg(0);
    {
        // protect socket
        std::lock_guard lock(pub_sub_mutex_);
        try {
            if (!subscriber_->recv(msg)) {
                return;
            }
            remote_msg->topic = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

            if (!subscriber_->recv(msg)) {
                return;
            }
            remote_msg->sender = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

            if (!subscriber_->recv(msg)) {
                return;
            }
            // We just move the msg instead of copy
            remote_msg->data = std::move(msg);
            msg.rebuild();

            if (!subscriber_->recv(msg)) {
                return;
            }
            remote_msg->msg_type = std::string(reinterpret_cast<char*>(msg.data()), msg.size());
        } catch (const zmq::error_t& e) {
            elog::error("Subscriber socket receive error: {}", e.what());
            return;
        }
    }

    std::lock_guard lock(remote_msg_mutex_);
    remote_msg_queue_.push_back(std::move(remote_msg));
    if (remote_msg_queue_.size() > kMsgQueueLimit) {
        elog::warn("Remote msg queue size > {}, drop msg of topic [{}]", kMsgQueueLimit, remote_msg_queue_.front()->topic);
        remote_msg_queue_.pop_front();
    }
    remote_msg_cv_.notify_one();
}

void NodeShared::remotePubLoop()
{
    using namespace std::chrono_literals;

    while (!exit_) {
        std::deque<std::unique_ptr<RemoteMsg>> remote_msgs_tmp;
        {
            std::unique_lock lock(remote_msg_mutex_);
            if (remote_msg_queue_.empty()) {
                remote_msg_cv_.wait_for(lock, 500ms,
                    [this] {return !remote_msg_queue_.empty() || exit_; });
            }

            if (exit_) {
                return;
            }
            remote_msgs_tmp.swap(remote_msg_queue_);
        }

        for (auto& msg : remote_msgs_tmp) {
            auto handler_info = checkHandlerInfo(msg->topic);
            if (!handler_info.have_local) {
                continue;
            }
            // We deserialize only once, all local handler shared the msg.
            ProtoMsgPtr proto_msg;
            bool invalid_msg = false;

            for (auto& [node_uuid, mp] : handler_info.local_handlers) {
                for (auto& [handler_uuid, handler] : mp) {
                    if (!handler) {
                        elog::error("Local subscription handler is null");
                        continue;
                    }

                    if (handler->getMsgType() != msg->msg_type) {
                        elog::error("Local subscription handler's msg_type does not match remote msg");
                        continue;
                    }

                    if (!proto_msg) {
                        // Do deserialize
                        proto_msg = handler->createMsg(msg->data.data(), msg->data.size(), msg->msg_type);
                        if (!proto_msg) {
                            elog::error("Failed to deserialize remote message for topic [{}]", msg->topic);
                            invalid_msg = true;
                            break;
                        }
                    }

                    handler->runLocalCallback(proto_msg, MessageInfo(msg->topic, msg->msg_type));
                }
                if (invalid_msg) {
                    break;
                }
            }
        }
    }
}

HandlerInfo NodeShared::checkHandlerInfo(const std::string& topic) const
{
    HandlerInfo info;
    info.have_local = local_subscribers_.normal_.getHandlers(topic, info.local_handlers);

    return info;
}

SubscriberInfo NodeShared::checkSubscriberInfo(const std::string& topic, const std::string& msg_type) const
{
    SubscriberInfo info;
    info.local_handler_info.have_local = local_subscribers_.normal_.getHandlers(topic, info.local_handler_info.local_handlers);
    info.have_remote = remote_subscribers_.hasTopic(topic, msg_type);

    return info;
}

void NodeShared::recvSrvRequest()
{
    elog::debug("Receive service request.");

    zmq::message_t msg(0);
    auto remote_request = std::make_unique<RemoteRequest>();

    {
        std::lock_guard lock(service_mutex_);
        try {
            if (!replier_->recv(msg)) {
                return;
            }

            // Recv topic
            if (!replier_->recv(msg)) {
                return;
            }
            remote_request->topic = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

            // Recv sender
            if (!replier_->recv(msg)) {
                return;
            }
            remote_request->sender = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

            // Recv dst_id
            if (!replier_->recv(msg)) {
                return;
            }
            remote_request->dst_id = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

            // Recv node_uuid
            if (!replier_->recv(msg)) {
                return;
            }
            remote_request->node_uuid = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

            // Recv req_uuid
            if (!replier_->recv(msg)) {
                return;
            }
            remote_request->req_uuid = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

            // Recv req_data
            if (!replier_->recv(msg)) {
                return;
            }
            remote_request->req_data = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

            // Recv req_type
            if (!replier_->recv(msg)) {
                return;
            }
            remote_request->req_type = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

            // Recv rep_type
            if (!replier_->recv(msg)) {
                return;
            }
            remote_request->rep_type = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

        } catch (const zmq::error_t& error) {
            elog::error("Service request read error: {}", error.what());
            return;
        }
    }

    std::lock_guard lk(remote_service_msg_mutex_);
    remote_request_queue_.push_back(std::move(remote_request));
    remote_service_msg_cv_.notify_one();
}

void NodeShared::recvSrvResponse()
{
    elog::debug("Receive remote service response");

    zmq::message_t msg(0);
    auto remote_response = std::make_unique<RemoteResponse>();

    {
        std::lock_guard lock(service_mutex_);
        try {
            if (!response_receiver_->recv(msg)) {
                return;
            }

            // Recv topic
            if (!response_receiver_->recv(msg)) {
                return;
            }
            remote_response->topic = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

            // Recv node_uuid
            if (!response_receiver_->recv(msg)) {
                return;
            }
            remote_response->node_uuid = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

            // Recv req_uuid
            if (!response_receiver_->recv(msg)) {
                return;
            }
            remote_response->req_uuid = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

            // Recv rep_data
            if (!response_receiver_->recv(msg)) {
                return;
            }
            remote_response->rep_data = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

            // Recv result_str
            if (!response_receiver_->recv(msg)) {
                return;
            }
            remote_response->result_str = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

        } catch (const zmq::error_t& e) {
            elog::error("Service response read error: {}", e.what());
            return;
        }
    }

    std::lock_guard lk(remote_service_msg_mutex_);
    remote_response_queue_.push_back(std::move(remote_response));
    remote_service_msg_cv_.notify_one();
}

void NodeShared::serviceHandleLoop()
{
    using namespace std::chrono_literals;

    while (!exit_) {
        std::deque<std::unique_ptr<RemoteRequest>> remote_requests_tmp;
        std::deque<std::unique_ptr<RemoteResponse>> remote_responses_tmp;

        {
            std::unique_lock lock(remote_service_msg_mutex_);
            remote_service_msg_cv_.wait_for(lock, 500ms,
                [this] { return !remote_request_queue_.empty()
                    || !remote_response_queue_.empty() || exit_; });
            if (exit_) {
                return;
            }

            remote_requests_tmp.swap(remote_request_queue_);
            remote_responses_tmp.swap(remote_response_queue_);
        }
        handleRequest(remote_requests_tmp);
        handleResponse(remote_responses_tmp);
    }
}

void NodeShared::handleRequest(const std::deque<std::unique_ptr<RemoteRequest>>& requests)
{
    using namespace std::chrono_literals;

    IRepHandlerPtr rep_handler;
    for (auto& request : requests) {
        bool has_handler = response_handlers_.getFirstHandler(request->topic, request->req_type,
            request->rep_type, rep_handler);

        if (!has_handler) {
            continue;
        }

        std::string rep_data;
        bool result = rep_handler->runCallback(request->req_data, rep_data);

        std::string result_str = result ? "1" : "0";

        {
            std::lock_guard lock(service_mutex_);
            // I am still not connected to this address
            if (std::find(service_connections_.begin(), service_connections_.end(),
                request->sender) == service_connections_.end()) {
                replier_->connect(request->sender.c_str());
                service_connections_.push_back(request->sender);
                std::this_thread::sleep_for(100ms);

                elog::debug("Connected to [{}] for sending a response", request->sender);
            }

            // send the reply
            try {
                zmq::message_t response;

                response.rebuild(request->dst_id.size());
                memcpy(response.data(), request->dst_id.data(), request->dst_id.size());
                replier_->send(response, zmq::send_flags::sndmore);

                response.rebuild(request->topic.size());
                memcpy(response.data(), request->topic.data(), request->topic.size());
                replier_->send(response, zmq::send_flags::sndmore);

                response.rebuild(request->node_uuid.size());
                memcpy(response.data(), request->node_uuid.data(), request->node_uuid.size());
                replier_->send(response, zmq::send_flags::sndmore);

                response.rebuild(request->req_uuid.size());
                memcpy(response.data(), request->req_uuid.data(), request->req_uuid.size());
                replier_->send(response, zmq::send_flags::sndmore);

                response.rebuild(rep_data.size());
                memcpy(response.data(), rep_data.data(), rep_data.size());
                replier_->send(response, zmq::send_flags::sndmore);

                response.rebuild(result_str.size());
                memcpy(response.data(), result_str.data(), result_str.size());
                replier_->send(response, zmq::send_flags::none);

            } catch (const zmq::error_t& e) {
                elog::error("response send error: {}", e.what());
                return;
            }
        }
    }
}

void NodeShared::handleResponse(const std::deque<std::unique_ptr<RemoteResponse>>& responses)
{
    for (auto& response : responses) {
        IReqHandlerPtr req_handler;
        bool has_handler = request_handlers_.getHandler(response->topic, response->node_uuid,
            response->req_uuid, req_handler);

        if (!has_handler) {
            elog::error("Received a service call response, but I don't have a handler for it.");
            continue;
        }

        bool result = (response->result_str == "1");
        req_handler->notifyResult(response->rep_data, result);

        if (!request_handlers_.removeHandler(response->topic, response->node_uuid, response->req_uuid)) {
            elog::error("NodeShared::handleResponse removeHandler error.");
        }
    }
}

} // namespace trans
} // namespace rf
