#include "trans/details/service_runtime.hpp"

namespace rf
{
namespace trans
{
namespace
{
void sendFrame(zmq::socket_t& socket, const std::string& value, const zmq::send_flags flags)
{
    socket.send(zmq::buffer(value), flags);
}

bool receiveFrame(zmq::socket_t& socket, std::string& value)
{
    zmq::message_t msg;
    if (!socket.recv(msg)) {
        return false;
    }
    value.assign(static_cast<const char*>(msg.data()), msg.size());
    return true;
}
} // namespace

ServiceRuntime::ServiceRuntime(zmq::context_t& context, SrvDiscovery& discovery, std::string process_uuid,
    std::string host_address)
    : discovery_(discovery), process_uuid_(std::move(process_uuid)), host_address_(std::move(host_address)),
      requester_(std::make_unique<zmq::socket_t>(context, ZMQ_ROUTER)),
      replier_(std::make_unique<zmq::socket_t>(context, ZMQ_ROUTER))
{
}

ServiceRuntime::~ServiceRuntime()
{
    stop();
}

bool ServiceRuntime::initialize()
{
    if (initialized_) return true;
    try {
        const std::string endpoint = "tcp://" + host_address_ + ":*";
        const int linger = 0;
        const int mandatory = 1;
        replier_->set(zmq::sockopt::routing_id, replier_uuid_);
        replier_->set(zmq::sockopt::linger, linger);
        replier_->set(zmq::sockopt::router_mandatory, mandatory);
        replier_->bind(endpoint);
        replier_address_ = replier_->get(zmq::sockopt::last_endpoint);
        requester_->set(zmq::sockopt::linger, linger);
        requester_->set(zmq::sockopt::router_mandatory, mandatory);

    } catch (const zmq::error_t& e) {
        elog::error("ServiceRuntime initialization error: {}", e.what());
        return false;
    }
    initialized_ = true;
    return true;
}

void ServiceRuntime::start()
{
    if (!initialized_ || started_) {
        return;
    }
    discovery_.setConnectionCb([this](const ServicePublisherInfo& pub) { onServiceConnected(pub); });
    discovery_.setDisconnectionCb([this](const ServicePublisherInfo& pub) { onServiceDisconnected(pub); });

    receive_thread_ = std::thread(&ServiceRuntime::receiveLoop, this);
    pthread_setname_np(receive_thread_.native_handle(), "rf_srv_recv");

    discovery_.start();

    handle_thread_ = std::thread(&ServiceRuntime::serviceHandleLoop, this);
    pthread_setname_np(handle_thread_.native_handle(), "rf_srv_loop");
    started_ = true;
}

void ServiceRuntime::stop()
{
    if (!started_) return;
    exit_ = true;
    queue_cv_.notify_all();
    if (receive_thread_.joinable()) receive_thread_.join();
    if (handle_thread_.joinable()) handle_thread_.join();
    discovery_.setConnectionCb(nullptr);
    discovery_.setDisconnectionCb(nullptr);
    started_ = false;
}

void ServiceRuntime::onServiceConnected(const ServicePublisherInfo& pub)
{
    const auto& addr = pub.getAddr();
    std::lock_guard lock(mutex_);
    if (!requester_connections_.count(addr)) {
        requester_->connect(addr);
        requester_connections_.insert(addr);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    IReqHandlerPtr handler;
    if (request_handlers_.getFirstHandler(pub.getTopic(), pub.getReqTypeName(), pub.getRepTypeName(), handler)) {
        sendPendingRequests(pub.getTopic(), pub.getReqTypeName(), pub.getRepTypeName());
    }
}

void ServiceRuntime::onServiceDisconnected(const ServicePublisherInfo& pub)
{
    std::lock_guard lock(mutex_);
    requester_connections_.erase(pub.getAddr());
}

bool ServiceRuntime::advertise(const std::string& topic, const std::string& node_uuid,
    const IRepHandlerPtr& handler, const std::string& req_type, const std::string& rep_type,
    const AdvertiseServiceOptions& options)
{
    response_handlers_.addHandler(topic, node_uuid, handler);
    return discovery_.advertise(ServicePublisherInfo(topic, replier_address_, replier_uuid_, process_uuid_, node_uuid,
        req_type, rep_type, options));
}

bool ServiceRuntime::unadvertise(const std::string& topic, const std::string& node_uuid)
{
    response_handlers_.removeHandlersForNode(topic, node_uuid);
    return discovery_.unadvertise(topic, node_uuid);
}

bool ServiceRuntime::hasService(const std::string& topic) const
{
    AddressMap<ServicePublisherInfo> publishers;
    return response_handlers_.hasTopic(topic) || getPublishers(topic, publishers);
}

void ServiceRuntime::getTopics(std::vector<std::string>& topics) const { discovery_.getTopicList(topics); }

bool ServiceRuntime::getLocalHandler(const std::string& topic, const std::string& req_type,
    const std::string& rep_type, IRepHandlerPtr& handler) const
{
    return response_handlers_.getFirstHandler(topic, req_type, rep_type, handler);
}

bool ServiceRuntime::request(const std::string& topic, const std::string& node_uuid, const IReqHandlerPtr& handler)
{
    AddressMap<ServicePublisherInfo> publishers;
    if (!getPublishers(topic, publishers)) return false;
    request_handlers_.addHandler(topic, node_uuid, handler);
    sendPendingRequests(topic, handler->getReqTypeName(), handler->getRepTypeName());
    return true;
}

bool ServiceRuntime::getPublishers(const std::string& topic, AddressMap<ServicePublisherInfo>& publishers) const
{
    return discovery_.getPublishers(topic, publishers);
}

void ServiceRuntime::sendPendingRequests(const std::string& topic, const std::string& req_type,
    const std::string& rep_type)
{
    AddressMap<ServicePublisherInfo> addresses;
    discovery_.getPublishers(topic, addresses);
    ServicePublisherInfo responder;
    bool found = false;
    for (const auto& [process, publishers] : addresses) {
        for (const auto& publisher : publishers) {
            if (publisher.getReqTypeName() == req_type && publisher.getRepTypeName() == rep_type) {
                responder = publisher;
                found = true;
                break;
            }
        }
        if (found) break;
    }
    if (!found) return;

    std::lock_guard lock(mutex_);
    if (!requester_connections_.count(responder.getAddr())) {
        requester_->connect(responder.getAddr());
        requester_connections_.insert(responder.getAddr());
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::map<std::string, std::map<std::string, IReqHandlerPtr>> handlers;
    if (!request_handlers_.getHandlers(topic, handlers)) return;
    for (const auto& [node_uuid, node_handlers] : handlers) for (const auto& [request_uuid, handler] : node_handlers) {
        if (handler->hasRequested() || handler->getReqTypeName() != req_type || handler->getRepTypeName() != rep_type) continue;
        std::string data;
        if (!handler->serialize(data)) continue;
        try {
            sendFrame(*requester_, responder.getSocketId(), zmq::send_flags::sndmore);
            sendFrame(*requester_, topic, zmq::send_flags::sndmore);
            sendFrame(*requester_, node_uuid, zmq::send_flags::sndmore);
            sendFrame(*requester_, request_uuid, zmq::send_flags::sndmore);
            sendFrame(*requester_, data, zmq::send_flags::sndmore);
            sendFrame(*requester_, req_type, zmq::send_flags::sndmore);
            sendFrame(*requester_, rep_type, zmq::send_flags::none);
            handler->setRequested(true);
        } catch (const zmq::error_t& e) { elog::error("Service request send failed: {}", e.what()); }
    }
}

void ServiceRuntime::receiveLoop()
{
    while (!exit_) {
        zmq::pollitem_t items[] = {
            {*replier_, 0, ZMQ_POLLIN, 0},
            {*requester_, 0, ZMQ_POLLIN, 0}
        };
        try {
            zmq::poll(items, 2, std::chrono::milliseconds(250));
        } catch (...) {
            continue;
        }
        if (items[0].revents & ZMQ_POLLIN) {
            receiveRequest();
        }
        if (items[1].revents & ZMQ_POLLIN) {
            receiveResponse();
        }
    }
}

void ServiceRuntime::receiveRequest()
{
    auto request = std::make_unique<RemoteRequest>();
    std::lock_guard lock(mutex_);
    try {
        if (!receiveFrame(*replier_, request->reply_routing_id) || !receiveFrame(*replier_, request->topic) ||
            !receiveFrame(*replier_, request->node_uuid) || !receiveFrame(*replier_, request->req_uuid) ||
            !receiveFrame(*replier_, request->req_data) || !receiveFrame(*replier_, request->req_type) ||
            !receiveFrame(*replier_, request->rep_type)) {
                return;
        }
    } catch (const zmq::error_t& e) {
        elog::error("Service request read error: {}", e.what());
        return;
    }

    std::lock_guard queue_lock(queue_mutex_);
    request_queue_.push_back(std::move(request));
    queue_cv_.notify_one();
}

void ServiceRuntime::receiveResponse()
{
    auto response = std::make_unique<RemoteResponse>();
    std::string ignored;
    std::lock_guard lock(mutex_);
    try {
        if (!receiveFrame(*requester_, ignored) || !receiveFrame(*requester_, response->topic) ||
            !receiveFrame(*requester_, response->node_uuid) || !receiveFrame(*requester_, response->req_uuid) ||
            !receiveFrame(*requester_, response->rep_data) || !receiveFrame(*requester_, response->result_str)) return;
    } catch (const zmq::error_t& e) {
        elog::error("Service response read error: {}", e.what());
        return;
    }
    std::lock_guard queue_lock(queue_mutex_);
    response_queue_.push_back(std::move(response));
    queue_cv_.notify_one();
}

void ServiceRuntime::serviceHandleLoop()
{
    using namespace std::chrono_literals;
    while (!exit_) {
        std::deque<std::unique_ptr<RemoteRequest>> requests;
        std::deque<std::unique_ptr<RemoteResponse>> responses;
        {
            std::unique_lock lock(queue_mutex_);
            queue_cv_.wait_for(lock, 500ms, [this] {
                return !request_queue_.empty() || !response_queue_.empty() || exit_;
            });
            if (exit_) {
                return;
            }
            requests.swap(request_queue_);
            responses.swap(response_queue_);
        }
        handleRequests(requests);
        handleResponses(responses);
    }
}

void ServiceRuntime::handleRequests(const std::deque<std::unique_ptr<RemoteRequest>>& requests)
{
    for (const auto& request : requests) {
        IRepHandlerPtr handler;
        if (!response_handlers_.getFirstHandler(request->topic, request->req_type, request->rep_type, handler)) continue;
        std::string reply_data;
        const std::string result = handler->runCallback(request->req_data, reply_data) ? "1" : "0";

        std::lock_guard lock(mutex_);
        try {
            sendFrame(*replier_, request->reply_routing_id, zmq::send_flags::sndmore);
            sendFrame(*replier_, request->topic, zmq::send_flags::sndmore);
            sendFrame(*replier_, request->node_uuid, zmq::send_flags::sndmore);
            sendFrame(*replier_, request->req_uuid, zmq::send_flags::sndmore);
            sendFrame(*replier_, reply_data, zmq::send_flags::sndmore);
            sendFrame(*replier_, result, zmq::send_flags::none);
        } catch (const zmq::error_t& e) {
            elog::error("Service response send error: {}", e.what());
        }
    }
}

void ServiceRuntime::handleResponses(const std::deque<std::unique_ptr<RemoteResponse>>& responses)
{
    for (const auto& response : responses) {
        IReqHandlerPtr handler;
        if (!request_handlers_.getHandler(response->topic, response->node_uuid, response->req_uuid, handler)) {
            elog::error("Received a service response without a matching handler."); continue;
        }
        handler->notifyResult(response->rep_data, response->result_str == "1");
        if (!request_handlers_.removeHandler(response->topic, response->node_uuid, response->req_uuid)) {
            elog::error("Service response handler removal failed.");
        }
    }
}

} // namespace trans
} // namespace rf
