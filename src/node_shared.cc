#include "trans/details/node_shared.hpp"

#include "trans/details/discovery.hpp"
#include "trans/details/message_runtime.hpp"
#include "trans/details/service_runtime.hpp"

namespace rf
{
namespace trans
{
NodeShared& NodeShared::getInstance()
{
    static NodeShared instance;
    return instance;
}

NodeShared::NodeShared() : context_(std::make_unique<zmq::context_t>(1))
{
    constexpr int kMessageDiscoveryPort = 10319;
    constexpr int kServiceDiscoveryPort = 10320;
    const std::string discovery_ip{"239.255.0.7"};

    msg_discovery_ = std::make_unique<MsgDiscovery>(process_uuid_, discovery_ip, kMessageDiscoveryPort);
    srv_discovery_ = std::make_unique<SrvDiscovery>(process_uuid_, discovery_ip, kServiceDiscoveryPort);
    message_runtime_ = std::make_unique<MessageRuntime>(*context_, *msg_discovery_, process_uuid_);

    if (!message_runtime_->initialize()) {
        return;
    }
    service_runtime_ = std::make_unique<ServiceRuntime>(*context_, *srv_discovery_, process_uuid_,
        message_runtime_->hostAddress());
    if (!service_runtime_->initialize()) {
        return;
    }

    message_runtime_->start();
    service_runtime_->start();
}

NodeShared::~NodeShared()
{
    // Stop discovery first so no callback can race a runtime's teardown.
    if (srv_discovery_) srv_discovery_->stop();
    if (msg_discovery_) msg_discovery_->stop();
    if (service_runtime_) service_runtime_->stop();
    if (message_runtime_) message_runtime_->stop();
}

bool NodeShared::unsubscribe(const std::string& topic, const std::string& node_uuid)
{
    return message_runtime_->unsubscribe(topic, node_uuid);
}

bool NodeShared::unadvertiseMessage(const std::string& topic, const std::string& node_uuid)
{
    return message_runtime_->unadvertise(topic, node_uuid);
}

bool NodeShared::advertiseMessage(const std::string& topic, const std::string& node_uuid,
    const std::string& msg_type, const AdvertiseMessageOptions& options, MessagePublisherInfo& publisher)
{
    return message_runtime_->advertise(topic, node_uuid, msg_type, options, publisher);
}

bool NodeShared::subscribe(const std::string& topic, const std::string& node_uuid, const ISubscriptionHandlerPtr& handler)
{
    return message_runtime_->subscribe(topic, node_uuid, handler);
}

bool NodeShared::advertiseService(const std::string& topic, const std::string& node_uuid,
    const IRepHandlerPtr& handler, const std::string& req_type, const std::string& rep_type,
    const AdvertiseServiceOptions& options)
{
    return service_runtime_->advertise(topic, node_uuid, handler, req_type, rep_type, options);
}

bool NodeShared::unadvertiseService(const std::string& topic, const std::string& node_uuid)
{
    return service_runtime_->unadvertise(topic, node_uuid);
}

bool NodeShared::hasService(const std::string& topic) const
{
    return service_runtime_->hasService(topic);
}

void NodeShared::getAdvertisedTopics(const std::string& node_uuid, std::set<std::string>& topics) const
{
    message_runtime_->getAdvertisedTopics(node_uuid, topics);
}

void NodeShared::getMessageTopics(std::vector<std::string>& topics) const
{
    message_runtime_->getTopics(topics);
}

void NodeShared::getServiceTopics(std::vector<std::string>& topics) const
{
    service_runtime_->getTopics(topics);
}

bool NodeShared::getLocalServiceHandler(const std::string& topic, const std::string& req_type,
    const std::string& rep_type, IRepHandlerPtr& handler) const
{
    return service_runtime_->getLocalHandler(topic, req_type, rep_type, handler);
}

bool NodeShared::requestRemoteService(const std::string& topic, const std::string& node_uuid,
    const IReqHandlerPtr& handler)
{
    return service_runtime_->request(topic, node_uuid, handler);
}

bool NodeShared::publishMessage(const MessagePublisherInfo& publisher, std::unique_ptr<ProtoMsg> msg)
{
    return message_runtime_->publish(publisher, std::move(msg));
}

bool NodeShared::hasSubscribers(const std::string& topic, const std::string& msg_type) const
{
    return message_runtime_->hasSubscribers(topic, msg_type);
}

} // namespace trans
} // namespace rf
