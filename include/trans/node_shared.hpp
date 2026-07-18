#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "zmq.hpp"

#include "trans/discovery.hpp"
#include "trans/trans_types.hpp"

namespace rf
{
namespace trans
{
class MessageRuntime;
class ServiceRuntime;

class NodeShared
{
public:
    static NodeShared& getInstance();
    NodeShared(const NodeShared&) = delete;
    NodeShared& operator=(const NodeShared&) = delete;

    bool unsubscribe(const std::string& topic, const std::string& node_uuid);
    bool advertiseMessage(const std::string& topic, const std::string& node_uuid,
        const std::string& msg_type, const AdvertiseMessageOptions& options,
        MessagePublisherInfo& publisher);
    bool subscribe(const std::string& topic, const std::string& node_uuid,
        const ISubscriptionHandlerPtr& handler);
    bool advertiseService(const std::string& topic, const std::string& node_uuid,
        const IRepHandlerPtr& handler, const std::string& req_type,
        const std::string& rep_type, const AdvertiseServiceOptions& options);
    bool unadvertiseService(const std::string& topic, const std::string& node_uuid);
    bool hasService(const std::string& topic) const;
    void getAdvertisedTopics(const std::string& node_uuid, std::set<std::string>& topics) const;
    void getMessageTopics(std::vector<std::string>& topics) const;
    void getServiceTopics(std::vector<std::string>& topics) const;
    bool getLocalServiceHandler(const std::string& topic, const std::string& req_type,
        const std::string& rep_type, IRepHandlerPtr& handler) const;
    bool requestRemoteService(const std::string& topic, const std::string& node_uuid,
        const IReqHandlerPtr& handler);
    bool publishMessage(const MessagePublisherInfo& publisher, std::unique_ptr<ProtoMsg> msg);
    bool hasSubscribers(const std::string& topic, const std::string& msg_type) const;

private:
    NodeShared();
    ~NodeShared();

    std::unique_ptr<zmq::context_t> context_;
    std::unique_ptr<MsgDiscovery> msg_discovery_;
    std::unique_ptr<SrvDiscovery> srv_discovery_;
    std::unique_ptr<MessageRuntime> message_runtime_;
    std::unique_ptr<ServiceRuntime> service_runtime_;
    std::string process_uuid_{generateUuidV4()};
};

} // namespace trans
} // namespace rf
