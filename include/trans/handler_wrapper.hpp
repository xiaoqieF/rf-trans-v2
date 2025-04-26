#pragma once

#include "trans/trans_types.hpp"
#include "trans/subscription_handler.hpp"
#include "trans/handler_storege.hpp"

namespace rf
{
namespace trans
{
class NodeShared;

class HandlerWrapper
{
public:
    bool hasSubscriber(const std::string& topic, const std::string& msg_type) const;
    bool hasSubscriber(const std::string& topic) const;

    std::vector<std::string> getNodeUuids(const std::string& topic, const std::string& msg_type) const;
    bool removeHandlersForNode(const std::string& topic, const std::string& node_uuid);

private:
    HandlerStorage<ISubscriptionHandler> normal_;

    friend NodeShared;
};

struct HandlerInfo
{
    std::map<std::string, std::map<std::string, ISubscriptionHandlerPtr>> local_handlers;
    bool have_local;
};

struct SubscriberInfo
{
    HandlerInfo local_handler_info;
    bool have_remote;
};

} // namespace trans
} // namespace rf