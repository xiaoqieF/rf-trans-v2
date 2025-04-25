#pragma once

#include "trans/handler_wrapper.hpp"

namespace rf
{
namespace trans
{
bool HandlerWrapper::hasSubscriber(const std::string& topic, const std::string& msg_type) const
{
    std::shared_ptr<ISubscriptionHandler> normal_subscriber_ptr;
    return normal_.getFirstHandler(topic, msg_type, normal_subscriber_ptr);
}

bool HandlerWrapper::hasSubscriber(const std::string& topic) const
{
    return normal_.hasHandlersForTopic(topic);
}

std::vector<std::string> HandlerWrapper::getNodeUuids(const std::string& topic, const std::string& msg_type) const
{
    std::vector<std::string> node_uuids;
    using HandlerPtr = std::shared_ptr<ISubscriptionHandler>;
    std::map<std::string, std::map<std::string, HandlerPtr>> handlers;

    normal_.getHandlers(topic, handlers);

    for (const auto& [node_uuid, node_map] : handlers) {
        for (const auto& [handler_uuid, handler] : node_map) {
            if (handler->getMsgType() == msg_type || handler->getMsgType() == kGenericMessageType) {
                node_uuids.push_back(handler->getNodeUuid());
            }
        }
    }

    return node_uuids;
}

bool HandlerWrapper::removeHandlersForNode(const std::string& topic, const std::string& node_uuid)
{
    return normal_.removeHandlersForNode(topic, node_uuid);
}

} // namespace trans
} // namespace rf