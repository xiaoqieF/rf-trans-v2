#pragma once

#include "trans/trans_types.hpp"
#include "trans/subscription_handler.hpp"
#include "trans/handler_storege.hpp"

namespace rf
{
namespace trans
{
class HandlerWrapper
{
public:
    bool hasSubscriber(const std::string& topic, const std::string& msg_type) const;
    bool hasSubscriber(const std::string& topic) const;

    std::vector<std::string> getNodeUuids(const std::string& topic, const std::string& msg_type) const;
    bool removeHandlersForNode(const std::string& topic, const std::string& node_uuid);

private:
    HandlerStorage<ISubscriptionHandler> normal_;
};

} // namespace trans
} // namespace rf