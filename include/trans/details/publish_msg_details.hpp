#pragma once

#include "trans/details/subscription_handler.hpp"
#include "trans/details/trans_types.hpp"
#include "trans/details/message_info.hpp"

namespace rf
{
namespace trans
{
struct PublishMsgDetails
{
    std::vector<ISubscriptionHandlerPtr> local_handlers;
    std::unique_ptr<char[]> shared_buffer{nullptr};
    std::unique_ptr<ProtoMsg> msg_copy{nullptr};
    size_t msg_size{0};
    MessageInfo info;
    std::string publisher_node_uuid;
};

} // namespace trans
} // namespace rf
