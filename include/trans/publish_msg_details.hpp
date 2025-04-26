#pragma once

#include "trans/subscription_handler.hpp"
#include "trans/trans_types.hpp"
#include "trans/message_info.hpp"

namespace rf
{
namespace trans
{
struct PublishMsgDetails
{
    std::vector<ISubscriptionHandler> local_handlers;
    std::unique_ptr<char[]> shared_buffer{nullptr};
    std::unique_ptr<ProtoMsg> msg_copy{nullptr};
    size_t msg_size{0};
    MessageInfo info;
    std::string publisher_node_uuid;
};

} // namespace trans
} // namespace rf