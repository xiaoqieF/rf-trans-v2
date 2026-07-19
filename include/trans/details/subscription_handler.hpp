#pragma once

#include <string>
#include <memory>

#include "elog/elog.h"

#include "trans/details/trans_types.hpp"
#include "trans/details/helpers.hpp"
#include "trans/details/message_info.hpp"

namespace rf
{
namespace trans
{

class ISubscriptionHandler
{
public:
    explicit ISubscriptionHandler(const std::string& node_uuid)
        : handler_uuid_(generateUuidV4()), node_uuid_(node_uuid) {}
    virtual ~ISubscriptionHandler() = default;

    virtual std::string getMsgType() = 0;
    virtual bool runLocalCallback(const ProtoMsgConstPtr& msg, const MessageInfo& info) = 0;
    virtual const ProtoMsgPtr createMsg(const void* data, size_t len, const std::string& type) const = 0;

    std::string getNodeUuid() const { return node_uuid_; }
    std::string getHandlerUuid() const { return handler_uuid_; }

private:
    std::string handler_uuid_;
    std::string node_uuid_;
};

template<typename T>
class SubscriptionHandler : public ISubscriptionHandler
{
public:
    explicit SubscriptionHandler(const std::string node_uuid) : ISubscriptionHandler(node_uuid)
    {
        static_assert(std::is_base_of_v<google::protobuf::Message, T>, "MsgType must be derived from google::protobuf::Message");
    }

    void setCallback(const MsgCallback<T>& cb) { cb_ = cb; }

    std::string getMsgType() override { return T::descriptor()->full_name(); }

    bool runLocalCallback(const ProtoMsgConstPtr& msg, const MessageInfo& info) override;
    const ProtoMsgPtr createMsg(const void* data, size_t len, const std::string& type) const override;

private:
    MsgCallback<T> cb_;
};

template<typename T>
bool SubscriptionHandler<T>::runLocalCallback(const ProtoMsgConstPtr& msg, const MessageInfo& info)
{
    if (!cb_) {
        elog::error("SubscriptionHandler::runLocalCallback error, callback is null");
        return false;
    }

    auto msg_ptr = std::dynamic_pointer_cast<const T>(msg);

    cb_(msg_ptr, info);
    return true;
}

template<typename T>
const ProtoMsgPtr SubscriptionHandler<T>::createMsg(const void* data, size_t len, const std::string&) const
{
    auto msg_ptr = std::make_shared<T>();

    if (!msg_ptr->ParseFromArray(data, len)) {
        elog::error("SubscriptionHandler::createMsg() ParseFromString error");
    }

    return msg_ptr;
}

} // namespace trans
} // namespace rf
