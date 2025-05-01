#pragma once

#include "elog/elog.h"

#include "trans/helpers.hpp"
#include "trans/trans_types.hpp"

namespace rf
{
namespace trans
{
class IRepHandler
{
public:
    IRepHandler() : handler_uuid_(generateUuidV4()) {}
    virtual ~IRepHandler() = default;

    virtual bool runLocalCallback(const std::shared_ptr<const ProtoMsg> msg_req, std::shared_ptr<ProtoMsg> msg_rep) = 0;
    // serialized input and output
    virtual bool runCallback(const std::string& req, std::string& rep) = 0;

    virtual std::string getReqTypeName() const = 0;
    virtual std::string getRepTypeName() const = 0;

    std::string getHandlerUuid() const { return handler_uuid_; }

protected:
    std::string handler_uuid_;
};

template<typename Req, typename Rep>
class RepHandler : IRepHandler
{
public:
    RepHandler()
    {
        static_assert(std::is_base_of_v<google::protobuf::Message, Req>, "Req must be derived from google::protobuf::Message");
        static_assert(std::is_base_of_v<google::protobuf::Message, Rep>, "Rep must be derived from google::protobuf::Message");
    }

    void setCallback(const std::function<bool(const std::shared_ptr<const Req>, std::shared_ptr<Rep>)> cb) { cb_ = cb; }

    bool runLocalCallback(std::shared_ptr<const ProtoMsg> msg_req, std::shared_ptr<ProtoMsg> msg_rep) override;
    // serialized input and output
    bool runCallback(const std::string& req, std::string& rep) override;

    std::string getReqTypeName() const override;
    std::string getRepTypeName() const override;

private:
    std::shared_ptr<Req> createMsg(const std::string& data) const;

private:
    std::function<bool(const std::shared_ptr<const Req>, std::shared_ptr<Rep>)> cb_;
};

template<typename Req, typename Rep>
bool RepHandler<Req, Rep>::runLocalCallback(const std::shared_ptr<const ProtoMsg> msg_req, std::shared_ptr<ProtoMsg> msg_rep)
{
    if (!cb_) {
        elog::error("RepHandler::runLocalCallback() error: callback is null");
        return false;
    }

    auto msg_req_ptr = std::dynamic_pointer_cast<const Req>(msg_req);
    auto msg_rep_ptr = std::dynamic_pointer_cast<Rep>(msg_rep);

    return cb_(msg_req_ptr, msg_rep_ptr);
}

template<typename Req, typename Rep>
bool RepHandler<Req, Rep>::runCallback(const std::string& req, std::string& rep)
{
    if (!cb_) {
        elog::error("RepHandler::runCallback() error: callback is null");
        return false;
    }

    auto msg_req = createMsg(req);
    if (!msg_req) {
        return false;
    }

    auto msg_rep = std::make_shared<Rep>();
    if (!cb_(msg_req, msg_rep)) {
        elog::warn("ReqHandler::runCallback(), user callback return false.");
        return false;
    }

    if (!msg_rep->SerializeToString(&rep)) {
        elog::error("RepHandler::runCallback() error: response serialize failed.");
        return false;
    }

    return true;
}

template<typename Req, typename Rep>
std::string RepHandler<Req, Rep>::getReqTypeName() const
{
    /// todo: How can I get type name at compile time
    return Req{}.GetTypeName();
}

template<typename Req, typename Rep>
std::string RepHandler<Req, Rep>::getRepTypeName() const
{
    /// todo: How can I get type name at compile time
    return Rep{}.GetTypeName();
}

template<typename Req, typename Rep>
std::shared_ptr<Req> RepHandler<Req, Rep>::createMsg(const std::string& data) const
{
    auto msg_ptr = std::make_shared<Req>();

    if (!msg_ptr->ParseFromString(data)) {
        elog::error("ReqHandler::createMsg() error: PraseFromString error");
    }

    return msg_ptr;
}

} // namespace trans
} // namespace rf