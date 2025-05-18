#pragma once

#include <mutex>
#include <condition_variable>

#include "elog/elog.h"

#include "trans/trans_types.hpp"
#include "trans/helpers.hpp"

namespace rf
{
namespace trans
{
class IReqHandler
{
public:
    explicit IReqHandler(const std::string& node_uuid)
        : rep_(""),
          handler_uuid_(generateUuidV4()),
          result_(false),
          requested_(false),
          rep_available_(false),
          node_uuid_(node_uuid)
    {}
    virtual ~IReqHandler() = default;

    virtual void notifyResult(const std::string& rep, const bool result) = 0;
    virtual bool serialize(std::string& buffer) const = 0;
    virtual std::string getReqTypeName() const = 0;
    virtual std::string getRepTypeName() const = 0;

    bool waitFor(const unsigned int timeout) {
        std::unique_lock lock(mutex_);
        return condition_.wait_for(lock, std::chrono::milliseconds(timeout),
        [this] {
            return rep_available_;
        });
    }

    std::string getNodeUuid() const { return node_uuid_; }
    std::string getHandlerUuid() const { return handler_uuid_; }
    std::string getRep() const { return rep_; }
    bool getResult() const { return result_; }
    bool hasRequested() const { return requested_; }
    void setRequested(const bool value) { requested_ = value; }

protected:
    std::condition_variable condition_;
    std::string rep_;
    std::string handler_uuid_;
    bool result_;
    bool requested_;
    bool rep_available_;
    std::mutex mutex_;

private:
    std::string node_uuid_;
};

template<typename Req, typename Rep>
class ReqHandler : public IReqHandler
{
public:
    explicit ReqHandler(const std::string& node_uuid)
        : IReqHandler(node_uuid)
    {
        static_assert(std::is_base_of_v<google::protobuf::Message, Req>, "Req must be derived from google::protobuf::Message");
        static_assert(std::is_base_of_v<google::protobuf::Message, Rep>, "Rep must be derived from google::protobuf::Message");
    }

    std::shared_ptr<Rep> createMsg(const std::string& data) const;
    void setCallback(const std::function<void(const std::shared_ptr<const Rep> rep, const bool result)>& cb) { cb_ = cb; }
    void setMessage(const std::shared_ptr<Req>& req_msg);
    bool serialize(std::string& buffer) const override;
    void notifyResult(const std::string& rep, const bool result) override;
    std::string getReqTypeName() const override;
    std::string getRepTypeName() const override;

private:
    std::shared_ptr<Req> req_msg_;
    std::function<void(const std::shared_ptr<const Rep> rep, const bool result)> cb_;
};

template<typename Req, typename Rep>
std::shared_ptr<Rep> ReqHandler<Req, Rep>::createMsg(const std::string& data) const
{
    auto msg_ptr = std::make_shared<Rep>();
    if (!msg_ptr->ParseFromString(data)) {
        elog::error("ReqHandler::createMsg() error: ParseFromString failed.");
    }

    return msg_ptr;
}

template<typename Req, typename Rep>
void ReqHandler<Req, Rep>::setMessage(const std::shared_ptr<Req>& req_msg)
{
    req_msg_ = req_msg;
}

template<typename Req, typename Rep>
bool ReqHandler<Req, Rep>::serialize(std::string& buffer) const
{
    if (!req_msg_->SerializeToString(&buffer)) {
        elog::error("ReqHandler::serialize() error serializing the request");
        return false;
    }

    return true;
}

template<typename Req, typename Rep>
void ReqHandler<Req, Rep>::notifyResult(const std::string& rep, const bool result)
{
    if (cb_) {
        auto msg = createMsg(rep);
        cb_(msg, result);
    } else {
        rep_ = rep;
        result_ = result;
    }
    std::lock_guard lock(mutex_);
    rep_available_ = true;
    condition_.notify_one();
}

template<typename Req, typename Rep>
std::string ReqHandler<Req, Rep>::getReqTypeName() const
{
    return Req::descriptor()->full_name();
}

template<typename Req, typename Rep>
std::string ReqHandler<Req, Rep>::getRepTypeName() const
{
    return Rep::descriptor()->full_name();
}


} // namespace trans
} // namespace rf