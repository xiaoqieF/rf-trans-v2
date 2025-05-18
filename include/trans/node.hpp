#pragma once

#include <vector>
#include <string>

#include "trans/publisher.hpp"
#include "trans/publisher_info.hpp"
#include "trans/trans_types.hpp"
#include "trans/advertise_options.hpp"
#include "trans/subscription_handler.hpp"
#include "trans/req_handler.hpp"
#include "trans/rep_handler.hpp"

#include "msgs/empty.pb.h"

namespace rf
{
namespace trans
{
class Node
{
public:
    Node();
    virtual ~Node();

    template<typename MessageT>
    Publisher advertise(const std::string& topic, const AdvertiseMessageOptions& ops = AdvertiseMessageOptions{});

    template<typename MessageT>
    bool subscribe(const std::string& topic, std::function<void(const std::shared_ptr<const MessageT>&)> callback);

    bool unsubscribe(const std::string& topic);

    // Advertise service with request and reply
    template<typename RequestT, typename ReplyT>
    bool advertise(const std::string& topic,
        std::function<bool(const std::shared_ptr<const RequestT>, std::shared_ptr<ReplyT>)> callback,
        const AdvertiseServiceOptions& ops = AdvertiseServiceOptions{});

    // Advertise service without reply
    template<typename RequestT>
    bool advertise(const std::string& topic, std::function<bool(const std::shared_ptr<const RequestT>)> callback,
        const AdvertiseServiceOptions& ops = AdvertiseServiceOptions{});

    // Advertise service without request
    template<typename ReplyT>
    bool advertise(const std::string& topic, std::function<bool(std::shared_ptr<ReplyT>)> callback,
        const AdvertiseServiceOptions& ops = AdvertiseServiceOptions{});

    template<typename RequestT, typename ReplyT>
    bool request(const std::string& topic, const std::shared_ptr<RequestT>& request,
        std::function<void(const std::shared_ptr<const ReplyT>, const bool)> callback);

    template<typename RequestT, typename ReplyT>
    bool request(const std::string& topic, const std::shared_ptr<RequestT>& request, const unsigned int timeout_ms,
        std::shared_ptr<ReplyT> reply, bool& result);

    template<typename RequestT>
    bool request(const std::string& topic, const std::shared_ptr<RequestT>& request,
        std::function<void(const bool)> callback);

    template<typename RequestT>
    bool request(const std::string& topic, const std::shared_ptr<RequestT>& request,
        const unsigned int timeout_ms, bool& result);

    template<typename ReplyT>
    bool request(const std::string& topic,
        std::function<void(const std::shared_ptr<const ReplyT>, const bool)> callback);

    template<typename ReplyT>
    bool request(const std::string& topic, const unsigned int timeout_ms,
        std::shared_ptr<ReplyT> reply, bool& result);

    bool waitForService(const std::string& topic, std::chrono::duration<int64_t, std::nano> timeout);

    bool unadvertiseService(const std::string& topic);

    std::set<std::string> getAdvertisedTopics();
    std::vector<std::string> getTopicList() const;
    std::vector<std::string> getServiceList() const;

    std::string getNodeUuid() const;

private:
    NodeShared& getNodeShared() const;
    bool subscribeHelper(const std::string& topic);
    std::unordered_set<std::string>& topicsSubscribed();
    std::unordered_set<std::string>& servicesAdvertised();

private:
    struct NodePrivate;
    std::unique_ptr<NodePrivate> impl_;
};

template<typename MessageT>
Publisher Node::advertise(const std::string& topic, const AdvertiseMessageOptions& ops)
{
    static_assert(std::is_base_of_v<google::protobuf::Message, MessageT>,
        "MessageT must be derived from google::protobuf::Message");
    auto msg_type = MessageT::descriptor()->full_name();
    auto current_topics = getAdvertisedTopics();
    if (std::find(current_topics.begin(), current_topics.end(), topic) != current_topics.end()) {
        elog::error("Topic [{}] already advertised. You cannot advertise the same topic on the same node.");
        return Publisher{};
    }

    MessagePublisherInfo pub(topic, getNodeShared().my_address_, "unused",
        getNodeShared().process_uuid_, getNodeUuid(), msg_type, ops);

    if (!getNodeShared().msg_discovery_->advertise(pub)) {
        elog::error("Node::advertise(): Error advertising topic[{}]", topic);
        return Publisher{};
    }

    return Publisher(pub);
}

template<typename MessageT>
bool Node::subscribe(const std::string& topic, std::function<void(const std::shared_ptr<const MessageT>&)> callback)
{
    MsgCallback<MessageT> cb = [callback] (const std::shared_ptr<const MessageT>& msg, const MessageInfo&) {
        callback(msg);
    };

    auto sub_handler = std::make_shared<SubscriptionHandler<MessageT>>(getNodeUuid());
    sub_handler->setCallback(cb);

    getNodeShared().local_subscribers_.normal_.addHandler(topic, getNodeUuid(), sub_handler);

    return subscribeHelper(topic);
}

template<typename RequestT, typename ReplyT>
bool Node::advertise(const std::string& topic,
    std::function<bool(const std::shared_ptr<const RequestT>, std::shared_ptr<ReplyT>)> callback,
    const AdvertiseServiceOptions& ops)
{
    auto rep_handler_ptr = std::make_shared<RepHandler<RequestT, ReplyT>>();
    rep_handler_ptr->setCallback(callback);

    if (servicesAdvertised().count(topic)) {
        elog::error("Node::advertise(): topic[{}] has already advertised by this node.");
        return false;
    }

    servicesAdvertised().insert(topic);

    getNodeShared().response_handlers_.addHandler(topic, getNodeUuid(), rep_handler_ptr);

    ServicePublisherInfo pub(topic, getNodeShared().my_replier_address_, getNodeShared().replier_uuid_,
        getNodeShared().process_uuid_, getNodeUuid(), RequestT::descriptor()->full_name(), ReplyT::descriptor()->full_name(), ops);

    if (!getNodeShared().advertisePublisher(pub)) {
        elog::error("Node::advertise(): Error advertising service [{}]", topic);
        return false;
    }

    return true;
}

// Advertise service without reply
template<typename RequestT>
bool Node::advertise(const std::string& topic, std::function<bool(const std::shared_ptr<const RequestT>)> callback,
    const AdvertiseServiceOptions& ops)
{
    auto f = [callback] (const std::shared_ptr<const RequestT>& req, std::shared_ptr<rf::msgs::Empty>) {
        return callback(req);
    };
    return advertise<RequestT, rf::msgs::Empty>(topic, f, ops);
}

// Advertise service without request
template<typename ReplyT>
bool Node::advertise(const std::string& topic, std::function<bool(std::shared_ptr<ReplyT>)> callback,
    const AdvertiseServiceOptions& ops)
{
    auto f = [callback] (const std::shared_ptr<const rf::msgs::Empty>&, std::shared_ptr<ReplyT> rep) {
        return callback(rep);
    };
    return advertise<rf::msgs::Empty, ReplyT>(topic, f, ops);
}

template<typename RequestT, typename ReplyT>
bool Node::request(const std::string& topic, const std::shared_ptr<RequestT>& req,
    std::function<void(const std::shared_ptr<const ReplyT>, const bool)> callback)
{
    // First, find a local responser
    IRepHandlerPtr rep_handler;
    bool local_responser_found = getNodeShared().response_handlers_.getFirstHandler(
        topic, RequestT::descriptor()->full_name(), ReplyT::descriptor()->full_name(), rep_handler
    );

    auto msg_rep = std::make_shared<ReplyT>();
    if (local_responser_found) {
        bool result = rep_handler->runLocalCallback(req, msg_rep);

        callback(msg_rep, result);
        return true;
    }

    AddressMap<ServicePublisherInfo> remote_publishers;
    if (getNodeShared().getServicePublishers(topic, remote_publishers)) {
        // No local responser, find remote one
        auto req_handler_ptr = std::make_shared<ReqHandler<RequestT, ReplyT>>(getNodeUuid());

        req_handler_ptr->setMessage(req);
        req_handler_ptr->setCallback(callback);

        getNodeShared().request_handlers_.addHandler(topic, getNodeUuid(), req_handler_ptr);
        getNodeShared().sendPendingRemoteReqs(topic, RequestT::descriptor()->full_name(), ReplyT::descriptor()->full_name());
    } else {
        elog::warn("No service server available, topic[{}]", topic);
        return false;
    }
    return true;
}

template<typename RequestT, typename ReplyT>
bool Node::request(const std::string& topic, const std::shared_ptr<RequestT>& req,
    const unsigned int timeout_ms, std::shared_ptr<ReplyT> reply, bool& result)
{
    // First, find a local responser
    IRepHandlerPtr rep_handler;
    bool local_responser_found = getNodeShared().response_handlers_.getFirstHandler(
        topic, RequestT::descriptor()->full_name(), ReplyT::descriptor()->full_name(), rep_handler
    );

    auto msg_rep = std::make_shared<ReplyT>();
    if (local_responser_found) {
        result = rep_handler->runLocalCallback(req, msg_rep);
        return true;
    }

    auto req_handler_ptr = std::make_shared<ReqHandler<RequestT, ReplyT>>(getNodeUuid());
    req_handler_ptr->setMessage(req);

    AddressMap<ServicePublisherInfo> remote_publishers;
    if (getNodeShared().getServicePublishers(topic, remote_publishers)) {
        // No local responser, find remote one
        getNodeShared().request_handlers_.addHandler(topic, getNodeUuid(), req_handler_ptr);
        getNodeShared().sendPendingRemoteReqs(topic, RequestT::descriptor()->full_name(), ReplyT::descriptor()->full_name());
    } else {
        elog::warn("No service server available, topic[{}]", topic);
        result = false;
        return false;
    }

    bool executed = req_handler_ptr->waitFor(timeout_ms);
    if (!executed) {
        return false;
    }

    if (!req_handler_ptr->getResult()) {
        result = false;
        return true;
    }

    if (!reply->ParseFromString(req_handler_ptr->getRep())) {
        elog::error("Node::requset(): Error parsing the response");
        result = false;
        return true;
    }

    result = true;
    return true;
}

template<typename RequestT>
bool Node::request(const std::string& topic, const std::shared_ptr<RequestT>& req,
    std::function<void(const bool)> callback)
{
    auto f = [callback] (const std::shared_ptr<const rf::msgs::Empty>&, const bool result) {
        callback(result);
    };

    return request<RequestT, rf::msgs::Empty>(topic, req, f);
}

template<typename RequestT>
bool Node::request(const std::string& topic, const std::shared_ptr<RequestT>& req,
    const unsigned int timeout, bool& result)
{
    auto rep = std::make_shared<rf::msgs::Empty>();
    return request<RequestT, rf::msgs::Empty>(topic, req, timeout, rep, result);
}

template<typename ReplyT>
bool Node::request(const std::string& topic,
    std::function<void(const std::shared_ptr<const ReplyT>, const bool)> callback)
{
    auto req = std::make_shared<rf::msgs::Empty>();
    return request<rf::msgs::Empty, ReplyT>(topic, req, callback);
}

template<typename ReplyT>
bool Node::request(const std::string& topic, const unsigned int timeout,
    std::shared_ptr<ReplyT> reply, bool& result)
{
    auto req = std::make_shared<rf::msgs::Empty>();
    return request<rf::msgs::Empty, ReplyT>(topic, req, timeout, reply, result);
}

} // namespace trans
} // namespace rf
