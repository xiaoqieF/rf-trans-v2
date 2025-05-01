
#include <string>
#include <unordered_set>

#include "trans/node.hpp"
#include "trans/publisher.hpp"
#include "trans/node_shared.hpp"

namespace rf
{
namespace trans
{
struct Node::NodePrivate
{
    NodePrivate() : node_uuid(generateUuidV4()), node_shared(NodeShared::getInstance()) {}

    std::string node_uuid;
    NodeShared& node_shared;
    std::unordered_set<std::string> subscribed_topics;
    std::unordered_set<std::string> advertised_srvs;
};

Node::Node() : impl_(new NodePrivate)
{
}

Node::~Node()
{
    auto sub_topics = impl_->subscribed_topics;
    for (const auto& topic : sub_topics) {
        unsubscribe(topic);
    }

    auto adv_srvs = impl_->advertised_srvs;
    for (const auto& service : adv_srvs) {
        if (!unadvertiseService(service)) {
            elog::error("Node::~Node(): Error unadvertising service: {}", service);
        }
    }
}

NodeShared& Node::nodeShared() const
{
    return impl_->node_shared;
}

bool Node::subscribeHelper(const std::string& topic)
{
    impl_->subscribed_topics.insert(topic);

    if (!nodeShared().msg_discovery_->discover(topic)) {
        elog::error("Node::subscribeHelper(): Error discovering topic[{}]", topic);
        return false;
    }

    return true;
}

bool Node::unadvertiseService(const std::string& topic)
{
    impl_->advertised_srvs.erase(topic);
    nodeShared().response_handlers_.removeHandlersForNode(topic, getNodeUuid());

    if (!nodeShared().srv_discovery_->unadvertise(topic, getNodeUuid())) {
        return false;
    }

    return true;
}

/// TODO: move this function to node_shared
bool Node::unsubscribe(const std::string& topic)
{
    /// note that we dont remove msg in local_pub_queue manually.
    /// when unsubscribed called, callback may be called in a short duration
    nodeShared().local_subscribers_.removeHandlersForNode(topic, getNodeUuid());

    impl_->subscribed_topics.erase(topic);

    // If I am the last subscriber, remove the filter for this topic.
    if (!nodeShared().local_subscribers_.hasSubscriber(topic)) {
        std::lock_guard lk(nodeShared().pub_sub_mutex_);
        nodeShared().subscriber_->set(zmq::sockopt::unsubscribe, topic);
    }

    AddressMap<MessagePublisherInfo> addresses;

    nodeShared().msg_discovery_->getPublishers(topic, addresses);

    for (auto& [p_uuid, pubs] : addresses) {
        MessagePublisherInfo pub(topic, nodeShared().my_address_, p_uuid, nodeShared().process_uuid_, getNodeUuid(),
        kGenericMessageType, AdvertiseMessageOptions{});
        NodeShared().msg_discovery_->unRegisterNode(pub);
    }

    return true;
}

std::unordered_set<std::string>& Node::topicsSubscribed()
{
    return impl_->subscribed_topics;
}

std::unordered_set<std::string>& Node::servicesAdvertised()
{
    return impl_->advertised_srvs;
}

std::set<std::string> Node::getAdvertisedTopics()
{
    std::set<std::string> topics;
    std::vector<MessagePublisherInfo> pubs;
    nodeShared().msg_discovery_->getInfo().getPublishersByNode(nodeShared().process_uuid_, impl_->node_uuid, pubs);
    for (const auto& pub : pubs) {
        topics.insert(pub.getTopic());
    }

    return topics;
}

std::vector<std::string> Node::getTopicList() const
{
    std::vector<std::string> topics;
    nodeShared().msg_discovery_->getTopicList(topics);

    return topics;
}

std::vector<std::string> Node::getServiceList() const
{
    std::vector<std::string> topics;
    nodeShared().srv_discovery_->getTopicList(topics);

    return topics;
}

std::string Node::getNodeUuid() const
{
    return impl_->node_uuid;
}

} // namespace trans
} // namespace rf
