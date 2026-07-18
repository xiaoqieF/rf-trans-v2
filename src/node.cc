
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

NodeShared& Node::getNodeShared() const
{
    return impl_->node_shared;
}

bool Node::unadvertiseService(const std::string& topic)
{
    impl_->advertised_srvs.erase(topic);
    return getNodeShared().unadvertiseService(topic, getNodeUuid());
}

bool Node::waitForService(const std::string& topic, std::chrono::duration<int64_t, std::nano> timeout)
{
    using namespace std::chrono_literals;
    auto begin = std::chrono::steady_clock::now();

    while (true) {
        if (getNodeShared().hasService(topic)) {
            return true;
        }
        std::this_thread::sleep_for(20ms);
        auto now = std::chrono::steady_clock::now();
        if (now - begin > timeout) {
            break;
        }
    }
    return false;
}

bool Node::unsubscribe(const std::string& topic)
{
    if (!getNodeShared().unsubscribe(topic, getNodeUuid())) {
        return false;
    }

    impl_->subscribed_topics.erase(topic);
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
    getNodeShared().getAdvertisedTopics(impl_->node_uuid, topics);
    return topics;
}

std::vector<std::string> Node::getTopicList() const
{
    std::vector<std::string> topics;
    getNodeShared().getMessageTopics(topics);

    return topics;
}

std::vector<std::string> Node::getServiceList() const
{
    std::vector<std::string> topics;
    getNodeShared().getServiceTopics(topics);

    return topics;
}

std::string Node::getNodeUuid() const
{
    return impl_->node_uuid;
}

} // namespace trans
} // namespace rf
