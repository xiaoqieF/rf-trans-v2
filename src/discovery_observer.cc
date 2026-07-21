#include "trans/discovery_observer.hpp"

#include <utility>

#include "trans/details/discovery.hpp"
#include "trans/details/helpers.hpp"

namespace rf
{
namespace trans
{
namespace
{
std::vector<MessageEndpointInfo> snapshotMessageEndpoints(const MsgDiscovery& discovery)
{
    std::vector<MessageEndpointInfo> result;
    for (const auto& topic : discovery.getInfo().getTopicList()) {
        AddressMap<MessagePublisherInfo> publishers;
        if (!discovery.getPublishers(topic, publishers)) {
            continue;
        }
        for (const auto& process : publishers) {
            for (const auto& publisher : process.second) {
                const auto& options = publisher.getOptions();
                result.push_back({publisher.getTopic(), publisher.getAddr(), publisher.getCtrl(),
                    publisher.getProcessUuid(), publisher.getProcessName(), publisher.getNodeUuid(), publisher.getMsgType(),
                    options.getScope(), options.throttled(),
                    options.throttled() ? options.getMsgsPerSec() : 0});
            }
        }
    }
    return result;
}

std::vector<ServiceEndpointInfo> snapshotServiceEndpoints(const SrvDiscovery& discovery)
{
    std::vector<ServiceEndpointInfo> result;
    for (const auto& topic : discovery.getInfo().getTopicList()) {
        AddressMap<ServicePublisherInfo> publishers;
        if (!discovery.getPublishers(topic, publishers)) {
            continue;
        }
        for (const auto& process : publishers) {
            for (const auto& publisher : process.second) {
                result.push_back({publisher.getTopic(), publisher.getAddr(), publisher.getSocketId(),
                    publisher.getProcessUuid(), publisher.getProcessName(), publisher.getNodeUuid(), publisher.getReqTypeName(),
                    publisher.getRepTypeName(), publisher.getOptions().getScope()});
            }
        }
    }
    return result;
}
} // namespace

class DiscoveryObserver::Impl
{
public:
    explicit Impl(const DiscoveryObserverOptions& options)
        : message_discovery(generateUuidV4(), options.multicast_group, options.message_port, options.host_address),
          service_discovery(generateUuidV4(), options.multicast_group, options.service_port, options.host_address)
    {
        message_discovery.start();
        service_discovery.start();
    }

    MsgDiscovery message_discovery;
    SrvDiscovery service_discovery;
};

DiscoveryObserver::DiscoveryObserver(const DiscoveryObserverOptions& options)
    : impl_(std::make_unique<Impl>(options))
{
}

DiscoveryObserver::~DiscoveryObserver() = default;
DiscoveryObserver::DiscoveryObserver(DiscoveryObserver&&) noexcept = default;
DiscoveryObserver& DiscoveryObserver::operator=(DiscoveryObserver&&) noexcept = default;

std::vector<MessageEndpointInfo> DiscoveryObserver::messageEndpoints() const
{
    return snapshotMessageEndpoints(impl_->message_discovery);
}

std::vector<ServiceEndpointInfo> DiscoveryObserver::serviceEndpoints() const
{
    return snapshotServiceEndpoints(impl_->service_discovery);
}

std::string DiscoveryObserver::hostAddress() const
{
    return impl_->message_discovery.getHostAddr();
}

} // namespace trans
} // namespace rf
