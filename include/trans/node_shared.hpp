#pragma once

#include <string>
#include <thread>

#include "trans/trans_types.hpp"
#include "trans/discovery.hpp"
#include "trans/handler_wrapper.hpp"


namespace rf
{
namespace trans
{
class NodeShared
{
public:

private:
    static constexpr int kDefaultMsgDiscoveryPort = 10317;
    static constexpr int kDefaultSrvDiscoveryPort = 10318;

    std::string response_receiver_id_;
    std::string replier_id_;
    std::string process_id_;

    std::thread receive_msg_thread_;

    const std::string discovery_ip_{"239.255.0.7"};
    int msg_discovery_port_{kDefaultMsgDiscoveryPort};
    int srv_discovery_port_{kDefaultSrvDiscoveryPort};

    TopicStorage<MessagePublisher> connections_;
    TopicStorage<MessagePublisher> remove_subscribers_;
    std::vector<std::string> service_connections_;

    HandlerWrapper local_subscribers_;


};

} // namespace trans
} // namespace rf