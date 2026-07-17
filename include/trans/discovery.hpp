#pragma once

#include <array>
#include <cerrno>
#include <cstring>
#include <string>
#include <vector>
#include <limits>
#include <sstream>
#include <stdexcept>

#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>

#include <zmq.hpp>

#include "msgs/discovery.pb.h"
#include "trans/msgs.hpp"
#include "trans/net_utils.hpp"
#include "trans/trans_types.hpp"
#include "trans/publisher_info.hpp"
#include "trans/topic_storage.hpp"

namespace rf
{
namespace trans
{
template<typename Pub>
class Discovery
{
public:
    Discovery(const std::string& process_uuid,
              const std::string& ip,
              const int port);
    virtual ~Discovery();

    void start();

    bool advertise(const Pub& publisher);
    bool unadvertise(const std::string& topic, const std::string& node_uuid);

    // Try to discover [topic] immediately, which sends a SUBSCRIBE message about the topic.
    bool discover(const std::string& topic) const;
    void registerNode(const MessagePublisherInfo& pub) const;
    void unRegisterNode(const MessagePublisherInfo& pub) const;

    const TopicStorage<Pub>& getInfo() const;
    bool getPublishers(const std::string& topic, AddressMap<Pub>& publishers) const;

    void getTopicList(std::vector<std::string>& topics) const;
    std::string getHostAddr() const;
    unsigned int getActivityInterval() const;
    void setActivityInterval(const unsigned int ms);
    unsigned int getHeartbeatInterval() const;
    void setHeartbeatInterval(const unsigned int ms);
    unsigned int getSilenceInterval() const;
    void setSilenceInterval(const unsigned int ms);

    void setConnectionCb(const DiscoveryCallback<Pub>& cb);
    void setDisconnectionCb(const DiscoveryCallback<Pub>& cb);
    void setRegistrationCb(const DiscoveryCallback<Pub>& cb);
    void setUnregistrationCb(const DiscoveryCallback<Pub>& cb);

    [[deprecated("Use setRegistrationCb instead.")]]
    void setRegisterationCb(const DiscoveryCallback<Pub>& cb) { setRegistrationCb(cb); }
    [[deprecated("Use setUnregistrationCb instead.")]]
    void setUnregisterationCb(const DiscoveryCallback<Pub>& cb) { setUnregistrationCb(cb); }

    void printCurrentState() const;

private:
    void loop();
    bool registerNetIface(const std::string& ip);

    template<typename T>
    void sendMsg(const msgs::Discovery::Type type, const T& pub) const;

    void sendMulticast(const msgs::Discovery& msg) const;
    int nextTimeout() const;
    void recvDiscoveryMsg();
    void dispatchDiscoveryMsg(const std::string& from_ip, char* msg, uint16_t len);

    void updateHeartbeat();
    void updateActivity();

private:
    // ms
    static constexpr unsigned int kDefActivityInterval = 200;
    static constexpr unsigned int kDefHeartbeatInterval = 1000;
    static constexpr unsigned int kDefSilenceInterval = 3000;
    static constexpr int kTimeout = 250;
    static constexpr uint8_t kWireVersion = 0;
    static constexpr uint16_t kMaxRcvStr = std::numeric_limits<uint16_t>::max();

    std::string multicast_group_;
    int port_;
    sockaddr_in multicast_addr_;

    std::string process_uuid_;
    std::string host_addr_;
    std::vector<int> multicast_sockets_;
    std::array<char, kMaxRcvStr> recv_buf_{};
    std::atomic<unsigned int> silence_interval_{kDefSilenceInterval};
    std::atomic<unsigned int> activity_interval_{kDefActivityInterval};
    std::atomic<unsigned int> heartbeat_interval_{kDefHeartbeatInterval};

    DiscoveryCallback<Pub> connection_cb_{nullptr};
    DiscoveryCallback<Pub> disconnection_cb_{nullptr};
    DiscoveryCallback<Pub> registration_cb_{nullptr};
    DiscoveryCallback<Pub> unregistration_cb_{nullptr};

    TopicStorage<Pub> known_publishers_;

    // process_uuid --> recent active timestamp
    std::map<std::string, Timestamp> last_seen_by_process_;
    Timestamp time_next_heartbeat_;
    Timestamp time_next_activity_;

    mutable std::mutex mutex_;
    std::thread discover_loop_thread_;

    std::atomic<bool> enabled_{false};
    std::atomic<bool> exit_{false};
};

using MsgDiscovery = Discovery<MessagePublisherInfo>;
using SrvDiscovery = Discovery<ServicePublisherInfo>;

namespace details
{
inline bool pollSockets(const std::vector<int>& sockets, const int timeout)
{
    // Discovery uses one socket for its selected local interface.
    zmq::pollitem_t items[] = {
        {0, static_cast<zmq_fd_t>(sockets.at(0)), ZMQ_POLLIN, 0}
    };

    try {
        zmq::poll(&items[0], sizeof(items) / sizeof(items[0]), std::chrono::milliseconds(timeout));
    } catch (...) {
        return false;
    }

    return items[0].revents & ZMQ_POLLIN;
}
}

template<typename Pub>
Discovery<Pub>::Discovery(const std::string& process_uuid,
                        const std::string& ip,
                        const int port)
    : multicast_group_(ip),
      port_(port),
      process_uuid_(process_uuid),
      host_addr_()
{
    if (port_ <= 0 || port_ > std::numeric_limits<uint16_t>::max()) {
        throw std::invalid_argument("Discovery initialization failed: port must be in [1, 65535].");
    }

    in_addr multicast_addr{};
    if (inet_pton(AF_INET, multicast_group_.c_str(), &multicast_addr) != 1 ||
        (ntohl(multicast_addr.s_addr) & 0xf0000000U) != 0xe0000000U) {
        throw std::invalid_argument("Discovery initialization failed: multicast group must be an IPv4 multicast address.");
    }

    host_addr_ = determineHost();

    const auto failInitialization = [this](const std::string& reason) {
        for (const auto sock : multicast_sockets_) {
            close(sock);
        }
        multicast_sockets_.clear();
        throw std::runtime_error("Discovery initialization failed: " + reason);
    };

    if (!registerNetIface(host_addr_)) {
        const std::string error = strerror(errno);
        failInitialization("failed to configure multicast interface " + host_addr_ + ": " + error + ".");
    }

    // Reuse addr and reuse port
    int reuse_addr = 1;
    if (setsockopt(multicast_sockets_.at(0), SOL_SOCKET, SO_REUSEADDR,
        reinterpret_cast<const char*>(&reuse_addr), sizeof(reuse_addr)) != 0) {
        const std::string error = strerror(errno);
        failInitialization("failed to set SO_REUSEADDR: " + error + ".");
    }

    int reuse_port = 1;
    if (setsockopt(multicast_sockets_.at(0), SOL_SOCKET, SO_REUSEPORT,
        reinterpret_cast<const char*>(&reuse_port), sizeof(reuse_port)) != 0) {
        const std::string error = strerror(errno);
        failInitialization("failed to set SO_REUSEPORT: " + error + ".");
    }

    // Bind the first socket to the discovery port
    sockaddr_in local_addr;
    memset(&local_addr, 0, sizeof(local_addr));
    local_addr.sin_family = AF_INET;
    local_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    local_addr.sin_port = htons(static_cast<ushort>(port_));

    if (bind(multicast_sockets_.at(0), reinterpret_cast<sockaddr*>(&local_addr), sizeof(sockaddr_in)) < 0) {
        const std::string error = strerror(errno);
        failInitialization("failed to bind UDP port " + std::to_string(port_) + ": " + error + ".");
    }

    memset(&multicast_addr_, 0, sizeof(multicast_addr_));
    multicast_addr_.sin_family = AF_INET;
    multicast_addr_.sin_addr = multicast_addr;
    multicast_addr_.sin_port = htons(port);

    // printCurrentState();
}

template<typename Pub>
Discovery<Pub>::~Discovery()
{
    exit_ = true;
    if (discover_loop_thread_.joinable()) {
        discover_loop_thread_.join();
    }

    sendMsg(msgs::Discovery::BYE, PublisherInfo("", "", process_uuid_, "", AdvertiseOptions{}));

    for (const auto sock : multicast_sockets_) {
        close(sock);
    }
}

template<typename Pub>
bool Discovery<Pub>::advertise(const Pub& publisher)
{
    if (!enabled_) {
        return false;
    }

    DiscoveryCallback<Pub> conn_cb;

    {
        std::lock_guard lock(mutex_);
        if (!known_publishers_.addPublisher(publisher)) {
            return false;
        }
        conn_cb = connection_cb_;
    }

    if (conn_cb) {
        conn_cb(publisher);
    }

    if (publisher.getOptions().getScope() != Scope::PROCESS) {
        sendMsg(msgs::Discovery::ADVERTISE, publisher);
    }
    return true;
}

template<typename Pub>
bool Discovery<Pub>::unadvertise(const std::string& topic, const std::string& node_uuid)
{
    if (!enabled_) {
        return false;
    }

    Pub publisher;
    {
        std::lock_guard lock(mutex_);
        if (!known_publishers_.getPublisher(topic, process_uuid_, node_uuid, publisher)) {
            return true;
        }

        known_publishers_.delPublishersByNode(topic, process_uuid_, node_uuid);
    }

    if (publisher.getOptions().getScope() != Scope::PROCESS) {
        sendMsg(msgs::Discovery::UNADVERTISE, publisher);
    }

    return true;
}

template<typename Pub>
bool Discovery<Pub>::discover(const std::string& topic) const
{
    if (!enabled_) {
        return false;
    }

    DiscoveryCallback<Pub> conn_cb;
    AddressMap<Pub> addresses;
    {
        std::lock_guard lock(mutex_);
        conn_cb = connection_cb_;
    }

    Pub pub;
    pub.setTopic(topic);
    pub.setProcessUuid(process_uuid_);

    sendMsg(msgs::Discovery::SUBSCRIBE, pub);

    bool found = false;
    {
        std::lock_guard lock(mutex_);
        found = known_publishers_.getPublishers(topic, addresses);
    }

    if (found) {
        for (const auto& proc : addresses) {
            for (const auto& node :proc.second) {
                if (conn_cb) {
                    conn_cb(node);
                }
            }
        }
    }

    return true;

}

template<typename Pub>
void Discovery<Pub>::registerNode(const MessagePublisherInfo& pub) const
{
    sendMsg(msgs::Discovery::NEW_CONNECTION, pub);
}

template<typename Pub>
void Discovery<Pub>::unRegisterNode(const MessagePublisherInfo& pub) const
{
    sendMsg(msgs::Discovery::END_CONNECTION, pub);
}

template<typename Pub>
const TopicStorage<Pub>& Discovery<Pub>::getInfo() const
{
    return known_publishers_;
}

template<typename Pub>
bool Discovery<Pub>::getPublishers(const std::string& topic, AddressMap<Pub>& publishers) const
{
    return known_publishers_.getPublishers(topic, publishers);
}

template<typename Pub>
void Discovery<Pub>::getTopicList(std::vector<std::string>& topics) const
{
    topics = known_publishers_.getTopicList();
}

template<typename Pub>
std::string Discovery<Pub>::getHostAddr() const
{
    return host_addr_;
}

template<typename Pub>
unsigned int Discovery<Pub>::getActivityInterval() const
{
    return activity_interval_.load();
}

template<typename Pub>
void Discovery<Pub>::setActivityInterval(const unsigned int ms)
{
    activity_interval_.store(ms);
}

template<typename Pub>
unsigned int Discovery<Pub>::getHeartbeatInterval() const
{
    return heartbeat_interval_.load();
}

template<typename Pub>
void Discovery<Pub>::setHeartbeatInterval(const unsigned int ms)
{
    heartbeat_interval_.store(ms);
}

template<typename Pub>
unsigned int Discovery<Pub>::getSilenceInterval() const
{
    return silence_interval_.load();
}

template<typename Pub>
void Discovery<Pub>::setSilenceInterval(const unsigned int ms)
{
    silence_interval_.store(ms);
}

template<typename Pub>
void Discovery<Pub>::setConnectionCb(const DiscoveryCallback<Pub>& cb)
{
    std::lock_guard lock(mutex_);
    connection_cb_ = cb;
}

template<typename Pub>
void Discovery<Pub>::setDisconnectionCb(const DiscoveryCallback<Pub>& cb)
{
    std::lock_guard lock(mutex_);
    disconnection_cb_ = cb;
}

template<typename Pub>
void Discovery<Pub>::setRegistrationCb(const DiscoveryCallback<Pub>& cb)
{
    std::lock_guard lock(mutex_);
    registration_cb_ = cb;
}

template<typename Pub>
void Discovery<Pub>::setUnregistrationCb(const DiscoveryCallback<Pub>& cb)
{
    std::lock_guard lock(mutex_);
    unregistration_cb_ = cb;
}

template<typename Pub>
void Discovery<Pub>::printCurrentState() const
{
    std::stringstream ss;
    std::lock_guard lock(mutex_);

    ss << "---------------\n";
    ss << std::boolalpha << "Enabled: "
              << enabled_.load() << std::endl;
    ss << "Discovery state" << std::endl;
    ss << "\tUUID: " << process_uuid_ << std::endl;
    ss << "Settings" << std::endl;
    ss << "\tActivity: " << activity_interval_
              << " ms." << std::endl;
    ss << "\tHeartbeat: " << heartbeat_interval_
              << "ms." << std::endl;
    ss << "\tSilence: " << silence_interval_
              << " ms." << std::endl;
    ss << "Known information:" << std::endl;
    known_publishers_.print(ss);

    // Used to calculate the elapsed time.
    Timestamp now = std::chrono::steady_clock::now();

    ss << "Activity" << std::endl;
    if (last_seen_by_process_.empty())
      ss << "\t<empty>" << std::endl;
    else
    {
      for (auto &proc : last_seen_by_process_)
      {
        // Elapsed time since the last update from this publisher.
        std::chrono::duration<double> elapsed = now - proc.second;

        ss << "\t" << proc.first << std::endl;
        ss << "\t\t" << "Since: " << std::chrono::duration_cast<
          std::chrono::milliseconds>(elapsed).count() << " ms. ago. "
          << std::endl;
      }
    }
    ss << "---------------" << std::endl;
    elog::debug(ss.str());
}

template<typename Pub>
void Discovery<Pub>::start()
{
    if (enabled_.exchange(true)) {
        return;
    }

    auto now = std::chrono::steady_clock::now();
    time_next_activity_ = now;
    time_next_heartbeat_ = now;

    discover_loop_thread_ = std::thread(&Discovery::loop, this);
    pthread_setname_np(discover_loop_thread_.native_handle(), "discovery_loop");
}

template<typename Pub>
void Discovery<Pub>::loop()
{
    while (!exit_) {
        int timeout = nextTimeout();

        if (details::pollSockets(multicast_sockets_, timeout)) {
            recvDiscoveryMsg();
            // printCurrentState();
        }

        updateHeartbeat();
        updateActivity();
    }
}

template<typename Pub>
void Discovery<Pub>::recvDiscoveryMsg()
{
    sockaddr_in client_addr;
    socklen_t addr_len = sizeof(client_addr);

    int received = recvfrom(multicast_sockets_.at(0), recv_buf_.data(), recv_buf_.size(), 0,
        reinterpret_cast<sockaddr*>(&client_addr), &addr_len);

    if (received >= static_cast<int>(sizeof(uint16_t))) {
        uint16_t network_msg_len = 0;
        memcpy(&network_msg_len, recv_buf_.data(), sizeof(network_msg_len));
        const uint16_t msg_len = ntohs(network_msg_len);

        if (static_cast<size_t>(received) == static_cast<size_t>(msg_len) + sizeof(msg_len))
        {
            std::string src_addr = inet_ntoa(client_addr.sin_addr);
            uint16_t src_port = ntohs(client_addr.sin_port);

            // elog::trace("Received discovery msg from {}: {}. receive_len: {}", src_addr, src_port, received);

            dispatchDiscoveryMsg(src_addr, recv_buf_.data() + sizeof(msg_len), msg_len);
        }
    } else if (received < 0) {
        elog::error("Discovery::recvDiscoveryMsg recvfrom error.");
    }
}

template<typename Pub>
void Discovery<Pub>::dispatchDiscoveryMsg(const std::string& from_ip, char* msg, uint16_t len)
{
    msgs::Discovery discovery_msg;
    if (!discovery_msg.ParseFromArray(msg, len)) {
        elog::debug("discovery_msg parse failed.");
        return;
    }
    // we ignore the version of the msg.

    std::string recv_process_uuid = discovery_msg.process_uuid();

    // discard our own discovery messages
    if (recv_process_uuid == process_uuid_) {
        return;
    }

    bool is_sender_local = from_ip == host_addr_ || from_ip.find("127.") == 0;

    DiscoveryCallback<Pub> connect_cb;
    DiscoveryCallback<Pub> disconnect_cb;
    DiscoveryCallback<Pub> register_cb;
    DiscoveryCallback<Pub> unregister_cb;

    {
        std::lock_guard lock(mutex_);
        last_seen_by_process_[recv_process_uuid] = std::chrono::steady_clock::now();

        connect_cb = connection_cb_;
        disconnect_cb = disconnection_cb_;
        register_cb = registration_cb_;
        unregister_cb = unregistration_cb_;
    }

    if (discovery_msg.type() != msgs::Discovery::HEARTBEAT) {
        elog::trace("Handle msg: {}, topic: \"{}\"", msgs::toString(discovery_msg.type()),
            discovery_msg.has_pub() ? discovery_msg.pub().topic() : discovery_msg.sub().topic());
    }
    switch (discovery_msg.type()) {
        case msgs::Discovery::ADVERTISE:
        {
            Pub publisher;
            publisher.setFromDiscovery(discovery_msg);

            if (publisher.getOptions().getScope() == Scope::PROCESS ||
                (publisher.getOptions().getScope() == Scope::HOST && !is_sender_local)) {
                return;
            }

            bool added;
            {
                std::lock_guard lock(mutex_);
                added = known_publishers_.addPublisher(publisher);
            }
            if (added && connect_cb) {
                connect_cb(publisher);
            }
            break;
        }
        case msgs::Discovery::UNADVERTISE:
        {
            Pub publisher;
            publisher.setFromDiscovery(discovery_msg);

            if (publisher.getOptions().getScope() == Scope::PROCESS ||
                (publisher.getOptions().getScope() == Scope::HOST && !is_sender_local)) {
                return;
            }

            {
                std::lock_guard lock(mutex_);
                known_publishers_.delPublishersByNode(publisher.getTopic(),
                    publisher.getProcessUuid(), publisher.getNodeUuid());
            }
            if (disconnect_cb) {
                disconnect_cb(publisher);
            }
            break;
        }
        case msgs::Discovery::SUBSCRIBE:
        {
            std::string recv_topic;
            if (discovery_msg.has_sub()) {
                recv_topic = discovery_msg.sub().topic();
            } else {
                elog::error("Subscription discovery message is missing Subscriber info.");
                break;
            }
            // If our process has any publishers for this topic, send an ADVERTISE message back to the subscriber.
            AddressMap<Pub> addresses;
            {
                std::lock_guard lock(mutex_);
                if (!known_publishers_.hasAnyPublishers(recv_topic, process_uuid_)) {
                    break;
                }

                if (!known_publishers_.getPublishers(recv_topic, addresses)) {
                    break;
                }
            }

            for (const auto& node_info : addresses[process_uuid_]) {
                if (node_info.getOptions().getScope() == Scope::PROCESS ||
                    (node_info.getOptions().getScope() == Scope::HOST && !is_sender_local)) {
                    continue;
                }

                sendMsg(msgs::Discovery::ADVERTISE, node_info);
            }
            break;
        }
        case msgs::Discovery::NEW_CONNECTION:
        {
            Pub publisher;
            publisher.setFromDiscovery(discovery_msg);

            if (register_cb) {
                register_cb(publisher);
            }
            break;
        }
        case msgs::Discovery::END_CONNECTION:
        {
            Pub publisher;
            publisher.setFromDiscovery(discovery_msg);

            if (unregister_cb) {
                unregister_cb(publisher);
            }
            break;
        }
        case msgs::Discovery::HEARTBEAT:
        {
            // Do nothing, timestamp has been updated.
            break;
        }
        case msgs::Discovery::BYE:
        {
            {
                std::lock_guard lock(mutex_);
                last_seen_by_process_.erase(recv_process_uuid);
                known_publishers_.delPublishersByProc(recv_process_uuid);
            }

            if (disconnect_cb) {
                Pub pub;
                pub.setProcessUuid(recv_process_uuid);
                disconnect_cb(pub);
            }
            break;
        }
        default:
            elog::error("Unknown message type[{}].", static_cast<int>(discovery_msg.type()));
            break;
    }
}

template<typename Pub>
void Discovery<Pub>::updateHeartbeat()
{
    auto now = std::chrono::steady_clock::now();
    if (now < time_next_heartbeat_) {
        return;
    }

    PublisherInfo pub("", "", process_uuid_, "", AdvertiseServiceOptions{});
    sendMsg(msgs::Discovery::HEARTBEAT, pub);

    // Re-Advertise topics that are advertised inside this process
    std::map<std::string, std::vector<Pub>> nodes;
    {
        std::lock_guard lock(mutex_);
        known_publishers_.getPublishersByProc(process_uuid_,nodes);
    }

    for (const auto& topic : nodes) {
        for (const auto& node : topic.second) {
            sendMsg(msgs::Discovery::ADVERTISE, node);
        }
    }

    time_next_heartbeat_ = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(heartbeat_interval_.load());
}

template<typename Pub>
void Discovery<Pub>::updateActivity()
{
    auto now = std::chrono::steady_clock::now();
    if (now < time_next_activity_) {
        return;
    }

    DiscoveryCallback<Pub> disconnect_cb;
    std::vector<std::string> expired_process_uuids;

    {
        std::lock_guard lock(mutex_);
        disconnect_cb = disconnection_cb_;

        for (auto it = last_seen_by_process_.begin(); it != last_seen_by_process_.end();) {
            auto elapsed = now - it->second;
            // This publisher has expired
            if (std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() > silence_interval_.load()) {
                known_publishers_.delPublishersByProc(it->first);
                expired_process_uuids.push_back(it->first);
                last_seen_by_process_.erase(it ++);
            } else {
                ++ it;
            }
        }
    }

    time_next_activity_ = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(activity_interval_.load());

    if (!disconnect_cb) {
        return;
    }

    for (const auto& uuid : expired_process_uuids) {
        Pub publisher;
        publisher.setProcessUuid(uuid);
        disconnect_cb(publisher);
    }
}

template<typename Pub>
template<typename T>
void Discovery<Pub>::sendMsg(const msgs::Discovery::Type type, const T& pub) const
{
    msgs::Discovery discovery_msg;
    discovery_msg.set_version(kWireVersion);
    discovery_msg.set_type(type);
    discovery_msg.set_process_uuid(process_uuid_);
    pub.fillDiscovery(discovery_msg);

    switch (type) {
        case msgs::Discovery::ADVERTISE:
        case msgs::Discovery::UNADVERTISE:
        case msgs::Discovery::NEW_CONNECTION:
        case msgs::Discovery::END_CONNECTION:
            pub.fillDiscovery(discovery_msg);
            break;
        case msgs::Discovery::SUBSCRIBE:
            discovery_msg.mutable_sub()->set_topic(pub.getTopic());
            break;
        case msgs::Discovery::HEARTBEAT:
        case msgs::Discovery::BYE:
            break;
        default:
            elog::error("Discovery::SendMsg(), unexpected type: {}", static_cast<int>(type));
            return;
    }

    sendMulticast(discovery_msg);

    // elog::debug("Sending {}, msg [{}]", msgs::toString(type), pub.getTopic());
}

template<typename Pub>
void Discovery<Pub>::sendMulticast(const msgs::Discovery& msg) const
{
    size_t msg_size_full = msg.ByteSizeLong();
    if (msg_size_full + sizeof(uint16_t) > kMaxRcvStr) {
        elog::error("Discovery message too large to send. this should not happen.");
        return;
    }

    const uint16_t msg_size = static_cast<uint16_t>(msg_size_full);
    uint16_t total_size = msg_size + sizeof(msg_size);
    auto send_buf = std::make_unique<char[]>(total_size);
    const uint16_t network_msg_size = htons(msg_size);
    memcpy(send_buf.get(), &network_msg_size, sizeof(network_msg_size));

    if (msg.SerializeToArray(send_buf.get() + sizeof(msg_size), msg_size)) {
        for (auto sock : multicast_sockets_) {
            errno = 0;
            if (sendto(sock, send_buf.get(), total_size, 0,
                reinterpret_cast<const sockaddr*>(&multicast_addr_), sizeof(multicast_addr_)) != total_size) {
                if (errno == EPERM || errno == ENOBUFS) {
                    continue;
                }
                elog::error("Exception sending a multicast message: {}", strerror(errno));
                break;
            }
        }
    } else {
        elog::error("Discovery::SendMulticast: error serializing msg.");
    }
}

template<typename Pub>
int Discovery<Pub>::nextTimeout() const
{
    auto now = std::chrono::steady_clock::now();
    auto time_until_next_heartbeat = time_next_heartbeat_ - now;
    auto time_until_next_activity = time_next_activity_ - now;

    int t = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::min(time_until_next_activity, time_until_next_heartbeat)
    ).count());
    int t2 = std::min(t, kTimeout);
    return std::max(t2, 0);
}

template<typename Pub>
bool Discovery<Pub>::registerNetIface(const std::string& ip)
{
    int sock = static_cast<int>(socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP));
    if (sock < 0) {
        elog::error("Socket creation failed: {}", strerror(errno));
        return false;
    }

    // Socket option: IP_MULTICAST_IF.
    // This socket option needs to be applied to each socket used to send
    // data. This option selects the source interface for outgoing messages.
    struct in_addr if_addr;
    if_addr.s_addr = inet_addr(ip.c_str());
    if (setsockopt(sock, IPPROTO_IP, IP_MULTICAST_IF,
        reinterpret_cast<const char*>(&if_addr), sizeof(if_addr)) != 0) {
        const int error = errno;
        elog::error("Error setting socket option (IP_MULTICAST_IF): {}", strerror(error));
        close(sock);
        errno = error;
        return false;
    }

    // Join the multicast group. We have to do it for each network interface
    // but we can do it on the same socket. We will use the socket at
    // position 0 for receiving multicast information.
    struct ip_mreq group;
    group.imr_multiaddr.s_addr = inet_addr(multicast_group_.c_str());
    group.imr_interface.s_addr = inet_addr(ip.c_str());
    const int receive_sock = multicast_sockets_.empty() ? sock : multicast_sockets_.front();
    if (setsockopt(receive_sock, IPPROTO_IP, IP_ADD_MEMBERSHIP,
        reinterpret_cast<const char*>(&group), sizeof(group)) != 0) {
        const int error = errno;
        elog::error("Error setting socket option (IP_ADD_MEMBERSHIP): {}", strerror(error));
        close(sock);
        errno = error;
        return false;
    }

    multicast_sockets_.push_back(sock);

    return true;
}


} // namespace trans
} // namespace rf
