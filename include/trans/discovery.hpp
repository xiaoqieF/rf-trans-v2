#pragma once

#include <string>
#include <vector>
#include <limits>
#include <sstream>

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

/// TODO: move some unnecessary mutex
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
    void waitForInit();

    bool advertise(const Pub& publisher);
    bool unadvertise(const std::string& topic, const std::string& node_uuid);

    // Try to discover [topic] immediately, whic send a SUBSCRIBE msg about topic
    bool discover(const std::string& topic) const;
    void sendSubscribersRep(const MessagePublisherInfo& pub) const;
    void registerNode(const MessagePublisherInfo& pub) const;
    void unRegisterNode(const MessagePublisherInfo& pub) const;

    const TopicStorage<Pub>& getInfo() const;
    bool getPublishers(const std::string& topic, AddressMap<Pub>& publishers) const;
    bool getRemoteSubscribers(const std::string& topic, AddressMap<Pub>& subscribers) const;

    // How do this func work ?
    void getTopicList(std::vector<std::string>& topics);
    std::string getHostAddr() const;
    unsigned int getActivityInterval() const;
    void setActivityInterval(const unsigned int ms);
    unsigned int getHeartbeatInterval() const;
    void setHeartbeatInterval(const unsigned int ms);
    unsigned int getSilenceInterval() const;
    void setSilenceInterval(const unsigned int ms);

    void setConnectionCb(const DiscoveryCallback<Pub>& cb);
    void setDisconnectionCb(const DiscoveryCallback<Pub>& cb);
    void setRegisterationCb(const DiscoveryCallback<Pub>& cb);
    void setUnregisterationCb(const DiscoveryCallback<Pub>& cb);
    void setSubscribersCb(const std::function<void()>& cb);

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
    static constexpr unsigned int kDefActivityInterval = 100;
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
    std::vector<std::string> host_ingerfaces_;
    std::vector<int> sockets_;
    unsigned int silence_interval_;
    unsigned int activity_interval_;
    unsigned int heartbeat_interval_;

    DiscoveryCallback<Pub> connection_cb_{nullptr};
    DiscoveryCallback<Pub> disconnection_cb_{nullptr};
    DiscoveryCallback<Pub> registration_cb_{nullptr};
    DiscoveryCallback<Pub> unregistration_cb_{nullptr};
    std::function<void()> subscribers_cb_{nullptr};

    TopicStorage<Pub> info_;
    TopicStorage<Pub> remote_subscribers_;

    // process_uuid --> recent active timestamp
    std::map<std::string, Timestamp> activity_;
    Timestamp time_next_heartbeat_;
    Timestamp time_next_activity_;
    unsigned int heartbeat_count_{0};

    mutable std::mutex mutex_;
    std::thread discover_loop_thread_;

    std::atomic<bool> enabled_{false};
    std::atomic<bool> initialized_{false};
    std::atomic<bool> exit_{false};

    std::condition_variable initialize_cv_;
};

using MsgDiscovery = Discovery<MessagePublisherInfo>;
using SrvDiscovery = Discovery<ServicePublisherInfo>;

namespace details
{
inline bool pollSockets(const std::vector<int>& sockets, const int timeout)
{
    // We only listen to the first socket, cause we bind all interfaces on it.
    // all messages from different interfaces will come to the first socket.
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
      host_addr_(determineHost()),
      silence_interval_(kDefSilenceInterval),
      activity_interval_(kDefActivityInterval),
      heartbeat_interval_(kDefHeartbeatInterval)
{
    std::string host_ip;
    if (getEnv("RF_HOST_IP", host_ip) && !host_ip.empty()) {
        host_ingerfaces_ = {host_ip};
    } else {
        host_ingerfaces_ = determineInterfaces();
    }

    for (const auto& net_iface : host_ingerfaces_) {
        auto succeed = registerNetIface(net_iface);
        if (!succeed && net_iface == host_addr_) {
            registerNetIface("127.0.0.1");
            elog::error("Maybe you set RF_HOST_IP with a non-correct IP address: {}, Using 127.0.0.1 as hostname.", net_iface);
            host_addr_ = "127.0.0.1";
        }
    }

    // Reuse addr and reuse port
    int reuse_addr = 1;
    if (setsockopt(sockets_.at(0), SOL_SOCKET, SO_REUSEADDR,
        reinterpret_cast<const char*>(&reuse_addr), sizeof(reuse_addr)) != 0) {
        elog::error("Error setting socket option (SO_REUSEADDR).");
        return;
    }

    int reuse_port = 1;
    if (setsockopt(sockets_.at(0), SOL_SOCKET, SO_REUSEPORT,
        reinterpret_cast<const char*>(&reuse_port), sizeof(reuse_port)) != 0) {
        elog::error("Error setting socket option (SO_REUSEPORT).");
        return;
    }

    // Bind the first socket to the discovery port
    sockaddr_in local_addr;
    memset(&local_addr, 0, sizeof(local_addr));
    local_addr.sin_family = AF_INET;
    local_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    local_addr.sin_port = htons(static_cast<ushort>(port_));

    if (bind(sockets_.at(0), reinterpret_cast<sockaddr*>(&local_addr), sizeof(sockaddr_in)) < 0) {
        elog::error("Bind to a local port[{}] failed.", port_);
        return;
    }

    memset(&multicast_addr_, 0, sizeof(multicast_addr_));
    multicast_addr_.sin_family = AF_INET;
    multicast_addr_.sin_addr.s_addr = inet_addr(multicast_group_.c_str());
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

    for (const auto sock : sockets_) {
        close(sock);
    }
}

template<typename Pub>
void Discovery<Pub>::waitForInit()
{
    std::unique_lock lock(mutex_);
    if (!initialized_) {
        initialize_cv_.wait(lock, [this] { return initialized_.load(); });
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
        if (!info_.addPublisher(publisher)) {
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
        if (!info_.getPublisher(topic, process_uuid_, node_uuid, publisher)) {
            return true;
        }

        info_.delPublishersByNode(topic, process_uuid_, node_uuid);
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
        found = info_.getPublishers(topic, addresses);
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
void Discovery<Pub>::sendSubscribersRep(const MessagePublisherInfo& pub) const
{
    sendMsg(msgs::Discovery::SUBSCRIBERS_REP, pub);
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
    std::lock_guard lock(mutex_);
    return info_;
}

template<typename Pub>
bool Discovery<Pub>::getPublishers(const std::string& topic, AddressMap<Pub>& publishers) const
{
    std::lock_guard lock(mutex_);
    return info_.getPublishers(topic, publishers);
}

template<typename Pub>
bool Discovery<Pub>::getRemoteSubscribers(const std::string& topic, AddressMap<Pub>& subscribers) const
{
    std::lock_guard lock(mutex_);
    return remote_subscribers_;
}

template<typename Pub>
void Discovery<Pub>::getTopicList(std::vector<std::string>& topics)
{
    topics.clear();
    {
        std::lock_guard lock(mutex_);
        remote_subscribers_.clear();
    }

    PublisherInfo pub("", "", process_uuid_, "", AdvertiseOptions{});
    sendMsg(msgs::Discovery::SUBSCRIBERS_REQ, pub);

    waitForInit();

    std::lock_guard lock(mutex_);
    topics = info_.getTopicList();

    /// ??? whis shoud not work
    std::vector<std::string> remote_subs = remote_subscribers_.getTopicList();

    for (const auto& t : remote_subs) {
        if (std::find(topics.begin(), topics.end(), t) != topics.end()) {
            topics.push_back(t);
        }
    }
}

template<typename Pub>
std::string Discovery<Pub>::getHostAddr() const
{
    return host_addr_;
}

template<typename Pub>
unsigned int Discovery<Pub>::getActivityInterval() const
{
    std::lock_guard lock(mutex_);
    return activity_interval_;
}

template<typename Pub>
void Discovery<Pub>::setActivityInterval(const unsigned int ms)
{
    std::lock_guard lock(mutex_);
    activity_interval_ = ms;
}

template<typename Pub>
unsigned int Discovery<Pub>::getHeartbeatInterval() const
{
    std::lock_guard lock(mutex_);
    return heartbeat_interval_;
}

template<typename Pub>
void Discovery<Pub>::setHeartbeatInterval(const unsigned int ms)
{
    std::lock_guard lock(mutex_);
    heartbeat_interval_ = ms;
}

template<typename Pub>
unsigned int Discovery<Pub>::getSilenceInterval() const
{
    std::lock_guard lock(mutex_);
    return silence_interval_;
}

template<typename Pub>
void Discovery<Pub>::setSilenceInterval(const unsigned int ms)
{
    std::lock_guard lock(mutex_);
    silence_interval_ = ms;
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
void Discovery<Pub>::setRegisterationCb(const DiscoveryCallback<Pub>& cb)
{
    std::lock_guard lock(mutex_);
    registration_cb_ = cb;
}

template<typename Pub>
void Discovery<Pub>::setUnregisterationCb(const DiscoveryCallback<Pub>& cb)
{
    std::lock_guard lock(mutex_);
    unregistration_cb_ = cb;
}

template<typename Pub>
void Discovery<Pub>::setSubscribersCb(const std::function<void()>& cb)
{
    std::lock_guard lock(mutex_);
    subscribers_cb_ = cb;
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
    info_.print(ss);

    // Used to calculate the elapsed time.
    Timestamp now = std::chrono::steady_clock::now();

    ss << "Activity" << std::endl;
    if (activity_.empty())
      ss << "\t<empty>" << std::endl;
    else
    {
      for (auto &proc : activity_)
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
    enabled_ = true;
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

        if (details::pollSockets(sockets_, timeout)) {
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
    // todo: could we cut down kMaxRcvStr
    char recv_buf[kMaxRcvStr];
    sockaddr_in client_addr;
    socklen_t addr_len = sizeof(client_addr);

    int received = recvfrom(sockets_.at(0), recv_buf, sizeof(recv_buf), 0,
        reinterpret_cast<sockaddr*>(&client_addr), &addr_len);

    if (received > 0) {
        uint16_t msg_len = 0;
        memcpy(&msg_len, recv_buf, sizeof(msg_len));

        if (msg_len + sizeof(msg_len) == static_cast<uint16_t>(received))
        {
            std::string src_addr = inet_ntoa(client_addr.sin_addr);
            uint16_t src_port = ntohs(client_addr.sin_port);

            // elog::trace("Received discovery msg from {}: {}. receive_len: {}", src_addr, src_port, received);

            dispatchDiscoveryMsg(src_addr, recv_buf + sizeof(msg_len), msg_len);
        }
    } else {
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

    bool is_sender_local = (std::find(host_ingerfaces_.begin(),
        host_ingerfaces_.end(), from_ip) != host_ingerfaces_.end()) ||
        (from_ip.find("127.") == 0);

    DiscoveryCallback<Pub> connect_cb;
    DiscoveryCallback<Pub> disconnect_cb;
    DiscoveryCallback<Pub> register_cb;
    DiscoveryCallback<Pub> unregister_cb;
    std::function<void()> subscribers_req_cb;

    {
        std::lock_guard lock(mutex_);
        activity_[recv_process_uuid] = std::chrono::steady_clock::now();

        connect_cb = connection_cb_;
        disconnect_cb = disconnection_cb_;
        register_cb = registration_cb_;
        unregister_cb = unregistration_cb_;
        subscribers_req_cb = subscribers_cb_;
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
                added = info_.addPublisher(publisher);
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

            if (disconnect_cb) {
                disconnect_cb(publisher);
            }

            {
                std::lock_guard lock(mutex_);
                info_.delPublishersByNode(publisher.getTopic(),
                    publisher.getProcessUuid(), publisher.getNodeUuid());
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
            AddressMap<Pub> addresses;
            {
                std::lock_guard lock(mutex_);
                if (!info_.hasAnyPublishers(recv_topic, process_uuid_)) {
                    break;
                }

                if (!info_.getPublishers(recv_topic, addresses)) {
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
        case msgs::Discovery::SUBSCRIBERS_REQ:
        {
            if(subscribers_req_cb) {
                subscribers_req_cb();
            }
            break;
        }
        case msgs::Discovery::SUBSCRIBERS_REP:
        {
            Pub publisher;
            publisher.setFromDiscovery(discovery_msg);

            {
                std::lock_guard lock(mutex_);
                remote_subscribers_.addPublisher(publisher);
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
                activity_.erase(recv_process_uuid);
            }

            if (disconnect_cb) {
                Pub pub;
                pub.setProcessUuid(recv_process_uuid);
                disconnect_cb(pub);
            }

            {
                std::lock_guard lock(mutex_);
                info_.delPublishersByProc(recv_process_uuid);
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
        info_.getPublishersByProc(process_uuid_,nodes);
    }

    for (const auto& topic : nodes) {
        for (const auto& node : topic.second) {
            sendMsg(msgs::Discovery::ADVERTISE, node);
        }
    }

    if (!initialized_) {
        if (heartbeat_count_ == 2u) {
            initialized_ = true;
        }
        initialize_cv_.notify_all();
    }
    ++ heartbeat_count_;

    time_next_heartbeat_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(heartbeat_interval_);
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

        for (auto it = activity_.begin(); it != activity_.end();) {
            auto elapsed = now - it->second;
            // This publisher has expired
            if (std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() > silence_interval_) {
                info_.delPublishersByProc(it->first);
                expired_process_uuids.push_back(it->first);
                activity_.erase(it ++);
            } else {
                ++ it;
            }
        }
    }

    time_next_activity_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(activity_interval_);

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
        case msgs::Discovery::SUBSCRIBERS_REQ:
        case msgs::Discovery::SUBSCRIBERS_REP:
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
    uint16_t msg_size = 0;
    size_t msg_size_full = msg.ByteSizeLong();
    if (msg_size_full + sizeof(msg_size) > kMaxRcvStr) {
        elog::error("Discovery message too large to send. this should not happen.");
        return;
    }

    msg_size = msg_size_full;
    uint16_t total_size = msg_size + sizeof(msg_size);
    auto send_buf = std::make_unique<char[]>(total_size);
    memcpy(send_buf.get(), &msg_size, sizeof(msg_size));

    if (msg.SerializeToArray(send_buf.get() + sizeof(msg_size), msg_size)) {
        for (auto sock : sockets_) {
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
    auto time_until_next_activity = time_next_heartbeat_ - now;

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
        elog::error("Socket creation failed.");
        return false;
    }

    // Socket option: IP_MULTICAST_IF.
    // This socket option needs to be applied to each socket used to send
    // data. This option selects the source interface for outgoing messages.
    struct in_addr if_addr;
    if_addr.s_addr = inet_addr(ip.c_str());
    if (setsockopt(sock, IPPROTO_IP, IP_MULTICAST_IF,
        reinterpret_cast<const char*>(&if_addr), sizeof(if_addr)) != 0) {
        elog::error("Error setting socket option (IP_MULTICAST_IF).");
        return false;
    }

    sockets_.push_back(sock);

    // Join the multicast group. We have to do it for each network interface
    // but we can do it on the same socket. We will use the socket at
    // position 0 for receiving multicast information.
    /// TODO: Do not receive msgs of different interface at the same socket.
    struct ip_mreq group;
    group.imr_multiaddr.s_addr = inet_addr(multicast_group_.c_str());
    group.imr_interface.s_addr = inet_addr(ip.c_str());
    if (setsockopt(sockets_.at(0), IPPROTO_IP, IP_ADD_MEMBERSHIP,
        reinterpret_cast<const char*>(&group), sizeof(group)) != 0) {
        elog::error("Error setting socket option (IP_ADD_MEMBERSHIP): {}", strerror(errno));
        return false;
    }

    return true;
}


} // namespace trans
} // namespace rf