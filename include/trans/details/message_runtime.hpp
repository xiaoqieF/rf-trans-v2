#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "zmq.hpp"

#include "trans/details/discovery.hpp"
#include "trans/details/handler_wrapper.hpp"
#include "trans/details/publish_msg_details.hpp"
#include "trans/details/shm_ring.hpp"
#include "trans/details/trans_types.hpp"

namespace rf
{
namespace trans
{
class MessageRuntime
{
public:
    MessageRuntime(zmq::context_t& context, MsgDiscovery& discovery, std::string process_uuid);
    ~MessageRuntime();

    MessageRuntime(const MessageRuntime&) = delete;
    MessageRuntime& operator=(const MessageRuntime&) = delete;

    bool initialize();
    void start();
    void stop();

    bool unsubscribe(const std::string& topic, const std::string& node_uuid);
    bool unadvertise(const std::string& topic, const std::string& node_uuid);
    bool advertise(const std::string& topic, const std::string& node_uuid,
        const std::string& msg_type, const AdvertiseMessageOptions& options,
        MessagePublisherInfo& publisher);
    bool subscribe(const std::string& topic, const std::string& node_uuid,
        const ISubscriptionHandlerPtr& handler);
    bool publish(const MessagePublisherInfo& publisher, std::unique_ptr<ProtoMsg> msg);
    bool hasSubscribers(const std::string& topic, const std::string& msg_type) const;

    void getAdvertisedTopics(const std::string& node_uuid, std::set<std::string>& topics) const;
    void getTopics(std::vector<std::string>& topics) const;

    const std::string& hostAddress() const { return host_address_; }
    const std::string& address() const { return address_; }

private:
    enum class SubscriberCommandType
    {
        Connect,
        Subscribe,
        Unsubscribe,
    };

    struct SubscriberCommand
    {
        SubscriberCommandType type;
        std::string value;
        std::vector<MessagePublisherInfo> registrations;
    };

    bool publishRemote(const std::string& topic, char* data, size_t data_size,
        DeallocFunc* ffn, const std::string& msg_type);
    bool publishRemoteShm(const MessagePublisherInfo& publisher, const ProtoMsg& msg);
    std::shared_ptr<ShmRing> writerRingFor(const std::string& topic, size_t required_size);
    std::shared_ptr<ShmRing> readerRingFor(const std::string& shm_name);
    void removeWriterRing(const std::string& topic);
    void receiveLoop();
    void localDispatchLoop();
    void remoteDispatchLoop();

    void receiveMessage();
    void enqueueSubscriberCommand(SubscriberCommandType type, std::string value,
        std::vector<MessagePublisherInfo> registrations = {});
    void applySubscriberCommands();

    void onPublisherConnected(const MessagePublisherInfo& pub);
    void onPublisherDisconnected(const MessagePublisherInfo& pub);
    void onSubscriberRegistered(const MessagePublisherInfo& pub);
    void onSubscriberUnregistered(const MessagePublisherInfo& pub);

    HandlerInfo localHandlers(const std::string& topic) const;
    SubscriberInfo subscribers(const std::string& topic, const std::string& msg_type) const;

private:
    static constexpr size_t kMsgQueueLimit = 1000;

    MsgDiscovery& discovery_;
    const std::string process_uuid_;

    std::string host_address_;
    std::string address_;

    std::unique_ptr<zmq::socket_t> subscriber_;
    std::unique_ptr<zmq::socket_t> publisher_;
    mutable std::mutex publisher_mutex_;

    TopicStorage<MessagePublisherInfo> remote_publishers_;
    TopicStorage<MessagePublisherInfo> remote_subscribers_;
    HandlerWrapper local_subscribers_;

    std::deque<SubscriberCommand> subscriber_commands_;
    mutable std::mutex subscriber_commands_mutex_;

    std::atomic_bool exit_{false};
    bool initialized_{false};
    bool started_{false};
    std::thread receive_thread_;

    std::thread local_dispatch_thread_;
    std::deque<std::unique_ptr<PublishMsgDetails>> local_publish_queue_;
    mutable std::mutex local_publish_mutex_;
    std::condition_variable local_publish_cv_;

    std::thread remote_dispatch_thread_;
    std::deque<std::unique_ptr<RemoteMsg>> remote_message_queue_;
    mutable std::mutex remote_message_mutex_;
    std::condition_variable remote_message_cv_;

    // Shm rings this process writes into, one per advertised topic that uses
    // shared memory. Created lazily on first publish, torn down (unlinked)
    // on unadvertise or destruction. Held as shared_ptr (not unique_ptr) so a
    // concurrent unadvertise() erasing the map entry can't destroy a ring
    // while another thread is still mid-write() on the pointer it looked up
    // just before releasing writer_rings_mutex_.
    std::unordered_map<std::string, std::shared_ptr<ShmRing>> writer_rings_;
    mutable std::mutex writer_rings_mutex_;

    // Shm rings this process reads from, keyed by shm object name, opened
    // lazily on first message received for that name.
    std::unordered_map<std::string, std::shared_ptr<ShmRing>> reader_rings_;
    mutable std::mutex reader_rings_mutex_;
};

} // namespace trans
} // namespace rf
