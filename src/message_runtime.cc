#include "trans/message_runtime.hpp"

namespace rf
{
namespace trans
{

MessageRuntime::MessageRuntime(zmq::context_t& context, MsgDiscovery& discovery, std::string process_uuid)
    : discovery_(discovery),
      process_uuid_(std::move(process_uuid)),
      subscriber_(std::make_unique<zmq::socket_t>(context, ZMQ_SUB)),
      publisher_(std::make_unique<zmq::socket_t>(context, ZMQ_PUB))
{
}

MessageRuntime::~MessageRuntime()
{
    stop();
}

bool MessageRuntime::initialize()
{
    if (initialized_) {
        return true;
    }

    try {
        host_address_ = discovery_.getHostAddr();
        const std::string any_tcp_ep = "tcp://" + host_address_ + ":*";

        int linger_val = 0;
        publisher_->set(zmq::sockopt::linger, linger_val);
        publisher_->set(zmq::sockopt::sndhwm, kDefaultSndHwm);
        publisher_->bind(any_tcp_ep.c_str());
        address_ = publisher_->get(zmq::sockopt::last_endpoint);

        subscriber_->set(zmq::sockopt::rcvhwm, kDefaultRcvHwm);
    } catch (const zmq::error_t& e) {
        elog::error("MessageRuntime initialization error: {}", e.what());
        return false;
    }

    initialized_ = true;
    return true;
}

void MessageRuntime::start()
{
    if (!initialized_ || started_) {
        return;
    }

    receive_thread_ = std::thread(&MessageRuntime::receiveLoop, this);
    pthread_setname_np(receive_thread_.native_handle(), "rf_recv_loop");

    discovery_.setConnectionCb([this] (const MessagePublisherInfo& pub) {
        onPublisherConnected(pub);
    });
    discovery_.setDisconnectionCb([this] (const MessagePublisherInfo& pub) {
        onPublisherDisconnected(pub);
    });
    discovery_.setRegistrationCb([this] (const MessagePublisherInfo& pub) {
        onSubscriberRegistered(pub);
    });
    discovery_.setUnregistrationCb([this] (const MessagePublisherInfo& pub) {
        onSubscriberUnregistered(pub);
    });
    discovery_.start();

    local_dispatch_thread_ = std::thread(&MessageRuntime::localDispatchLoop, this);
    pthread_setname_np(local_dispatch_thread_.native_handle(), "rf_local_pub");

    remote_dispatch_thread_ = std::thread(&MessageRuntime::remoteDispatchLoop, this);
    pthread_setname_np(remote_dispatch_thread_.native_handle(), "rf_remote_pub");

    started_ = true;
}

void MessageRuntime::stop()
{
    if (!started_) {
        return;
    }

    exit_ = true;
    local_publish_cv_.notify_all();
    remote_message_cv_.notify_all();

    if (receive_thread_.joinable()) {
        receive_thread_.join();
    }
    if (local_dispatch_thread_.joinable()) {
        local_dispatch_thread_.join();
    }
    if (remote_dispatch_thread_.joinable()) {
        remote_dispatch_thread_.join();
    }

    discovery_.setConnectionCb(nullptr);
    discovery_.setDisconnectionCb(nullptr);
    discovery_.setRegistrationCb(nullptr);
    discovery_.setUnregistrationCb(nullptr);
    started_ = false;
}

void MessageRuntime::onPublisherConnected(const MessagePublisherInfo& pub)
{
    const std::string topic = pub.getTopic();
    const std::string addr = pub.getAddr();
    const std::string remote_proc_uuid = pub.getProcessUuid();

    if (!local_subscribers_.hasSubscriber(topic) || remote_proc_uuid == process_uuid_) {
        return;
    }

    std::vector<MessagePublisherInfo> registrations;
    MessagePublisherInfo registration(pub);
    registration.setProcessUuid(process_uuid_);
    registration.setCtrl(pub.getProcessUuid());
    const auto handler_node_uuids = local_subscribers_.getNodeUuids(topic, pub.getMsgType());
    for (const auto& node_uuid : handler_node_uuids) {
        registration.setNodeUuid(node_uuid);
        registrations.push_back(registration);
    }

    if (!remote_publishers_.hasPublisher(addr)) {
        elog::debug("Connect to pub.");
        enqueueSubscriberCommand(SubscriberCommandType::Connect, addr);
    }

    enqueueSubscriberCommand(SubscriberCommandType::Subscribe, topic, std::move(registrations));
    remote_publishers_.addPublisher(pub);
    elog::debug("Connected to [{}] for data.", addr);
}

void MessageRuntime::onPublisherDisconnected(const MessagePublisherInfo& pub)
{
    const std::string topic = pub.getTopic();
    const std::string remote_proc_uuid = pub.getProcessUuid();
    const std::string node_uuid = pub.getNodeUuid();

    elog::debug("New disconnection detected, process_uuid: {}", remote_proc_uuid);
    if (topic.empty() && node_uuid.empty()) {
        remote_subscribers_.delPublishersByProc(remote_proc_uuid);
        remote_publishers_.delPublishersByProc(remote_proc_uuid);
        return;
    }

    remote_publishers_.delPublishersByNode(topic, remote_proc_uuid, node_uuid);
}

void MessageRuntime::onSubscriberRegistered(const MessagePublisherInfo& pub)
{
    if (pub.getCtrl() != process_uuid_) {
        return;
    }

    elog::debug("Registering a new remote connection.\n\t Proc uuid: {}\n\t Node uuid: {}",
        pub.getProcessUuid(), pub.getNodeUuid());
    remote_subscribers_.addPublisher(pub);
}

void MessageRuntime::onSubscriberUnregistered(const MessagePublisherInfo& pub)
{
    if (!pub.getCtrl().empty() && pub.getCtrl() != process_uuid_) {
        return;
    }

    elog::debug("EndRegistering a remote connection.\n\t Proc uuid: {}\n\t Node uuid: {}",
        pub.getProcessUuid(), pub.getNodeUuid());
    remote_subscribers_.delPublishersByNode(pub.getTopic(), pub.getProcessUuid(), pub.getNodeUuid());
}

bool MessageRuntime::unsubscribe(const std::string& topic, const std::string& node_uuid)
{
    elog::trace("unsubscribe topic[{}]", topic);
    local_subscribers_.removeHandlersForNode(topic, node_uuid);

    if (!local_subscribers_.hasSubscriber(topic)) {
        enqueueSubscriberCommand(SubscriberCommandType::Unsubscribe, topic);
    }

    AddressMap<MessagePublisherInfo> addresses;
    discovery_.getPublishers(topic, addresses);
    for (const auto& [publisher_process_uuid, publishers] : addresses) {
        MessagePublisherInfo pub(topic, address_, publisher_process_uuid, process_uuid_, node_uuid,
            kGenericMessageType, AdvertiseMessageOptions{});
        discovery_.unRegisterNode(pub);
    }

    return true;
}

bool MessageRuntime::advertise(const std::string& topic, const std::string& node_uuid,
    const std::string& msg_type, const AdvertiseMessageOptions& options, MessagePublisherInfo& publisher)
{
    publisher = MessagePublisherInfo(topic, address_, "unused", process_uuid_, node_uuid, msg_type, options);
    return discovery_.advertise(publisher);
}

bool MessageRuntime::subscribe(const std::string& topic, const std::string& node_uuid,
    const ISubscriptionHandlerPtr& handler)
{
    local_subscribers_.normal_.addHandler(topic, node_uuid, handler);
    return discovery_.discover(topic);
}

bool MessageRuntime::publishRemote(const std::string& topic, char* data, const size_t data_size,
    DeallocFunc* ffn, const std::string& msg_type)
{
    zmq::message_t msg0(topic.data(), topic.size()),
                   msg1(address_.data(), address_.size()),
                   msg2(data, data_size, ffn, nullptr),
                   msg3(msg_type.data(), msg_type.size());

    std::lock_guard lock(publisher_mutex_);
    try {
        publisher_->send(msg0, zmq::send_flags::sndmore);
        publisher_->send(msg1, zmq::send_flags::sndmore);
        publisher_->send(msg2, zmq::send_flags::sndmore);
        publisher_->send(msg3, zmq::send_flags::none);
    } catch (const zmq::error_t& e) {
        elog::error("MessageRuntime::publishRemote error: {}", e.what());
        return false;
    }
    return true;
}

bool MessageRuntime::publish(const MessagePublisherInfo& publisher, std::unique_ptr<ProtoMsg> msg)
{
    const std::string& topic = publisher.getTopic();
    const std::string msg_type = publisher.getMsgType();
    const auto subscriber_info = subscribers(topic, msg_type);

    if (subscriber_info.have_remote) {
        const auto msg_size = msg->ByteSizeLong();
        char* serialize_buffer = new char[msg_size];
        if (!msg->SerializeToArray(serialize_buffer, msg_size)) {
            elog::error("Publisher::publish() error serializing data");
            delete [] serialize_buffer;
            return false;
        }

        auto deallocator = [] (void* buffer, void*) {
            delete [] reinterpret_cast<char*>(buffer);
        };

        if (!publishRemote(topic, serialize_buffer, msg_size, deallocator, msg_type)) {
            return false;
        }
    }

    if (subscriber_info.local_handler_info.have_local) {
        auto details = std::make_unique<PublishMsgDetails>();
        details->info.setTopic(topic);
        details->info.setMsgType(msg_type);
        details->msg_copy = std::move(msg);
        details->publisher_node_uuid = publisher.getNodeUuid();

        for (const auto& [node_uuid, handlers] : subscriber_info.local_handler_info.local_handlers) {
            for (const auto& [handler_uuid, handler] : handlers) {
                if (!handler) {
                    elog::error("Publisher::publish() error, null local subscription handler");
                    continue;
                }
                if (handler->getMsgType() != msg_type) {
                    elog::error("Publisher::publish() error, msg type mismatch");
                    continue;
                }
                details->local_handlers.push_back(handler);
            }
        }

        std::lock_guard lock(local_publish_mutex_);
        local_publish_queue_.push_back(std::move(details));
        local_publish_cv_.notify_one();
    }

    return true;
}

bool MessageRuntime::hasSubscribers(const std::string& topic, const std::string& msg_type) const
{
    return local_subscribers_.hasSubscriber(topic, msg_type) || remote_subscribers_.hasTopic(topic, msg_type);
}

void MessageRuntime::getAdvertisedTopics(const std::string& node_uuid, std::set<std::string>& topics) const
{
    std::vector<MessagePublisherInfo> publishers;
    discovery_.getInfo().getPublishersByNode(process_uuid_, node_uuid, publishers);
    for (const auto& publisher : publishers) {
        topics.insert(publisher.getTopic());
    }
}

void MessageRuntime::getTopics(std::vector<std::string>& topics) const
{
    discovery_.getTopicList(topics);
}

void MessageRuntime::localDispatchLoop()
{
    using namespace std::chrono_literals;

    while (!exit_) {
        std::deque<std::unique_ptr<PublishMsgDetails>> message_details;
        {
            std::unique_lock lock(local_publish_mutex_);
            if (local_publish_queue_.empty()) {
                local_publish_cv_.wait_for(lock, 500ms,
                    [this] { return !local_publish_queue_.empty() || exit_; });
            }
            if (exit_) {
                return;
            }
            if (local_publish_queue_.empty()) {
                continue;
            }
            message_details.swap(local_publish_queue_);
        }

        for (auto& message_detail : message_details) {
            std::shared_ptr<ProtoMsg> shared_msg = std::move(message_detail->msg_copy);
            for (auto& handler : message_detail->local_handlers) {
                handler->runLocalCallback(shared_msg, message_detail->info);
            }
        }
    }
}

void MessageRuntime::receiveLoop()
{
    while (!exit_) {
        applySubscriberCommands();

        zmq::pollitem_t items[] = {
            {*subscriber_, 0, ZMQ_POLLIN, 0},
        };

        try {
            zmq::poll(items, 1, std::chrono::milliseconds(250));
        } catch (...) {
            continue;
        }

        if (items[0].revents & ZMQ_POLLIN) {
            receiveMessage();
        }
    }
}

void MessageRuntime::enqueueSubscriberCommand(const SubscriberCommandType type, std::string value,
    std::vector<MessagePublisherInfo> registrations)
{
    std::lock_guard lock(subscriber_commands_mutex_);
    subscriber_commands_.push_back({type, std::move(value), std::move(registrations)});
}

void MessageRuntime::applySubscriberCommands()
{
    std::deque<SubscriberCommand> commands;
    {
        std::lock_guard lock(subscriber_commands_mutex_);
        commands.swap(subscriber_commands_);
    }

    for (const auto& command : commands) {
        try {
            switch (command.type) {
            case SubscriberCommandType::Connect:
                subscriber_->connect(command.value.c_str());
                break;
            case SubscriberCommandType::Subscribe:
                if (local_subscribers_.hasSubscriber(command.value)) {
                    subscriber_->set(zmq::sockopt::subscribe, command.value);
                    for (const auto& registration : command.registrations) {
                        discovery_.registerNode(registration);
                    }
                }
                break;
            case SubscriberCommandType::Unsubscribe:
                if (!local_subscribers_.hasSubscriber(command.value)) {
                    subscriber_->set(zmq::sockopt::unsubscribe, command.value);
                }
                break;
            }
        } catch (const zmq::error_t& e) {
            elog::error("Subscriber socket update error: {}", e.what());
        }
    }
}

void MessageRuntime::receiveMessage()
{
    auto remote_msg = std::make_unique<RemoteMsg>();
    zmq::message_t msg(0);
    try {
        if (!subscriber_->recv(msg)) {
            return;
        }
        remote_msg->topic = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

        if (!subscriber_->recv(msg)) {
            return;
        }
        remote_msg->sender = std::string(reinterpret_cast<char*>(msg.data()), msg.size());

        if (!subscriber_->recv(msg)) {
            return;
        }
        remote_msg->data = std::move(msg);
        msg.rebuild();

        if (!subscriber_->recv(msg)) {
            return;
        }
        remote_msg->msg_type = std::string(reinterpret_cast<char*>(msg.data()), msg.size());
    } catch (const zmq::error_t& e) {
        elog::error("Subscriber socket receive error: {}", e.what());
        return;
    }

    std::lock_guard lock(remote_message_mutex_);
    remote_message_queue_.push_back(std::move(remote_msg));
    if (remote_message_queue_.size() > kMsgQueueLimit) {
        elog::warn("Remote msg queue size > {}, drop msg of topic [{}]", kMsgQueueLimit,
            remote_message_queue_.front()->topic);
        remote_message_queue_.pop_front();
    }
    remote_message_cv_.notify_one();
}

void MessageRuntime::remoteDispatchLoop()
{
    using namespace std::chrono_literals;

    while (!exit_) {
        std::deque<std::unique_ptr<RemoteMsg>> remote_messages;
        {
            std::unique_lock lock(remote_message_mutex_);
            if (remote_message_queue_.empty()) {
                remote_message_cv_.wait_for(lock, 500ms,
                    [this] { return !remote_message_queue_.empty() || exit_; });
            }
            if (exit_) {
                return;
            }
            remote_messages.swap(remote_message_queue_);
        }

        for (auto& message : remote_messages) {
            const auto handler_info = localHandlers(message->topic);
            if (!handler_info.have_local) {
                continue;
            }

            ProtoMsgPtr proto_msg;
            bool invalid_msg = false;
            for (const auto& [node_uuid, handlers] : handler_info.local_handlers) {
                for (const auto& [handler_uuid, handler] : handlers) {
                    if (!handler) {
                        elog::error("Local subscription handler is null");
                        continue;
                    }
                    if (handler->getMsgType() != message->msg_type) {
                        elog::error("Local subscription handler's msg_type does not match remote msg");
                        continue;
                    }
                    if (!proto_msg) {
                        proto_msg = handler->createMsg(message->data.data(), message->data.size(), message->msg_type);
                        if (!proto_msg) {
                            elog::error("Failed to deserialize remote message for topic [{}]", message->topic);
                            invalid_msg = true;
                            break;
                        }
                    }
                    handler->runLocalCallback(proto_msg, MessageInfo(message->topic, message->msg_type));
                }
                if (invalid_msg) {
                    break;
                }
            }
        }
    }
}

HandlerInfo MessageRuntime::localHandlers(const std::string& topic) const
{
    HandlerInfo info;
    info.have_local = local_subscribers_.normal_.getHandlers(topic, info.local_handlers);
    return info;
}

SubscriberInfo MessageRuntime::subscribers(const std::string& topic, const std::string& msg_type) const
{
    SubscriberInfo info;
    info.local_handler_info.have_local = local_subscribers_.normal_.getHandlers(
        topic, info.local_handler_info.local_handlers);
    info.have_remote = remote_subscribers_.hasTopic(topic, msg_type);
    return info;
}

} // namespace trans
} // namespace rf
