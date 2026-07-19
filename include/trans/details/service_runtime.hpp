#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "zmq.hpp"

#include "trans/details/discovery.hpp"
#include "trans/details/handler_storage.hpp"
#include "trans/details/rep_handler.hpp"
#include "trans/details/req_handler.hpp"

namespace rf
{
namespace trans
{
class ServiceRuntime
{
public:
    ServiceRuntime(zmq::context_t& context, SrvDiscovery& discovery, std::string process_uuid,
        std::string host_address);
    ~ServiceRuntime();

    ServiceRuntime(const ServiceRuntime&) = delete;
    ServiceRuntime& operator=(const ServiceRuntime&) = delete;

    bool initialize();
    void start();
    void stop();

    bool advertise(const std::string& topic, const std::string& node_uuid,
        const IRepHandlerPtr& handler, const std::string& req_type,
        const std::string& rep_type, const AdvertiseServiceOptions& options);
    bool unadvertise(const std::string& topic, const std::string& node_uuid);
    bool hasService(const std::string& topic) const;
    void getTopics(std::vector<std::string>& topics) const;
    bool getLocalHandler(const std::string& topic, const std::string& req_type,
        const std::string& rep_type, IRepHandlerPtr& handler) const;
    bool request(const std::string& topic, const std::string& node_uuid,
        const IReqHandlerPtr& handler);

    const std::string& replierAddress() const { return replier_address_; }
    const std::string& replierId() const { return replier_uuid_; }

private:
    void onServiceConnected(const ServicePublisherInfo& pub);
    void onServiceDisconnected(const ServicePublisherInfo& pub);
    void receiveLoop();
    void serviceHandleLoop();
    void receiveRequest();
    void receiveResponse();
    void handleRequests(const std::deque<std::unique_ptr<RemoteRequest>>& requests);
    void handleResponses(const std::deque<std::unique_ptr<RemoteResponse>>& responses);
    void sendPendingRequests(const std::string& topic, const std::string& req_type,
        const std::string& rep_type);
    bool getPublishers(const std::string& topic, AddressMap<ServicePublisherInfo>& publishers) const;

private:
    SrvDiscovery& discovery_;
    const std::string process_uuid_;
    const std::string host_address_;
    const std::string replier_uuid_{generateUuidV4()};

    std::unique_ptr<zmq::socket_t> requester_;
    std::unique_ptr<zmq::socket_t> replier_;
    std::string replier_address_;

    mutable std::recursive_mutex mutex_;
    std::unordered_set<std::string> requester_connections_;
    HandlerStorage<IRepHandler> response_handlers_;
    HandlerStorage<IReqHandler> request_handlers_;

    std::atomic_bool exit_{false};
    bool initialized_{false};
    bool started_{false};
    std::thread receive_thread_;
    std::thread handle_thread_;
    std::deque<std::unique_ptr<RemoteRequest>> request_queue_;
    std::deque<std::unique_ptr<RemoteResponse>> response_queue_;
    mutable std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
};

} // namespace trans
} // namespace rf
