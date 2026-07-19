#pragma once

#include <map>
#include <memory>
#include <string>

#include "elog/elog.h"

#include "trans/details/trans_types.hpp"

namespace rf
{
namespace trans
{
template<typename T>
class HandlerStorage
{
// handler_uuid --> handler
using RequesterHandlerMap = std::map<std::string, std::shared_ptr<T>>;
// node_uuid --> RequesterHandlerMap
using NodeHandlerMap = std::map<std::string, RequesterHandlerMap>;
// topic --> NodeHandlerMap
using TopicHandlerMap = std::map<std::string, NodeHandlerMap>;

public:
    HandlerStorage() = default;
    virtual ~HandlerStorage() = default;

    bool hasTopic(const std::string& topic) const;
    bool getHandlers(const std::string& topic, NodeHandlerMap& handlers) const;
    // Get the first handler for a topic that matches a specific pair
    // of request/response types.
    bool getFirstHandler(const std::string& topic, const std::string& req_type_name,
        const std::string& rep_type_name, std::shared_ptr<T>& handler) const;
    // Get the first handler for a topic that matches a specific
    // message type.
    bool getFirstHandler(const std::string& topic, const std::string& msg_type, std::shared_ptr<T>& handler) const;
    bool getHandler(const std::string& topic, const std::string& node_uuid,
        const std::string& handler_uuid, std::shared_ptr<T>& handler) const;
    TopicHandlerMap& getAllHandlers()
    {
        std::lock_guard lk(mutex_);
        return data_;
    }

    void addHandler(const std::string& topic, const std::string& node_uuid, const std::shared_ptr<T>& handler);

    bool hasHandlersForTopic(const std::string& topic) const;
    bool hasHandlersForNode(const std::string& topic, const std::string node_uuid) const;

    bool removeHandler(const std::string& topic, const std::string& node_uuid, const std::string& req_uuid);
    bool removeHandlersForNode(const std::string& topic, const std::string& node_uuid);

private:
    mutable std::mutex mutex_;
    TopicHandlerMap data_;
};

template<typename T>
bool HandlerStorage<T>::hasTopic(const std::string& topic) const
{
    std::lock_guard lk(mutex_);
    return data_.find(topic) != data_.end();
}

template<typename T>
void HandlerStorage<T>::addHandler(const std::string& topic, const std::string& node_uuid, const std::shared_ptr<T>& handler)
{
    std::lock_guard lk(mutex_);
    data_[topic][node_uuid].emplace(handler->getHandlerUuid(), handler);
}

template<typename T>
bool HandlerStorage<T>::getHandlers(const std::string& topic, NodeHandlerMap& handlers) const
{
    std::lock_guard lk(mutex_);
    if (data_.find(topic) == data_.end()) {
        return false;
    }

    handlers = data_.at(topic);
    return true;
}

template<typename T>
bool HandlerStorage<T>::getFirstHandler(const std::string& topic, const std::string& req_type_name,
    const std::string& rep_type_name, std::shared_ptr<T>& handler) const
{
    std::lock_guard lk(mutex_);
    if (data_.find(topic) == data_.end()) {
        return false;
    }

    const auto& mp = data_.at(topic);
    for (const auto& [node_uuid, node_map]: mp) {
        for (const auto& [handler_uuid, handler1]: node_map) {
            if (req_type_name == handler1->getReqTypeName() &&
                rep_type_name == handler1->getRepTypeName()) {
                handler = handler1;
                return true;
            }
        }
    }
    return false;
}

template<typename T>
bool HandlerStorage<T>::getFirstHandler(const std::string& topic, const std::string& msg_type, std::shared_ptr<T>& handler) const
{
    std::lock_guard lk(mutex_);
    if (data_.find(topic) == data_.end()) {
        return false;
    }

    const auto& mp = data_.at(topic);
    for (const auto& [node_uuid, node_map]: mp) {
        for (const auto& [handler_uuid, handler1]: node_map) {
            if (msg_type == handler1->getMsgType()) {
                handler = handler1;
                return true;
            }
        }
    }

    return false;
}

template<typename T>
bool HandlerStorage<T>::getHandler(const std::string& topic, const std::string& node_uuid,
    const std::string& handler_uuid, std::shared_ptr<T>& handler) const
{
    std::lock_guard lk(mutex_);
    if (data_.find(topic) == data_.end()) {
        return false;
    }

    const auto& mp = data_.at(topic);
    if (mp.find(node_uuid) == mp.end()) {
        return false;
    }

    if (mp.at(node_uuid).find(handler_uuid) == mp.at(node_uuid).end()) {
        return false;
    }
    handler = mp.at(node_uuid).at(handler_uuid);
    return true;
}

template<typename T>
bool HandlerStorage<T>::hasHandlersForTopic(const std::string& topic) const
{
    std::lock_guard lk(mutex_);
    if (data_.find(topic) == data_.end()) {
        return false;
    }
    return !data_.at(topic).empty();
}

template<typename T>
bool HandlerStorage<T>::hasHandlersForNode(const std::string& topic, const std::string node_uuid) const
{
    std::lock_guard lk(mutex_);
    if (data_.find(topic) == data_.end()) {
        return false;
    }

    return data_.at(topic).find(node_uuid) != data_.at(topic).end();
}

template<typename T>
bool HandlerStorage<T>::removeHandler(const std::string& topic, const std::string& node_uuid, const std::string& req_uuid)
{
    std::lock_guard lk(mutex_);
    std::size_t count = 0;
    if (data_.find(topic) != data_.end()) {
        if (data_[topic].find(node_uuid) != data_[topic].end()) {
            count = data_[topic][node_uuid].erase(req_uuid);

            if (data_[topic][node_uuid].empty()) {
                data_[topic].erase(node_uuid);
            }

            if (data_[topic].empty()) {
                data_[topic].erase(topic);
            }
        }
    }
    return count > 0;
}

template<typename T>
bool HandlerStorage<T>::removeHandlersForNode(const std::string& topic, const std::string& node_uuid)
{
    std::lock_guard lk(mutex_);
    std::size_t count = 0;
    if (data_.find(topic) != data_.end()) {
        count = data_[topic].erase(node_uuid);

        if (data_[topic].empty()) {
            data_.erase(topic);
        }
    }
    return count > 0;
}


} // namespace trans
} // namespace rf
