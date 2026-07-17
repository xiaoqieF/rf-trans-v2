#pragma once

#include <algorithm>
#include <map>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

#include "elog/elog.h"
#include "trans/trans_types.hpp"
#include "trans/publisher_info.hpp"

namespace rf
{
namespace trans
{
template<typename Pub>
class TopicStorage
{
public:
    /// process_id --> Publisher_list
    using ProcessMap = std::map<std::string, std::vector<Pub>>;
    /// topic --> processMap
    using TopicMap = std::map<std::string, ProcessMap>;

    TopicStorage() = default;
    virtual ~TopicStorage() = default;

    bool addPublisher(const Pub& publisher);
    bool hasTopic(const std::string& topic) const;
    bool hasTopic(const std::string& topic, const std::string& msg_type) const;
    bool hasAnyPublishers(const std::string& topic, const std::string& p_uuid) const;
    bool hasPublisher(const std::string& addr) const;
    bool getPublisher(const std::string& topic, const std::string& p_uuid,
                      const std::string& n_uuid, Pub& publisher) const;
    bool getPublishers(const std::string& topic, ProcessMap& info) const;
    bool delPublishersByNode(const std::string& topic, const std::string& p_uuid, const std::string& n_uuid);
    bool delPublishersByProc(const std::string& p_uuid);
    void getPublishersByProc(const std::string& p_uuid, std::map<std::string, std::vector<Pub>>& pubs) const;
    void getPublishersByNode(const std::string& p_uuid, const std::string& n_uuid, std::vector<Pub>& pubs) const;

    std::vector<std::string> getTopicList() const;
    void print(std::ostream& out) const;
    void clear()
    {
        std::lock_guard lk(mutex_);
        data_.clear();
    }

private:
    mutable std::recursive_mutex mutex_;
    TopicMap data_;
};

template<typename Pub>
bool TopicStorage<Pub>::addPublisher(const Pub& publisher)
{
    std::lock_guard lk(mutex_);
    auto& mp = data_[publisher.getTopic()];
    if (mp.find(publisher.getProcessUuid()) != mp.end()) {
        auto& pub_list = mp[publisher.getProcessUuid()];
        auto found = std::find_if(pub_list.begin(), pub_list.end(), [&publisher] (const Pub& pub) {
            return pub.getAddr() == publisher.getAddr()
                && pub.getNodeUuid() == publisher.getNodeUuid();
        });

        // The publisher was already exist.
        if (found != pub_list.end()) {
            return false;
        }
    }
    mp[publisher.getProcessUuid()].push_back(publisher);
    return true;
}

template<typename Pub>
bool TopicStorage<Pub>::hasTopic(const std::string& topic) const
{
    std::lock_guard lk(mutex_);
    return data_.find(topic) != data_.end();
}

template<typename Pub>
bool TopicStorage<Pub>::hasTopic(const std::string& topic, const std::string& msg_type) const
{
    std::lock_guard lk(mutex_);
    if (!hasTopic(topic)) {
        return false;
    }
    auto& mp = data_.at(topic);
    for (const auto& [pid, pub_list]: mp) {
        auto found = std::find_if(pub_list.begin(), pub_list.end(), [&msg_type] (const Pub& pub) {
            return pub.getMsgType() == msg_type || pub.getMsgType() == kGenericMessageType;
        });

        if (found != pub_list.end()) {
            return true;
        }
    }
    return false;
}

template<typename Pub>
bool TopicStorage<Pub>::hasAnyPublishers(const std::string& topic, const std::string& p_uuid) const
{
    std::lock_guard lk(mutex_);
    if (!hasTopic(topic)) {
        return false;
    }

    auto& mp = data_.at(topic);

    return mp.find(p_uuid) != mp.end();
}

template<typename Pub>
bool TopicStorage<Pub>::hasPublisher(const std::string& addr) const
{
    std::lock_guard lk(mutex_);
    for (const auto& topic : data_) {
        for (const auto& proc : topic.second) {
            for (const auto& pub : proc.second) {
                if (pub.getAddr() == addr) {
                    return true;
                }
            }
        }
    }
    return false;
}

template<typename Pub>
bool TopicStorage<Pub>::getPublisher(const std::string& topic, const std::string& p_uuid,
                    const std::string& n_uuid, Pub& publisher) const
{
    std::lock_guard lk(mutex_);
    if (data_.find(topic) == data_.end()) {
        return false;
    }

    auto& mp = data_.at(topic);
    if (mp.find(p_uuid) == mp.end()) {
        return false;
    }

    auto& pub_list = mp.at(p_uuid);
    auto found = std::find_if(pub_list.begin(), pub_list.end(), [&n_uuid] (const Pub& pub) {
        return pub.getNodeUuid() == n_uuid;
    });

    if (found != pub_list.end()) {
        publisher = *found;
        return true;
    }
    return false;
}

template<typename Pub>
bool TopicStorage<Pub>::getPublishers(const std::string& topic, ProcessMap& info) const
{
    std::lock_guard lk(mutex_);
    if (!hasTopic(topic)) {
        return false;
    }
    info = data_.at(topic);
    return true;
}

template<typename Pub>
bool TopicStorage<Pub>::delPublishersByNode(const std::string& topic, const std::string& p_uuid, const std::string& n_uuid)
{
    std::lock_guard lk(mutex_);
    std::size_t count = 0;
    if (data_.find(topic) == data_.end()) {
        return false;
    }

    auto& mp = data_[topic];
    if (mp.find(p_uuid) == mp.end()) {
        return false;
    }

    auto& pub_list = mp[p_uuid];
    auto pri_size = pub_list.size();
    pub_list.erase(std::remove_if(pub_list.begin(), pub_list.end(), [&n_uuid] (const Pub& pub) {
        return pub.getNodeUuid() == n_uuid;
    }), pub_list.end());

    count = pri_size - pub_list.size();
    if (pub_list.empty()) {
        mp.erase(p_uuid);
    }
    if (mp.empty()) {
        data_.erase(topic);
    }
    return count > 0;
}

template<typename Pub>
bool TopicStorage<Pub>::delPublishersByProc(const std::string& p_uuid)
{
    std::lock_guard lk(mutex_);
    std::size_t count = 0;
    for (auto it = data_.begin(); it != data_.end(); ) {
        auto& mp = it->second;
        count += mp.erase(p_uuid);
        if (mp.empty()) {
            data_.erase(it ++);
        } else {
            ++ it;
        }
    }
    return count > 0;
}

template<typename Pub>
void TopicStorage<Pub>::getPublishersByProc(const std::string& p_uuid, std::map<std::string, std::vector<Pub>>& pubs) const
{
    std::lock_guard lk(mutex_);
    pubs.clear();

    for (const auto& topic : data_) {
        auto& mp = topic.second;

        if (mp.find(p_uuid) != mp.end()) {
            auto& pub_list = mp.at(p_uuid);
            for (const auto& pub : pub_list) {
                pubs[pub.getNodeUuid()].push_back(pub);
            }
        }
    }
}

template<typename Pub>
void TopicStorage<Pub>::getPublishersByNode(const std::string& p_uuid, const std::string& n_uuid, std::vector<Pub>& pubs) const
{
    std::lock_guard lk(mutex_);
    pubs.clear();

    for (const auto& topic : data_) {
        auto& mp = topic.second;

        if (mp.find(p_uuid) != mp.end()) {
            auto& pub_list = mp.at(p_uuid);
            for (const auto& pub : pub_list) {
                if (pub.getNodeUuid() == n_uuid) {
                    pubs.push_back(pub);
                }
            }
        }
    }
}

template<typename Pub>
std::vector<std::string> TopicStorage<Pub>::getTopicList() const
{
    std::lock_guard lk(mutex_);
    std::vector<std::string> topics;
    for (const auto& topic : data_) {
        topics.push_back(topic.first);
    }
    return topics;
}

template<typename Pub>
void TopicStorage<Pub>::print(std::ostream& out) const
{
    std::lock_guard lk(mutex_);
    out << "---\n";
    for (auto const &topic : data_)
    {
      out << "[" << topic.first << "]\n";
      auto &m = topic.second;
      for (auto const &proc : m)
      {
        out << "\tProc. UUID: " << proc.first << "\n";
        auto &v = proc.second;
        for (auto const &publisher : v)
        {
          out << publisher;
        }
      }
    }
}

} // namespace trans
} // namespace rf
