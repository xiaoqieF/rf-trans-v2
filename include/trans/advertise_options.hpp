#pragma once

#include <iostream>

#include "trans/helpers.hpp"

namespace rf
{
namespace trans
{
enum class Scope
{
    PROCESS = 0,
    HOST,
    ALL
};

class AdvertiseOptions
{
public:
    AdvertiseOptions() = default;
    virtual ~AdvertiseOptions() = default;

    bool operator==(const AdvertiseOptions& rhs) const {
        return scope_ == rhs.scope_;
    }
    bool operator!=(const AdvertiseOptions& rhs) const {
        return !(*this == rhs);
    }

    Scope getScope() const { return scope_; }
    void setScope(Scope scope) { scope_ = scope; }

    friend std::ostream& operator<<(std::ostream& out, const AdvertiseOptions& other);

private:
    Scope scope_{Scope::ALL};
};

class AdvertiseMessageOptions : public AdvertiseOptions
{
public:
    AdvertiseMessageOptions() = default;
    virtual ~AdvertiseMessageOptions() = default;

    bool operator==(const AdvertiseMessageOptions& rhs) const {
        return AdvertiseOptions::operator==(rhs) && msgs_per_sec_ == rhs.msgs_per_sec_;
    }

    bool operator!=(const AdvertiseMessageOptions& rhs) const {
        return !(*this == rhs);
    }

    bool throttled() const { return msgs_per_sec_ != kUnthrottled; }
    uint64_t getMsgsPerSec() const { return msgs_per_sec_; }
    void setMsgsPerSec(uint64_t msg_per_sec) { msgs_per_sec_ = msg_per_sec; }

    friend std::ostream& operator<<(std::ostream& out, const AdvertiseMessageOptions& other);

private:
    uint64_t msgs_per_sec_{kUnthrottled};
};

class AdvertiseServiceOptions : public AdvertiseOptions
{
public:
    AdvertiseServiceOptions() = default;
    virtual ~AdvertiseServiceOptions() = default;

    bool operator==(const AdvertiseServiceOptions& rhs) const {
        return AdvertiseOptions::operator==(rhs);
    }

    bool operator!=(const AdvertiseServiceOptions& rhs) const {
        return !(*this == rhs);
    }

    friend std::ostream& operator<<(std::ostream& out, const AdvertiseServiceOptions& other);

};

inline std::ostream& operator<<(std::ostream& out, const AdvertiseOptions& other)
{
    out << "Advertise options:\n" << "\tScope: ";
    if (other.getScope() == Scope::PROCESS) {
        out << "Process" << std::endl;
    } else if (other.getScope() == Scope::HOST) {
        out << "Host" << std::endl;
    } else {
        out << "All" << std::endl;
    }
    return out;
}

inline std::ostream& operator<<(std::ostream& out, const AdvertiseMessageOptions& other)
{
    out << static_cast<AdvertiseOptions>(other);
    if (other.throttled()) {
        out << "\tThrottled? Yes" << std::endl;
        out << "\tRate: " << other.msgs_per_sec_ << " msgs/sec" << std::endl;
    } else {
        out << "\tThrottled? No" << std::endl;
    }

    return out;
}

inline std::ostream& operator<<(std::ostream& out, const AdvertiseServiceOptions& other)
{
    out << static_cast<AdvertiseOptions>(other);
    return out;
}

} // namespace trans
} // namespace rf