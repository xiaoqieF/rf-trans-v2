#include <random>

#include "elog/elog.h"
#include "trans/discovery.hpp"

using namespace std::chrono_literals;

std::string generate_uuid_v4() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);
    std::uniform_int_distribution<> dis2(8, 11); // for variant

    std::stringstream ss;
    ss << std::hex;

    for (int i = 0; i < 8; ++i) ss << dis(gen);
    ss << "-";
    for (int i = 0; i < 4; ++i) ss << dis(gen);
    ss << "-4"; // version 4
    for (int i = 0; i < 3; ++i) ss << dis(gen);
    ss << "-";
    ss << dis2(gen); // variant
    for (int i = 0; i < 3; ++i) ss << dis(gen);
    ss << "-";
    for (int i = 0; i < 12; ++i) ss << dis(gen);

    return ss.str();
}

int main()
{
    rf::trans::Discovery<rf::trans::MessagePublisher> dis(generate_uuid_v4(), "239.255.0.7", 10317);
    dis.start();
    std::this_thread::sleep_for(100s);
    return 0;
}