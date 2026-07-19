#include <random>

#include "elog/elog.h"
#include "trans/details/discovery.hpp"

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
    const std::string addr1 = "tcp://127.0.0.1:12345";
    const std::string ctrl1 = "tcp://127.0.0.1:12346";

    auto puuid1 = generate_uuid_v4();
    rf::trans::Discovery<rf::trans::MessagePublisherInfo> dis(puuid1, "239.255.0.7", 10317);
    dis.start();

    rf::trans::MessagePublisherInfo pub("topic", addr1, ctrl1, puuid1, "nuuid", "t", rf::trans::AdvertiseMessageOptions{});

    dis.advertise(pub);

    rf::trans::Discovery<rf::trans::MessagePublisherInfo> dis2(generate_uuid_v4(), "239.255.0.7", 10317);
    dis2.start();
    std::this_thread::sleep_for(100ms);

    dis2.setConnectionCb([](const rf::trans::MessagePublisherInfo& pub) {
        elog::info("connection cb");
    });

    if (dis2.discover("topic")) {
        elog::info("discover success.");
    };

    std::this_thread::sleep_for(100s);
    return 0;
}
