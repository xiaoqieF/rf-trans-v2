#include "msgs/header.pb.h"
#include "msgs/example.pb.h"
#include "trans/node.hpp"

using namespace std::chrono_literals;
using namespace rf::trans;

constexpr int kSubNodeNum = 10;

int main(int argc, char** argv)
{
    // Ensure node not to be destroyed.
    std::array<std::unique_ptr<Node>, kSubNodeNum> nodes;
    for (int i = 0; i < kSubNodeNum; ++ i) {
        nodes[i] = std::make_unique<Node>();
        nodes[i]->subscribe<rf::msgs::ExampleMsg>("/test_topic", [i](const std::shared_ptr<const rf::msgs::ExampleMsg> msg) {
            elog::info("sub[{}], recv msg: {}", i, msg->DebugString());
        });
    }

    // block here.
    while (true) {
        std::this_thread::sleep_for(1s);
    }

    return 0;
}