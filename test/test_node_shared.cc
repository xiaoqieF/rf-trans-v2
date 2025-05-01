#include "msgs/header.pb.h"
#include "trans/node.hpp"

using namespace std::chrono_literals;

int main()
{
    auto& node_shared = rf::trans::NodeShared::getInstance();

    // std::this_thread::sleep_for(100s);

    rf::msgs::Header header;

    return 0;
}