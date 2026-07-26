# rf_trans

基于 C++17、Protocol Buffers 和 ZeroMQ 的轻量级进程间通信库。消息和服务以 topic 组织,通过 UDP 组播自动发现同一网络中的端点,在本机或远端透明分发数据。

## 核心能力

- **发布订阅**:`Node::advertise<MessageT>()` / `Node::subscribe<MessageT>()`,消息类型继承 `google::protobuf::Message`。本地投递走内部队列,远端走 ZeroMQ PUB/SUB。
- **服务请求响应**:`advertise<RequestT, ReplyT>()` 注册服务,支持异步回调和带超时的同步调用,也支持仅请求/仅响应的服务形式。
- **自动发现**:UDP 组播发现消息与服务端点(默认端口 `10319` / `10320`),发现后自动建立 ZeroMQ 连接。
- **可见范围与限速**:发布可选 `PROCESS` / `HOST` / `ALL` 范围,支持 `setMsgsPerSec()` 限速。
- **CLI 诊断工具** `rf-trans`:只读端点查询、事件监听和原始消息抓包,不注册业务订阅或改变远端状态。

## 依赖

Linux、CMake >= 3.22、支持 C++17 的编译器、Protobuf(库 + `protoc`)、ZeroMQ(`libzmq` + `cppzmq`)、`pkg-config`。

```bash
sudo apt install build-essential cmake pkg-config protobuf-compiler libprotobuf-dev libzmq3-dev cppzmq-dev
```

## 构建

```bash
cmake -S . -B build -DBUILD_TESTING=OFF
cmake --build build -j
```

启用单元测试(通过 `FetchContent` 下载 GoogleTest,需要网络):

```bash
cmake -S . -B build -DBUILD_TESTING=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

安装到指定前缀并供其他 CMake 项目使用:

```bash
cmake -S . -B build -DBUILD_TESTING=OFF -DCMAKE_INSTALL_PREFIX=$HOME/.local
cmake --build build -j
cmake --install build
```

```cmake
find_package(rf_trans CONFIG REQUIRED)
target_link_libraries(my_application PRIVATE rf_trans::rf_trans)
```

## 使用示例

消息定义(`msgs/example.proto`):

```proto
message ExampleMsg {
    string name = 1;
    int32 age = 2;
}
```

### 发布订阅

```cpp
#include "msgs/example.pb.h"
#include "trans/node.hpp"

using rf::trans::Node;

int main()
{
    Node publisher_node;
    auto publisher = publisher_node.advertise<rf::msgs::ExampleMsg>("/greeting");

    Node subscriber_node;
    subscriber_node.subscribe<rf::msgs::ExampleMsg>("/greeting",
        [](const std::shared_ptr<const rf::msgs::ExampleMsg>& message) {
            elog::info("received: {}", message->ShortDebugString());
        });

    rf::msgs::ExampleMsg message;
    message.set_name("Nancy");
    message.set_age(12);
    publisher.publish(message);
}
```

`Publisher::hasConnections()` 可判断是否有订阅者;高频消息可用 `AdvertiseMessageOptions::setMsgsPerSec()` 限速。

### 服务调用

```cpp
// Server
Node server;
server.advertise<rf::msgs::ExampleMsg, rf::msgs::ExampleMsg>("/echo",
    [](const std::shared_ptr<const rf::msgs::ExampleMsg>& request,
       std::shared_ptr<rf::msgs::ExampleMsg> reply) {
        reply->CopyFrom(*request);
        return true;
    });

// Client
Node client;
auto request = std::make_shared<rf::msgs::ExampleMsg>();
request->set_name("hello");
client.request<rf::msgs::ExampleMsg, rf::msgs::ExampleMsg>("/echo", request,
    [](const std::shared_ptr<const rf::msgs::ExampleMsg>& reply, bool success) {
        if (success) elog::info("reply: {}", reply->ShortDebugString());
    });
```

`waitForService(topic, timeout)` 可在请求前等待服务被发现。完整可运行示例见 `test/examples/`:

```bash
./build/test/examples/example_pub
./build/test/examples/example_sub
```

## CLI 工具示例

`BUILD_CLI=ON`(默认)时构建 `rf-trans`,只读的端点观测与网络诊断工具:

```bash
# 等待发现窗口后列出当前消息端点
./build/tools/rf-trans topic list

# 查看一个 topic 的类型、地址、scope 和 UUID
./build/tools/rf-trans topic info /examples/multi_topic/temperature

# 监听 topic 实时速率与原始 protobuf 字段(类似 protoc --decode_raw)
./build/tools/rf-trans topic sniff /examples/multi_topic/temperature

# 查看服务端点
./build/tools/rf-trans service list --output json

# 持续输出端点上线/下线事件,适合日志采集
./build/tools/rf-trans watch topics --output jsonl

# 检查 CLI 选择的网卡、发现组、端口和当前可见端点数
./build/tools/rf-trans doctor
```

所有查询命令支持 `--output table|json|jsonl` 和 `--prefix <topic-prefix>`;`--host-ip`、`--discovery-group`、`--message-port`、`--service-port` 可覆盖发现网络设置。`topic info` / `service info` 找不到指定 topic 返回退出码 `3`,参数错误返回 `2`,发现初始化失败返回 `4`。

## 性能基准

位于 `test/benchmark/`,基于 Google Benchmark:

```bash
cmake -S . -B build-benchmark -DBUILD_TESTING=OFF -DBUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build-benchmark -j
./build-benchmark/test/benchmark/benchmark_pubsub_service \
  --benchmark_repetitions=5 --benchmark_report_aggregates_only=true
```

### 测试环境

- CPU: AMD Ryzen 5 9600X(12 线程),启用 CPU 频率调节(结果存在噪声)
- OS: Linux 6.17,x86_64
- 构建类型: Release(跨进程用例额外经过一次预热通信)

### 实测结果(manual_time,5 次重复取中位数)

进程内(隔离数据路径,不含网络往返):

| 场景 | Payload | 延迟/耗时 | 吞吐 |
|---|---|---|---|
| 端到端延迟 BM_PubSubEndToEndLatency | 32 B | 3.2 µs | — |
| 端到端延迟 BM_PubSubEndToEndLatency | 1 KiB | 3.4 µs | 287 MiB/s |
| 端到端延迟 BM_PubSubEndToEndLatency | 64 KiB | 3.6 µs | 16.9 GiB/s |
| 持续吞吐 BM_PubSubSustainedThroughput(128 条/批) | 32 B | 68 µs/批 | 1.88 M msg/s |
| 持续吞吐 BM_PubSubSustainedThroughput(128 条/批) | 1 KiB | 87 µs/批 | 1.47 M msg/s |
| 持续吞吐 BM_PubSubSustainedThroughput(128 条/批) | 64 KiB | 232 µs/批 | 550 k msg/s |
| 服务本地往返 BM_ServiceLocalRoundTrip | 32 B | 165 ns | 6.06 M req/s |
| 服务本地往返 BM_ServiceLocalRoundTrip | 64 KiB | 1.2 µs | 828 k req/s |

跨进程(经 UDP 组播发现 + ZeroMQ TCP 连接):

| 场景 | Payload | 延迟 |
|---|---|---|
| 端到端延迟 BM_PubSubInterprocessEndToEndLatency | 32 B | 11.0 µs |
| 端到端延迟 BM_PubSubInterprocessEndToEndLatency | 1 KiB | 10.9 µs |
| 端到端延迟 BM_PubSubInterprocessEndToEndLatency | 64 KiB | 23.0 µs |
| 服务跨进程往返 BM_ServiceInterprocessRoundTrip | 32 B | 32.7 µs |
| 服务跨进程往返 BM_ServiceInterprocessRoundTrip | 1 KiB | 35.8 µs |
| 服务跨进程往返 BM_ServiceInterprocessRoundTrip | 64 KiB | 79.7 µs |

跨进程用例波动较大(64 KiB 档 `manual_time_cv` 可达 10%+),数值仅供参考,具体表现受机器负载和 CPU 频率调节影响。

## 网络注意事项

- 依赖 IPv4 UDP 组播,跨主机部署需确保网络允许组播流量及动态 TCP 端口。
- 多网卡、VPN 或容器环境需设置 `RF_HOST_IP` 指定可达的本机地址,例如 `RF_HOST_IP=192.168.1.10 ./build/test/examples/example_pub`。
- `Scope::PROCESS` 不跨进程广播,`Scope::HOST` 仅同机发现,`Scope::ALL` 允许网络内发现。
- topic 字符串和 Protobuf 全限定类型名共同参与匹配,同名 topic 类型不一致时不会匹配。

## 目录说明

```text
include/trans/         对外 C++ 接口
include/trans/details/ 内部运行时、发现机制和模板实现
src/                    Node、Publisher、TopicSniffer 等实现
msgs/                   Discovery 协议与示例 Protobuf 消息
tools/                  CLI 工具 rf-trans
test/examples/          可执行通信示例
test/benchmark/         发布订阅与服务基准
test/ut/                GoogleTest 单元测试
```
