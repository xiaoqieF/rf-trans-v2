#include <atomic>
#include <cerrno>
#include <csignal>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <spawn.h>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

#include <benchmark/benchmark.h>

#include "msgs/example.pb.h"
#include "trans/node.hpp"

extern char** environ;

namespace
{
using namespace std::chrono_literals;
using rf::msgs::ExampleMsg;
using rf::trans::AdvertiseMessageOptions;
using rf::trans::AdvertiseServiceOptions;
using rf::trans::Node;
using rf::trans::Publisher;
using rf::trans::Scope;

constexpr auto kCallbackTimeout = 5s;
constexpr int kPublishBatchSize = 128;
constexpr auto kProcessExitTimeout = 500ms;

class InterprocessBenchmarkProcess
{
public:
    InterprocessBenchmarkProcess(const char* executable, std::string topic, std::string payload_size = {})
        : executable_(executable), topic_(std::move(topic)), payload_size_(std::move(payload_size)) {}

    ~InterprocessBenchmarkProcess() { stop(); }

    InterprocessBenchmarkProcess(const InterprocessBenchmarkProcess&) = delete;
    InterprocessBenchmarkProcess& operator=(const InterprocessBenchmarkProcess&) = delete;

    bool start()
    {
        int ready_pipe[2];
        if (pipe(ready_pipe) != 0) {
            error_ = "failed to create benchmark child status pipe";
            return false;
        }

        const std::string ready_fd = std::to_string(ready_pipe[1]);
        std::vector<char*> arguments{
            const_cast<char*>(executable_), const_cast<char*>("--topic"), const_cast<char*>(topic_.c_str()),
            const_cast<char*>("--ready-fd"), const_cast<char*>(ready_fd.c_str())
        };
        if (!payload_size_.empty()) {
            arguments.push_back(const_cast<char*>("--payload-size"));
            arguments.push_back(const_cast<char*>(payload_size_.c_str()));
        }
        arguments.push_back(nullptr);

        posix_spawn_file_actions_t file_actions;
        int spawn_result = posix_spawn_file_actions_init(&file_actions);
        if (spawn_result == 0) {
            spawn_result = posix_spawn_file_actions_addclose(&file_actions, ready_pipe[0]);
            pid_t child = -1;
            if (spawn_result == 0) {
                spawn_result = posix_spawn(&child, executable_, &file_actions, nullptr,
                    arguments.data(), environ);
            }
            posix_spawn_file_actions_destroy(&file_actions);
            if (spawn_result == 0) {
                pid_ = child;
            }
        }
        if (spawn_result != 0) {
            close(ready_pipe[0]);
            close(ready_pipe[1]);
            error_ = "failed to start benchmark child process";
            return false;
        }

        close(ready_pipe[1]);
        ready_fd_ = ready_pipe[0];

        char status = 0;
        if (!waitForStatus(status)) {
            stop();
            return false;
        }
        if (status != '1') {
            error_ = "benchmark child process failed to initialize";
            stop();
            return false;
        }
        return true;
    }

    bool waitForStatus(char& status,
        const std::chrono::milliseconds timeout = std::chrono::duration_cast<std::chrono::milliseconds>(kCallbackTimeout))
    {
        pollfd status_poll{ready_fd_, POLLIN, 0};
        const int poll_result = poll(&status_poll, 1, static_cast<int>(timeout.count()));
        if (poll_result <= 0 || !(status_poll.revents & POLLIN)) {
            error_ = poll_result == 0 ? "timed out waiting for benchmark child process"
                                    : "failed while waiting for benchmark child process";
            return false;
        }

        ssize_t bytes_read;
        do {
            bytes_read = read(ready_fd_, &status, sizeof(status));
        } while (bytes_read < 0 && errno == EINTR);
        if (bytes_read != sizeof(status)) {
            error_ = "benchmark child process closed its status pipe";
            return false;
        }
        return true;
    }

    const std::string& error() const { return error_; }

private:
    void stop() noexcept
    {
        if (ready_fd_ >= 0) {
            close(ready_fd_);
            ready_fd_ = -1;
        }
        if (pid_ <= 0) {
            return;
        }

        kill(pid_, SIGTERM);
        const auto deadline = std::chrono::steady_clock::now() + kProcessExitTimeout;
        while (std::chrono::steady_clock::now() < deadline) {
            const pid_t result = waitpid(pid_, nullptr, WNOHANG);
            if (result == pid_ || (result < 0 && errno == ECHILD)) {
                pid_ = -1;
                return;
            }
            std::this_thread::sleep_for(10ms);
        }

        kill(pid_, SIGKILL);
        while (waitpid(pid_, nullptr, 0) < 0 && errno == EINTR) {
        }
        pid_ = -1;
    }

    const char* const executable_;
    const std::string topic_;
    const std::string payload_size_;
    std::string error_;
    pid_t pid_{-1};
    int ready_fd_{-1};
};

std::atomic<uint64_t> next_topic_id{0};

std::string makeTopic(const char* benchmark_name)
{
    return std::string{"/benchmark/"} + benchmark_name + "/" + std::to_string(next_topic_id++);
}

class PubSubBenchmark
{
public:
    explicit PubSubBenchmark(const size_t payload_size)
        : topic_(makeTopic("pubsub")), state_(std::make_shared<CallbackState>(payload_size))
    {
        if (!subscriber_.subscribe<ExampleMsg>(topic_, [state = state_](const std::shared_ptr<const ExampleMsg>& message) {
                std::lock_guard lock(state->callback_mutex);
                const uint64_t sequence = state->received.fetch_add(1, std::memory_order_acq_rel) + 1;
                if (!message || message->name() != state->payload ||
                    message->age() != static_cast<int>(sequence)) {
                    state->callback_valid.store(false, std::memory_order_release);
                }
                state->callback_cv.notify_one();
            })) {
            throw std::runtime_error("failed to subscribe benchmark receiver");
        }

        AdvertiseMessageOptions options;
        options.setScope(Scope::PROCESS);
        publisher_ = std::make_unique<Publisher>(publisher_node_.advertise<ExampleMsg>(topic_, options));
        if (!publisher_->valid()) {
            throw std::runtime_error("failed to advertise benchmark publisher");
        }
        if (!waitForConnections()) {
            throw std::runtime_error("benchmark publisher did not observe its local subscriber");
        }
    }

    ~PubSubBenchmark()
    {
        subscriber_.unsubscribe(topic_);
        publisher_node_.unadvertise(topic_);
    }

    std::unique_ptr<ExampleMsg> makeMessage(const int sequence) const
    {
        auto message = std::make_unique<ExampleMsg>();
        message->set_name(state_->payload);
        message->set_age(sequence);
        return message;
    }

    bool publish(std::unique_ptr<ExampleMsg> message)
    {
        return publisher_->publish(std::move(message));
    }

    uint64_t received() const
    {
        return state_->received.load(std::memory_order_acquire);
    }

    bool waitFor(const uint64_t expected)
    {
        std::unique_lock lock(state_->callback_mutex);
        return state_->callback_cv.wait_for(lock, kCallbackTimeout, [state = state_, expected] {
            return state->received.load(std::memory_order_acquire) >= expected;
        });
    }

    bool callbackValid() const { return state_->callback_valid.load(std::memory_order_acquire); }
    size_t payloadSize() const { return state_->payload.size(); }

private:
    struct CallbackState
    {
        explicit CallbackState(const size_t payload_size) : payload(payload_size, 'x') {}

        const std::string payload;
        std::atomic<uint64_t> received{0};
        std::atomic<bool> callback_valid{true};
        std::mutex callback_mutex;
        std::condition_variable callback_cv;
    };

    bool waitForConnections() const
    {
        const auto deadline = std::chrono::steady_clock::now() + kCallbackTimeout;
        while (!publisher_->hasConnections() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(1ms);
        }
        return publisher_->hasConnections();
    }

    Node publisher_node_;
    Node subscriber_;
    const std::string topic_;
    const std::shared_ptr<CallbackState> state_;
    std::unique_ptr<Publisher> publisher_;
};

PubSubBenchmark& pubSubFixture(const size_t payload_size)
{
    static std::mutex fixture_mutex;
    // Google Benchmark reinvokes a benchmark while calibrating iteration counts.
    // Keep one endpoint pair per payload so calibration measures message flow,
    // not repeated endpoint churn.
    static auto* fixtures = new std::map<size_t, std::unique_ptr<PubSubBenchmark>>;

    std::lock_guard lock(fixture_mutex);
    auto& fixture = (*fixtures)[payload_size];
    if (!fixture) {
        fixture = std::make_unique<PubSubBenchmark>(payload_size);
    }
    return *fixture;
}

class InterprocessPubSubBenchmark
{
public:
    explicit InterprocessPubSubBenchmark(const size_t payload_size)
        : topic_(makeTopic("pubsub-interprocess")), payload_(payload_size, 'x'),
          subscriber_(RF_TRANS_BENCHMARK_SUBSCRIBER_PATH, topic_, std::to_string(payload_size))
    {
        AdvertiseMessageOptions options;
        options.setScope(Scope::HOST);
        publisher_ = std::make_unique<Publisher>(publisher_node_.advertise<ExampleMsg>(topic_, options));
        if (!publisher_->valid()) {
            throw std::runtime_error("failed to advertise interprocess benchmark publisher");
        }
        if (!subscriber_.start()) {
            throw std::runtime_error(subscriber_.error());
        }
        if (!waitForConnections()) {
            throw std::runtime_error("interprocess benchmark publisher did not observe its subscriber");
        }

        if (!warmup()) {
            throw std::runtime_error("interprocess benchmark subscriber did not complete a warmup message");
        }
    }

    std::unique_ptr<ExampleMsg> makeMessage() const
    {
        auto message = std::make_unique<ExampleMsg>();
        message->set_name(payload_);
        message->set_age(static_cast<int>(next_sequence_ + 1));
        return message;
    }

    bool publish(std::unique_ptr<ExampleMsg> message)
    {
        return publisher_->publish(std::move(message));
    }

    bool waitForSubscriber(const std::chrono::milliseconds timeout =
        std::chrono::duration_cast<std::chrono::milliseconds>(kCallbackTimeout))
    {
        char status = 0;
        if (!subscriber_.waitForStatus(status, timeout) || status != '1') {
            return false;
        }
        ++next_sequence_;
        return true;
    }

    size_t payloadSize() const { return payload_.size(); }

private:
    bool warmup()
    {
        const auto deadline = std::chrono::steady_clock::now() + kCallbackTimeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (!publish(makeMessage())) {
                return false;
            }
            if (waitForSubscriber(100ms)) {
                return true;
            }
        }
        return false;
    }

    bool waitForConnections() const
    {
        const auto deadline = std::chrono::steady_clock::now() + kCallbackTimeout;
        while (!publisher_->hasConnections() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(1ms);
        }
        return publisher_->hasConnections();
    }

    const std::string topic_;
    const std::string payload_;
    Node publisher_node_;
    InterprocessBenchmarkProcess subscriber_;
    std::unique_ptr<Publisher> publisher_;
    uint64_t next_sequence_{0};
};

using InterprocessPubSubFixtures = std::map<size_t, std::unique_ptr<InterprocessPubSubBenchmark>>;

InterprocessPubSubFixtures& interprocessPubSubFixtures()
{
    static auto* fixtures = new InterprocessPubSubFixtures;
    return *fixtures;
}

void destroyInterprocessPubSubFixtures()
{
    delete &interprocessPubSubFixtures();
}

InterprocessPubSubBenchmark& interprocessPubSubFixture(const size_t payload_size)
{
    static std::mutex fixture_mutex;
    static std::once_flag cleanup_once;

    std::lock_guard lock(fixture_mutex);
    auto& fixture = interprocessPubSubFixtures()[payload_size];
    if (!fixture) {
        fixture = std::make_unique<InterprocessPubSubBenchmark>(payload_size);
        std::call_once(cleanup_once, [] { std::atexit(destroyInterprocessPubSubFixtures); });
    }
    return *fixture;
}

struct ServiceRequestState
{
    std::mutex mutex;
    std::condition_variable cv;
    bool completed{false};
    bool succeeded{false};
};

class InterprocessServiceBenchmark
{
public:
    explicit InterprocessServiceBenchmark(const size_t payload_size)
        : topic_(makeTopic("service-interprocess")), payload_(payload_size, 'x'),
          server_(RF_TRANS_BENCHMARK_SERVICE_SERVER_PATH, topic_)
    {
        if (!server_.start()) {
            throw std::runtime_error(server_.error());
        }
        if (!client_.waitForService(topic_, kCallbackTimeout)) {
            throw std::runtime_error("benchmark service server was not discovered");
        }

        auto warmup_request = makeRequest();
        auto warmup_state = std::make_shared<ServiceRequestState>();
        if (!request(warmup_request, warmup_state) || !waitFor(warmup_state)) {
            throw std::runtime_error("benchmark service server did not complete a warmup request");
        }
    }

    std::shared_ptr<ExampleMsg> makeRequest() const
    {
        auto request = std::make_shared<ExampleMsg>();
        request->set_name(payload_);
        request->set_age(41);
        return request;
    }

    bool request(const std::shared_ptr<ExampleMsg>& request,
        const std::shared_ptr<ServiceRequestState>& state)
    {
        return client_.request<ExampleMsg, ExampleMsg>(topic_, request,
            [state, payload = payload_](const std::shared_ptr<const ExampleMsg>& reply, const bool success) {
                int response_age = reply ? reply->age() : -1;
                {
                    std::lock_guard lock(state->mutex);
                    state->succeeded = success && reply && reply->name() == payload && response_age == 42;
                    state->completed = true;
                }
                state->cv.notify_one();
                benchmark::DoNotOptimize(response_age);
            });
    }

    bool waitFor(const std::shared_ptr<ServiceRequestState>& state) const
    {
        std::unique_lock lock(state->mutex);
        return state->cv.wait_for(lock, kCallbackTimeout, [state] { return state->completed; }) && state->succeeded;
    }

    size_t payloadSize() const { return payload_.size(); }

private:
    const std::string topic_;
    const std::string payload_;
    InterprocessBenchmarkProcess server_;
    Node client_;
};

using InterprocessServiceFixtures = std::map<size_t, std::unique_ptr<InterprocessServiceBenchmark>>;

InterprocessServiceFixtures& interprocessServiceFixtures()
{
    static auto* fixtures = new InterprocessServiceFixtures;
    return *fixtures;
}

void destroyInterprocessServiceFixtures()
{
    delete &interprocessServiceFixtures();
}

InterprocessServiceBenchmark& interprocessServiceFixture(const size_t payload_size)
{
    static std::mutex fixture_mutex;
    static std::once_flag cleanup_once;

    std::lock_guard lock(fixture_mutex);
    auto& fixture = interprocessServiceFixtures()[payload_size];
    if (!fixture) {
        fixture = std::make_unique<InterprocessServiceBenchmark>(payload_size);
        // This runs before NodeShared's static teardown, allowing Node cleanup to use its runtime.
        std::call_once(cleanup_once, [] { std::atexit(destroyInterprocessServiceFixtures); });
    }
    return *fixture;
}

void BM_PubSubEndToEndLatency(benchmark::State& state)
{
    auto& fixture = pubSubFixture(static_cast<size_t>(state.range(0)));

    for (auto _ : state) {
        (void)_;
        const uint64_t expected = fixture.received() + 1;
        auto message = fixture.makeMessage(static_cast<int>(expected));

        const auto start = std::chrono::steady_clock::now();
        if (!fixture.publish(std::move(message))) {
            state.SkipWithError("publish failed");
            return;
        }
        if (!fixture.waitFor(expected)) {
            state.SkipWithError("timed out waiting for subscriber callback");
            return;
        }
        state.SetIterationTime(std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count());
    }

    if (!fixture.callbackValid()) {
        state.SkipWithError("subscriber received an unexpected message");
        return;
    }
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(fixture.payloadSize()));
    state.counters["payload_bytes"] = static_cast<double>(fixture.payloadSize());
}

void BM_PubSubSustainedThroughput(benchmark::State& state)
{
    auto& fixture = pubSubFixture(static_cast<size_t>(state.range(0)));

    for (auto _ : state) {
        (void)_;
        const uint64_t first_sequence = fixture.received() + 1;
        std::vector<std::unique_ptr<ExampleMsg>> messages;
        messages.reserve(kPublishBatchSize);
        for (int index = 0; index < kPublishBatchSize; ++index) {
            messages.push_back(fixture.makeMessage(static_cast<int>(first_sequence + index)));
        }

        const auto start = std::chrono::steady_clock::now();
        for (auto& message : messages) {
            if (!fixture.publish(std::move(message))) {
                state.SkipWithError("publish failed");
                return;
            }
        }
        if (!fixture.waitFor(first_sequence + kPublishBatchSize - 1)) {
            state.SkipWithError("timed out waiting for subscriber batch");
            return;
        }
        state.SetIterationTime(std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count());
    }

    if (!fixture.callbackValid()) {
        state.SkipWithError("subscriber received an unexpected message");
        return;
    }
    const int64_t messages = state.iterations() * kPublishBatchSize;
    state.SetItemsProcessed(messages);
    state.SetBytesProcessed(messages * static_cast<int64_t>(fixture.payloadSize()));
    state.counters["payload_bytes"] = static_cast<double>(fixture.payloadSize());
}

void BM_PubSubInterprocessEndToEndLatency(benchmark::State& state)
{
    auto& fixture = interprocessPubSubFixture(static_cast<size_t>(state.range(0)));

    for (auto _ : state) {
        (void)_;
        auto message = fixture.makeMessage();

        const auto start = std::chrono::steady_clock::now();
        const bool published = fixture.publish(std::move(message));
        const bool received = published && fixture.waitForSubscriber();
        state.SetIterationTime(std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count());

        if (!received) {
            state.SkipWithError("interprocess subscriber did not confirm the published message");
            return;
        }
    }

    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(fixture.payloadSize()));
    state.counters["payload_bytes"] = static_cast<double>(fixture.payloadSize());
}

void BM_ServiceLocalRoundTrip(benchmark::State& state)
{
    const size_t payload_size = static_cast<size_t>(state.range(0));
    const std::string topic = makeTopic("service");
    const std::string payload(payload_size, 'x');
    Node server;
    Node client;

    AdvertiseServiceOptions service_options;
    service_options.setScope(Scope::PROCESS);
    if (!server.advertise<ExampleMsg, ExampleMsg>(topic,
            [](const std::shared_ptr<const ExampleMsg>& request, std::shared_ptr<ExampleMsg> reply) {
                reply->set_name(request->name());
                reply->set_age(request->age() + 1);
                return true;
            }, service_options)) {
        state.SkipWithError("failed to advertise benchmark service");
        return;
    }
    if (!client.waitForService(topic, kCallbackTimeout)) {
        state.SkipWithError("benchmark service did not become available");
        return;
    }

    auto request = std::make_shared<ExampleMsg>();
    request->set_name(payload);
    request->set_age(41);

    for (auto _ : state) {
        (void)_;
        bool callback_called = false;
        bool callback_succeeded = false;
        const auto start = std::chrono::steady_clock::now();
        const bool accepted = client.request<ExampleMsg, ExampleMsg>(topic, request,
            [&callback_called, &callback_succeeded, &payload](const std::shared_ptr<const ExampleMsg>& reply,
                const bool success) {
                callback_called = true;
                int response_age = reply ? reply->age() : -1;
                callback_succeeded = success && reply && reply->name() == payload && response_age == 42;
                benchmark::DoNotOptimize(response_age);
            });
        state.SetIterationTime(std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count());

        if (!accepted || !callback_called || !callback_succeeded) {
            state.SkipWithError("service request failed");
            return;
        }
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(payload_size));
    state.counters["payload_bytes"] = static_cast<double>(payload_size);
}

void BM_ServiceInterprocessRoundTrip(benchmark::State& state)
{
    auto& fixture = interprocessServiceFixture(static_cast<size_t>(state.range(0)));
    auto request = fixture.makeRequest();

    for (auto _ : state) {
        (void)_;
        auto request_state = std::make_shared<ServiceRequestState>();
        const auto start = std::chrono::steady_clock::now();
        const bool accepted = fixture.request(request, request_state);
        const bool completed = accepted && fixture.waitFor(request_state);
        state.SetIterationTime(std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count());

        if (!completed) {
            state.SkipWithError("interprocess service request failed or timed out");
            return;
        }
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(fixture.payloadSize()));
    state.counters["payload_bytes"] = static_cast<double>(fixture.payloadSize());
}

BENCHMARK(BM_PubSubEndToEndLatency)->Arg(32)->Arg(1024)->Arg(65536)->UseManualTime();
BENCHMARK(BM_PubSubSustainedThroughput)->Arg(32)->Arg(1024)->Arg(65536)->UseManualTime();
BENCHMARK(BM_PubSubInterprocessEndToEndLatency)->Arg(32)->Arg(1024)->Arg(65536)->UseManualTime();
BENCHMARK(BM_ServiceLocalRoundTrip)->Arg(32)->Arg(1024)->Arg(65536)->UseManualTime();
BENCHMARK(BM_ServiceInterprocessRoundTrip)->Arg(32)->Arg(1024)->Arg(65536)->UseManualTime();
} // namespace

BENCHMARK_MAIN();
