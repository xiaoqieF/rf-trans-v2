find_package(benchmark 1.8 CONFIG QUIET)

if(NOT TARGET benchmark::benchmark)
    include(FetchContent)

    # Benchmarks only need the library; do not build Google Benchmark's own tests.
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "Build Google Benchmark tests" FORCE)
    FetchContent_Declare(
        google_benchmark
        URL https://github.com/google/benchmark/archive/refs/tags/v1.8.3.tar.gz
    )
    FetchContent_MakeAvailable(google_benchmark)
endif()

add_executable(benchmark_service_server benchmark_service_server.cc)
target_link_libraries(benchmark_service_server PRIVATE rf-trans pthread)

add_executable(benchmark_pubsub_subscriber benchmark_pubsub_subscriber.cc)
target_link_libraries(benchmark_pubsub_subscriber PRIVATE rf-trans pthread)

add_executable(benchmark_pubsub_service benchmark_pubsub_service.cc)
target_link_libraries(benchmark_pubsub_service PRIVATE rf-trans benchmark::benchmark pthread)
target_compile_definitions(benchmark_pubsub_service PRIVATE
    RF_TRANS_BENCHMARK_SERVICE_SERVER_PATH=\"$<TARGET_FILE:benchmark_service_server>\"
    RF_TRANS_BENCHMARK_SUBSCRIBER_PATH=\"$<TARGET_FILE:benchmark_pubsub_subscriber>\"
)
add_dependencies(benchmark_pubsub_service benchmark_service_server benchmark_pubsub_subscriber)
