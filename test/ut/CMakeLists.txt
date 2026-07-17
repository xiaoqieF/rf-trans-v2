include(FetchContent)

cmake_policy(SET CMP0135 NEW)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/release-1.12.1.zip
)

FetchContent_MakeAvailable(googletest)

add_executable(test_advertise_options test_advertise_options.cc)
target_include_directories(test_advertise_options PRIVATE ${GTEST_INCLUDE_DIRS})
target_link_libraries(test_advertise_options PRIVATE rf-trans GTest::gtest GTest::gtest_main pthread)

add_test(NAME test_advertise_options COMMAND test_advertise_options)

add_executable(test_topic_storage test_topic_storage.cc)
target_include_directories(test_topic_storage PRIVATE ${GTEST_INCLUDE_DIRS})
target_link_libraries(test_topic_storage PRIVATE rf-trans GTest::gtest GTest::gtest_main pthread)

add_test(NAME test_topic_storage COMMAND test_topic_storage)

add_executable(test_publisher_info test_publisher_info.cc)
target_include_directories(test_publisher_info PRIVATE ${GTEST_INCLUDE_DIRS})
target_link_libraries(test_publisher_info PRIVATE rf-trans GTest::gtest GTest::gtest_main pthread)

add_test(NAME test_publisher_info COMMAND test_publisher_info)

add_executable(test_discovery_init test_discovery_init.cc)
target_include_directories(test_discovery_init PRIVATE ${GTEST_INCLUDE_DIRS})
target_link_libraries(test_discovery_init PRIVATE rf-trans GTest::gtest GTest::gtest_main pthread)

add_test(NAME test_discovery_init COMMAND test_discovery_init)
