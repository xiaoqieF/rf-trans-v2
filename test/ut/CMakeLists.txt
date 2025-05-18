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
