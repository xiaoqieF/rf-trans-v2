cmake_minimum_required(VERSION 3.22)

project(rf-trans)

add_compile_options(-std=c++17 -Wall -Wextra -Wno-unused-parameter -Wno-unused-variable -O2)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)


find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(ZMQ REQUIRED libzmq)

file(GLOB_RECURSE SRCS "src/*.cpp" "src/*.cc")

set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/msgs)
set(PROTO_OUT_DIR ${CMAKE_BINARY_DIR}/msgs)
file(MAKE_DIRECTORY ${PROTO_OUT_DIR})

file(GLOB PROTO_FILES ${PROTO_DIR}/*.proto)

foreach(PROTO ${PROTO_FILES})
    get_filename_component(PROTO_NAME ${PROTO} NAME_WE)
    list(APPEND PROTO_SRCS ${PROTO_OUT_DIR}/${PROTO_NAME}.pb.cc)
    list(APPEND PROTO_HDRS ${PROTO_OUT_DIR}/${PROTO_NAME}.pb.h)
endforeach()

add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS --cpp_out=${PROTO_OUT_DIR} -I ${PROTO_DIR} ${PROTO_FILES}
    DEPENDS ${PROTO_FILES}
)

add_library(rf-trans ${SRCS} ${PROTO_SRCS})

target_include_directories(rf-trans PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/deps/elog
    ${PROJECT_BINARY_DIR}
    ${ZMQ_INCLUDE_DIRS}
)

target_link_libraries(rf-trans PRIVATE
    ${PROTOBUF_LIBRARIES}
    ${ZMQ_LIBRARIES}
)

add_subdirectory(test)

option(BUILD_TESTING "Enable tests" ON)

if(BUILD_TESTING)
    enable_testing()
    add_subdirectory(test/ut)
endif()