cmake_minimum_required(VERSION 3.22)

project(rf_trans VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

option(BUILD_TESTING "Enable unit tests" ON)
option(BUILD_BENCHMARKS "Build benchmark executables" OFF)
option(BUILD_CLI "Build the rf-trans command-line tool" ON)

if(BUILD_BENCHMARKS AND NOT CMAKE_CONFIGURATION_TYPES)
    # Performance measurements must not use Debug instrumentation or assertions.
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo MinSizeRel)
endif()


find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
pkg_check_modules(rf_trans_ZMQ REQUIRED IMPORTED_TARGET libzmq)

file(GLOB_RECURSE SRCS CONFIGURE_DEPENDS "src/*.cpp" "src/*.cc")

set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/msgs)
set(PROTO_OUT_DIR ${CMAKE_BINARY_DIR}/msgs)
file(MAKE_DIRECTORY ${PROTO_OUT_DIR})

file(GLOB PROTO_FILES CONFIGURE_DEPENDS ${PROTO_DIR}/*.proto)

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

add_library(rf_trans ${SRCS} ${PROTO_SRCS})
add_library(rf_trans::rf_trans ALIAS rf_trans)

set_target_properties(rf_trans PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
)

target_compile_features(rf_trans PUBLIC cxx_std_17)
target_compile_options(rf_trans PRIVATE
    -Wall
    -Wextra
    -Wno-unused-parameter
    -Wno-unused-variable
    -O2
)

target_include_directories(rf_trans PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/deps/elog>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(rf_trans PUBLIC
    protobuf::libprotobuf
    PkgConfig::rf_trans_ZMQ
    Threads::Threads
)

install(TARGETS rf_trans
    EXPORT rf_transTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(DIRECTORY include/trans DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY deps/elog/elog DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(FILES ${PROTO_HDRS} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/msgs)
install(DIRECTORY msgs/ DESTINATION ${CMAKE_INSTALL_DATADIR}/rf_trans/proto
    FILES_MATCHING PATTERN "*.proto"
)

set(RF_TRANS_CMAKE_INSTALL_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/rf_trans")

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/rf_transConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/rf_transConfig.cmake
    INSTALL_DESTINATION ${RF_TRANS_CMAKE_INSTALL_DIR}
)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/rf_transConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/rf_transConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/rf_transConfigVersion.cmake
    DESTINATION ${RF_TRANS_CMAKE_INSTALL_DIR}
)

install(EXPORT rf_transTargets
    FILE rf_transTargets.cmake
    NAMESPACE rf_trans::
    DESTINATION ${RF_TRANS_CMAKE_INSTALL_DIR}
)

if(BUILD_TESTING)
    enable_testing()
endif()

if(BUILD_CLI)
    add_subdirectory(tools)
endif()

add_subdirectory(test)

if(BUILD_TESTING)
    add_subdirectory(test/ut)
endif()
