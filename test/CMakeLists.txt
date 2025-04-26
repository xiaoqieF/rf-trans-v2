add_executable(test_discovery test_discovery.cc)
target_include_directories(test_discovery PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_discovery PRIVATE rf-trans)

add_executable(test_node_shared test_node_shared.cc)
target_include_directories(test_node_shared PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_node_shared PRIVATE rf-trans)