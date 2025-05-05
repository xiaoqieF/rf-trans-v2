add_executable(test_discovery test_discovery.cc)
target_include_directories(test_discovery PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_discovery PRIVATE rf-trans)

add_executable(test_pub test_pub.cc)
target_include_directories(test_pub PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_pub PRIVATE rf-trans)

add_executable(test_sub test_sub.cc)
target_include_directories(test_sub PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_sub PRIVATE rf-trans)

add_executable(test_response test_response.cc)
target_include_directories(test_response PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_response PRIVATE rf-trans)

add_executable(test_request test_request.cc)
target_include_directories(test_request PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_request PRIVATE rf-trans)