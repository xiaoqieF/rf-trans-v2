add_executable(main main.cc)

target_include_directories(main PUBLIC ${CMAKE_SOURCE_DIR}/include)

target_link_libraries(main PRIVATE rf-trans)