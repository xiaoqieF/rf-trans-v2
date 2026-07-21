add_executable(rf-trans rf_trans.cc)
target_link_libraries(rf-trans PRIVATE rf_trans::rf_trans)
target_compile_features(rf-trans PRIVATE cxx_std_17)

install(TARGETS rf-trans
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

if(BUILD_TESTING)
    add_test(NAME rf_trans_cli_help COMMAND rf-trans --help)
endif()
