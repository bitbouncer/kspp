add_executable(postgres2file postgres2file.cpp)
target_link_libraries(postgres2file ${CSI_LIBS_STATIC})

INSTALL(TARGETS postgres2file RUNTIME DESTINATION bin)
