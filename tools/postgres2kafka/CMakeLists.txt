add_executable(postgres2kafka postgres2kafka.cpp)
target_link_libraries(postgres2kafka ${CSI_LIBS_STATIC})

INSTALL(TARGETS postgres2kafka RUNTIME DESTINATION bin)
