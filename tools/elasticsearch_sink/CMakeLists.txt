add_executable(elasticsearch_sink elasticsearch_sink.cpp)
target_link_libraries(elasticsearch_sink ${CSI_LIBS_STATIC})

INSTALL(TARGETS elasticsearch_sink RUNTIME DESTINATION bin)



