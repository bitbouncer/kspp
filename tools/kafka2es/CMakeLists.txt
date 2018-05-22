add_executable(kafka2es kafka2es.cpp)
target_link_libraries(kafka2es kspp_es_s ${CSI_LIBS_STATIC})

INSTALL(TARGETS kafka2es RUNTIME DESTINATION bin)



