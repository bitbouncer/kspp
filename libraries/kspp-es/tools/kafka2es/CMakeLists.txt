add_executable(kafka2es kafka2es.cpp)

target_link_libraries(kafka2es kspp-es_s ${ALL_LIBS})

INSTALL(TARGETS kafka2es RUNTIME DESTINATION bin)



