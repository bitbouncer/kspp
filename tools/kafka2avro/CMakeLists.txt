add_executable(kafka2avro kafka2avro.cpp)

target_link_libraries(kafka2avro ${ALL_LIBS})

INSTALL(TARGETS kafka2avro RUNTIME DESTINATION bin)

