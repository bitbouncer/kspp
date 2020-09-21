add_executable(csv2avro csv2avro.cpp)

target_link_libraries(csv2avro ${ALL_LIBS})

INSTALL(TARGETS csv2avro RUNTIME DESTINATION bin)

