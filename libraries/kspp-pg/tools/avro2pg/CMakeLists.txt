add_executable(avro2pg avro2pg.cpp)

target_link_libraries(avro2pg kspp-pg_s ${ALL_LIBS})

INSTALL(TARGETS avro2pg RUNTIME DESTINATION bin)


