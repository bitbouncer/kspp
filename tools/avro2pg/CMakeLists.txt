add_executable(avro2pg avro2pg.cpp)

if (LINK_SHARED)
    target_link_libraries(avro2pg kspp_pg pq ${CSI_LIBS_SHARED})
else ()
    target_link_libraries(avro2pg kspp_pg_s pq ${CSI_LIBS_STATIC})
endif ()

INSTALL(TARGETS avro2pg RUNTIME DESTINATION bin)


