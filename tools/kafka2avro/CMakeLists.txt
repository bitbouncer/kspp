add_executable(kafka2avro kafka2avro.cpp)

if (LINK_SHARED)
    target_link_libraries(kafka2avro ${CSI_LIBS_SHARED})
else ()
    target_link_libraries(kafka2avro ${CSI_LIBS_STATIC})
endif ()

INSTALL(TARGETS kafka2avro RUNTIME DESTINATION bin)

