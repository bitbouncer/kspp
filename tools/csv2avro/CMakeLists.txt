add_executable(csv2avro csv2avro.cpp)

if (LINK_SHARED)
    target_link_libraries(csv2avro ${CSI_LIBS_SHARED})
else ()
    target_link_libraries(csv2avro ${CSI_LIBS_STATIC})
endif ()

INSTALL(TARGETS csv2avro RUNTIME DESTINATION bin)

