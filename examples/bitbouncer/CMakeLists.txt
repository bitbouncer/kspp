add_executable(bb_generic_avro_console_exporter main.cpp)

if (LINK_SHARED)
    target_link_libraries(bb_generic_avro_console_exporter ${BBSTREAMING_LIBS_SHARED} ${CSI_LIBS_SHARED})
else ()
    target_link_libraries(bb_generic_avro_console_exporter ${BBSTREAMING_LIBS_STATIC} ${CSI_LIBS_STATIC})
endif ()

INSTALL(TARGETS bb_generic_avro_console_exporter RUNTIME DESTINATION bin)


