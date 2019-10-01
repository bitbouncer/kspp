add_executable(bb2pg bb2pg.cpp)

if (LINK_SHARED)
    target_link_libraries(bb2pg ${BBSTREAMING_LIBS_SHARED} kspp_pg ${CSI_LIBS_SHARED})
else ()
    target_link_libraries(bb2pg ${BBSTREAMING_LIBS_STATIC} kspp_pg_s ${CSI_LIBS_STATIC})
endif ()

INSTALL(TARGETS bb2pg RUNTIME DESTINATION bin)


add_executable(bb2console bb2console.cpp)

if (LINK_SHARED)
    target_link_libraries(bb2console ${BBSTREAMING_LIBS_SHARED} ${CSI_LIBS_SHARED})
else ()
    target_link_libraries(bb2console ${BBSTREAMING_LIBS_STATIC} ${CSI_LIBS_STATIC})
endif ()

INSTALL(TARGETS bb2console RUNTIME DESTINATION bin)


add_executable(bb2avro bb2avro.cpp)

if (LINK_SHARED)
    target_link_libraries(bb2avro ${BBSTREAMING_LIBS_SHARED} ${CSI_LIBS_SHARED})
else ()
    target_link_libraries(bb2avro ${BBSTREAMING_LIBS_STATIC} ${CSI_LIBS_STATIC})
endif ()

INSTALL(TARGETS bb2avro RUNTIME DESTINATION bin)

