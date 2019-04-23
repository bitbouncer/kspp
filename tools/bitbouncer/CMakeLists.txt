add_executable(bb2pg bb2pg.cpp grpc_db_streamer.h)

if (LINK_SHARED)
    target_link_libraries(bb2pg ${BBSTREAMING_LIBS_SHARED} kspp_pg ${CSI_LIBS_SHARED})
else ()
    target_link_libraries(bb2pg ${BBSTREAMING_LIBS_STATIC} kspp_pg_s ${CSI_LIBS_STATIC})
endif ()

INSTALL(TARGETS bb2pg RUNTIME DESTINATION bin)


