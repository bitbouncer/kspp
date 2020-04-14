add_executable(kafka2rocksdb main.cpp)

if (LINK_SHARED)
    target_link_libraries(kafka2rocksdb ${CSI_LIBS_SHARED})
else ()
    target_link_libraries(kafka2rocksdb ${CSI_LIBS_STATIC})
endif ()

INSTALL(TARGETS kafka2rocksdb RUNTIME DESTINATION bin)
