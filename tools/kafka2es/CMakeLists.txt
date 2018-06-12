add_executable(kafka2es kafka2es.cpp)

if (LINK_SHARED)
    target_link_libraries(kafka2es kspp_es ${CSI_LIBS_SHARED})
else ()
    target_link_libraries(kafka2es kspp_es_s ${CSI_LIBS_STATIC})
endif ()

INSTALL(TARGETS kafka2es RUNTIME DESTINATION bin)



