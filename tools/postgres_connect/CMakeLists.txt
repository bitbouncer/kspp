add_executable(postgres2kafka postgres2kafka.cpp)

if (LINK_SHARED)
    target_link_libraries(postgres2kafka ${CSI_LIBS_SHARED})
else ()
    target_link_libraries(postgres2kafka ${CSI_LIBS_STATIC})
endif ()

INSTALL(TARGETS postgres2kafka RUNTIME DESTINATION bin)


add_executable(kafka2postgres kafka2postgres.cpp)

if (LINK_SHARED)
target_link_libraries(kafka2postgres ${CSI_LIBS_SHARED})
else ()
    target_link_libraries(kafka2postgres ${CSI_LIBS_STATIC})
endif()

INSTALL(TARGETS kafka2postgres RUNTIME DESTINATION bin)
