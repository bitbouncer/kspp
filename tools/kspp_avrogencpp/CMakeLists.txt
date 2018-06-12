add_executable(kspp_avrogencpp kspp_avrogencpp.cpp)

if (LINK_SHARED)
    target_link_libraries(kspp_avrogencpp ${CSI_LIBS_SHARED})
else ()
    target_link_libraries(kspp_avrogencpp ${CSI_LIBS_STATIC})
endif ()

INSTALL(TARGETS kspp_avrogencpp RUNTIME DESTINATION bin)
