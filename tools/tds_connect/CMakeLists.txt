add_executable(tds2kafka tds2kafka.cpp)

if (LINK_SHARED)
target_link_libraries(tds2kafka kspp_tds sybdb ${CSI_LIBS_SHARED})
else()
    target_link_libraries(tds2kafka kspp_tds_s sybdb ${CSI_LIBS_STATIC})
endif()

INSTALL(TARGETS tds2kafka RUNTIME DESTINATION bin)



