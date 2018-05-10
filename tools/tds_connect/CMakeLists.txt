add_executable(tds2kafka tds2kafka.cpp)
target_link_libraries(tds2kafka kspp_tds_s sybdb ${CSI_LIBS_STATIC})

INSTALL(TARGETS tds2kafka RUNTIME DESTINATION bin)



