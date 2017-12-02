add_executable(kspp_avrogencpp kspp_avrogencpp.cpp)
target_link_libraries(kspp_avrogencpp ${CSI_LIBS_STATIC})

INSTALL(TARGETS kspp_avrogencpp RUNTIME DESTINATION bin)
