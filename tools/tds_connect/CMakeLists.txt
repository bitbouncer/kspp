add_executable(tds2kafka tds2kafka.cpp)
target_link_libraries(tds2kafka sybdb ${ALL_LIBS})
INSTALL(TARGETS tds2kafka RUNTIME DESTINATION bin)



