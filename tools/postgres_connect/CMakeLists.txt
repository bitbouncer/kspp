add_executable(postgres2kafka postgres2kafka.cpp)
target_link_libraries(postgres2kafka ${CSI_LIBS_STATIC})

INSTALL(TARGETS postgres2kafka RUNTIME DESTINATION bin)



add_executable(kafka2postgres kafka2postgres.cpp)
target_link_libraries(kafka2postgres ${CSI_LIBS_STATIC})

INSTALL(TARGETS kafka2postgres RUNTIME DESTINATION bin)
