add_executable(kafka2rocksdb main.cpp)

target_link_libraries(kafka2rocksdb ${ALL_LIBS})

INSTALL(TARGETS kafka2rocksdb RUNTIME DESTINATION bin)
