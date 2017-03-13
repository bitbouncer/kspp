add_executable(test1 test1.cpp)
target_link_libraries(test1 ${CSI_LIBS})
add_test(NAME test1 COMMAND $<TARGET_FILE:test1>)
#add_test(test_foo test_foo)

add_executable(test2_mem_store test2_mem_store.cpp)
target_link_libraries(test2_mem_store ${CSI_LIBS})
add_test(NAME test2_mem_store COMMAND $<TARGET_FILE:test2_mem_store>)

add_executable(test2_mem_windowed_store test2_mem_windowed_store.cpp)
target_link_libraries(test2_mem_windowed_store ${CSI_LIBS})
add_test(NAME test2_mem_windowed_store COMMAND $<TARGET_FILE:test2_mem_windowed_store>)

add_executable(test2_mem_counter_store test2_mem_counter_store.cpp)
target_link_libraries(test2_mem_counter_store ${CSI_LIBS})
add_test(NAME test2_mem_counter_store COMMAND $<TARGET_FILE:test2_mem_counter_store>)

if (ENABLE_ROCKSDB)
add_executable(test2_rocksdb_store test2_rocksdb_store.cpp)
target_link_libraries(test2_rocksdb_store ${CSI_LIBS})
add_test(NAME test2_rocksdb_store COMMAND $<TARGET_FILE:test2_rocksdb_store>)

add_executable(test2_rocksdb_counter_store test2_rocksdb_counter_store.cpp)
target_link_libraries(test2_rocksdb_counter_store ${CSI_LIBS})
add_test(NAME test2_rocksdb_counter_store COMMAND $<TARGET_FILE:test2_rocksdb_counter_store>)
endif()