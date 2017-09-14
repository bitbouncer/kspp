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

    add_executable(test2_rocksdb_windowed_store test2_rocksdb_windowed_store.cpp)
    target_link_libraries(test2_rocksdb_windowed_store ${CSI_LIBS})
    add_test(NAME test2_rocksdb_windowed_store COMMAND $<TARGET_FILE:test2_rocksdb_windowed_store>)

    add_executable(test2_rocksdb_counter_store test2_rocksdb_counter_store.cpp)
    target_link_libraries(test2_rocksdb_counter_store ${CSI_LIBS})
    add_test(NAME test2_rocksdb_counter_store COMMAND $<TARGET_FILE:test2_rocksdb_counter_store>)
endif ()

add_executable(test3_mem_token_bucket test3_mem_token_bucket.cpp)
target_link_libraries(test3_mem_token_bucket ${CSI_LIBS})
add_test(NAME test3_mem_token_bucket COMMAND $<TARGET_FILE:test3_mem_token_bucket>)

add_executable(test4_kafka_consumer test4_kafka_consumer.cpp)
target_link_libraries(test4_kafka_consumer ${CSI_LIBS})
add_test(NAME test4_kafka_consumer COMMAND $<TARGET_FILE:test4_kafka_consumer>)

add_executable(test5_kafka_source_sink test5_kafka_source_sink.cpp)
target_link_libraries(test5_kafka_source_sink ${CSI_LIBS})
add_test(NAME test5_kafka_source_sink COMMAND $<TARGET_FILE:test5_kafka_source_sink>)

add_executable(test6_repartition test6_repartition.cpp)
target_link_libraries(test6_repartition ${CSI_LIBS})
add_test(NAME test6_repartition COMMAND $<TARGET_FILE:test6_repartition>)

add_executable(test7_cluster_uri test7_cluster_uri.cpp)
target_link_libraries(test7_cluster_uri ${CSI_LIBS})
add_test(NAME test7_cluster_uri COMMAND $<TARGET_FILE:test7_cluster_uri>)

add_executable(test7_url_vector test7_url_vector.cpp)
target_link_libraries(test7_url_vector ${CSI_LIBS})
add_test(NAME test7_url_vector COMMAND $<TARGET_FILE:test7_url_vector>)
