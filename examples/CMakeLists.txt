add_subdirectory(example_setup)
add_subdirectory(example1-partition-topology)
add_subdirectory(example2-join)
add_subdirectory(example3-count)
add_subdirectory(example4-count-large)
add_subdirectory(example5-repartition)
add_subdirectory(example6-filter)
add_subdirectory(example7-token-bucket)
add_subdirectory(example8-ktable-mem)

if (ENABLE_ROCKSDB)
    add_subdirectory(example9-ktable-rocksdb)
endif ()

add_subdirectory(example10-avro)
add_subdirectory(example12-generic-avro)

add_subdirectory(example14-raw-sink)

