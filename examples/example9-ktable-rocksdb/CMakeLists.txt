project(example9-ktable-rocksdb)

SET(SRCS
example9-ktable-rocksdb.cpp
)

add_executable(example9-ktable-rocksdb ${SRCS})
target_link_libraries(example9-ktable-rocksdb kspp_rocksdb_s ${CSI_LIBS})
