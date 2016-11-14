project(rocksdb-example)

SET(SRCS
rocksdb-example.cpp
)

add_executable(rocksdb-example ${SRCS})
target_link_libraries(rocksdb-example ${CSI_LIBS})
