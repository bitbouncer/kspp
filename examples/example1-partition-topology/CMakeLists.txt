project(example1-partition-topology)

SET(SRCS
example1-partition-topology.cpp
)

add_executable(example1-partition-topology ${SRCS})
target_link_libraries(example1-partition-topology ${CSI_LIBS_STATIC})
