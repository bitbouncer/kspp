project(example1-basic-partition-topology)

SET(SRCS
example1-basic-partition-topology.cpp
)

add_executable(example1-basic-partition-topology ${SRCS})
target_link_libraries(example1-basic-partition-topology ${CSI_LIBS})
