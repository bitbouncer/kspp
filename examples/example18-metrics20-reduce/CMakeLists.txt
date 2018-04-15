project(example18-metrics20-reduce)

SET(SRCS
example18-metrics20-reduce.cpp
)

add_executable(example18-metrics20-reduce ${SRCS})
target_link_libraries(example18-metrics20-reduce ${CSI_LIBS})
