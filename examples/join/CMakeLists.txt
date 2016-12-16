project(example_join)

SET(SRCS
example_join.cpp
)

add_executable(example_join ${SRCS})
target_link_libraries(example_join ${CSI_LIBS})
