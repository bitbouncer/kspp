project(example_count)

SET(SRCS
example_count.cpp
)

add_executable(example_count ${SRCS})
target_link_libraries(example_count ${CSI_LIBS})
