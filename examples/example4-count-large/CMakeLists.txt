project(example_count_large)

SET(SRCS
example_count_large.cpp
)

add_executable(example_count_large ${SRCS})
target_link_libraries(example_count_large ${CSI_LIBS})
