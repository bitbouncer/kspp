project(example3)

SET(SRCS
example3.cpp
)

add_executable(example3 ${SRCS})
target_link_libraries(example3 ${CSI_LIBS})
