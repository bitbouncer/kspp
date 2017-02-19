project(example1-basic)

SET(SRCS
example1-basic.cpp
)

add_executable(example1-basic ${SRCS})
target_link_libraries(example1-basic ${CSI_LIBS})
