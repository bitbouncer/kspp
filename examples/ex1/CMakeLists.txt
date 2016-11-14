project(ex1)

SET(SRCS
ex1.cpp
wingetopt.h
wingetopt.c
)

add_executable(ex1 ${SRCS})
target_link_libraries(ex1 ${CSI_LIBS})
