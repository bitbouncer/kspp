project(ex2)

SET(SRCS
ex2.cpp
wingetopt.h
wingetopt.c
)

add_executable(ex2 ${SRCS})
target_link_libraries(ex2 ${CSI_LIBS})
