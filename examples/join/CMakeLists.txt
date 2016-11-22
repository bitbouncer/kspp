project(join)

SET(SRCS
join.cpp
)

add_executable(join ${SRCS})
target_link_libraries(join ${CSI_LIBS})
