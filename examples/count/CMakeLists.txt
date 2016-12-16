project(group_by)

SET(SRCS
group_by.cpp
)

add_executable(group_by ${SRCS})
target_link_libraries(group_by ${CSI_LIBS})
