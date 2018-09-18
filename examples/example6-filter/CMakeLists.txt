project(example6-filter)

SET(SRCS
example6-filter.cpp
)

add_executable(example6-filter ${SRCS})
target_link_libraries(example6-filter ${CSI_LIBS_STATIC})
