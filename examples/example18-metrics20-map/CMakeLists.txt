project(example18-metrics20-map)

SET(SRCS
example18-metrics20-map.cpp
)

add_executable(example18-metrics20-map ${SRCS})
target_link_libraries(example18-metrics20-map ${CSI_LIBS})
