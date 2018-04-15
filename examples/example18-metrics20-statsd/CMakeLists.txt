project(example18-metrics20-statsd)

SET(SRCS
example18-metrics20-statsd.cpp
)

add_executable(example18-metrics20-statsd ${SRCS})
target_link_libraries(example18-metrics20-statsd ${CSI_LIBS})
