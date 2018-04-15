project(example18-metrics20-ingest-telegraf)

SET(SRCS
example18-metrics20-ingest-telegraf.cpp
)

add_executable(example18-metrics20-ingest-telegraf ${SRCS})
target_link_libraries(example18-metrics20-ingest-telegraf ${CSI_LIBS})
