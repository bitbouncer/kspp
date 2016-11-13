project(rdkafka_consumer_example)

SET(SRCS
rdkafka_consumer_example.cpp
)

add_executable(rdkafka_consumer_example ${SRCS})
target_link_libraries(rdkafka_consumer_example ${CSI_LIBS})
