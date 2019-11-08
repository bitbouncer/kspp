SET(SRCS
        kinesis-consumer.cpp
        )

add_executable(kinesis-consumer ${SRCS})
target_link_libraries(kinesis-consumer ${AWS_KINESIS_LIBS} ${CSI_LIBS_STATIC})
