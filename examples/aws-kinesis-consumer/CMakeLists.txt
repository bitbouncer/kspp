SET(SRCS
        aws-kinesis-consumer.cpp
        )

add_executable(aws-kinesis-consumer ${SRCS})
target_link_libraries(aws-kinesis-consumer ${AWS_KINESIS_LIBS} ${CSI_LIBS_STATIC})
