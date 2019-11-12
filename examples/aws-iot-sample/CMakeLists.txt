SET(SRCS
        aws-iot-sample.cpp
        )

add_executable(aws-iot-sample ${SRCS})
target_link_libraries(aws-iot-sample ${AWS_KINESIS_LIBS} ${CSI_LIBS_STATIC})
