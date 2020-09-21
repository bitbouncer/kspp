SET(SRCS
        kinesis-test.cpp
        )

add_executable(kinesis-test ${SRCS})
target_link_libraries(kinesis-test  ${AWS_KINESIS_LIBS} ${ALL_LIBS})
