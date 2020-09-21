project(example10-avro)

SET(SRCS
        example10-avro.cpp
        )

add_executable(example10-avro ${SRCS})
target_link_libraries(example10-avro ${ALL_LIBS})
