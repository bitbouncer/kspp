project(example11-avro-file)

SET(SRCS
example11-avro-file.cpp
)

add_executable(example11-avro-file ${SRCS})
target_link_libraries(example11-avro-file ${CSI_LIBS})
