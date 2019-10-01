
SET(SRCS
example12-generic-avro.cpp
)

add_executable(example12-generic-avro ${SRCS})
target_link_libraries(example12-generic-avro ${CSI_LIBS_STATIC})
