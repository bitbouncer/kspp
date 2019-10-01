project(example7-token-bucket)

SET(SRCS
example7-token-bucket.cpp
)

add_executable(example7-token-bucket ${SRCS})
target_link_libraries(example7-token-bucket ${CSI_LIBS_STATIC})
