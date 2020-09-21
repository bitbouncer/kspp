project(example14-raw-sink)

SET(SRCS
        example14-raw-sink.cpp
        )

add_executable(example14-raw-sink ${SRCS})
target_link_libraries(example14-raw-sink ${ALL_LIBS})
