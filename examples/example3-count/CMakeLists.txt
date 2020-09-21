project(example3-count)

SET(SRCS
        example3-count.cpp
        )

add_executable(example3-count ${SRCS})
target_link_libraries(example3-count ${ALL_LIBS})
