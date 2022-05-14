project(example4-count-large)

SET(SRCS
        example4-count-large.cpp
        )

add_executable(example4-count-large ${SRCS})
target_link_libraries(example4-count-large ${ALL_LIBS})
