project(example2-join)

SET(SRCS
        example2-join.cpp
        )

add_executable(example2-join ${SRCS})
target_link_libraries(example2-join ${ALL_LIBS})
