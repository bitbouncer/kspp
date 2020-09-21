SET(SRCS
        example_setup.cpp
        )

add_executable(example_setup ${SRCS})
target_link_libraries(example_setup ${ALL_LIBS})
