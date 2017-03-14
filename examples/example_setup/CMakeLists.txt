project(test_setup)

SET(SRCS
test_setup.cpp
)

add_executable(test_setup ${SRCS})
target_link_libraries(test_setup ${CSI_LIBS})
