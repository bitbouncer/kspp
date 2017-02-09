project(example8-ktable-mem)

SET(SRCS
example8-ktable-mem.cpp
)

add_executable(example8-ktable-mem ${SRCS})
target_link_libraries(example8-ktable-mem ${CSI_LIBS})
