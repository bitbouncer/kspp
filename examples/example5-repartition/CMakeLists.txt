project(example5_repartition)

SET(SRCS
example5_repartition.cpp
)

add_executable(example5_repartition ${SRCS})
target_link_libraries(example5_repartition ${CSI_LIBS})
