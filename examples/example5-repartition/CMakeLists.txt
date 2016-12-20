project(example5-repartition)

SET(SRCS
example5-repartition.cpp
)

add_executable(example5-repartition ${SRCS})
target_link_libraries(example5-repartition ${CSI_LIBS})
