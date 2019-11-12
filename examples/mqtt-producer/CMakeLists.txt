SET(SRCS
        mqtt-producer.cpp
        )

add_executable(mqtt-producer ${SRCS})
target_link_libraries(mqtt-producer ${CSI_LIBS_STATIC})
