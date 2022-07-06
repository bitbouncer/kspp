SET(SRCS
        mqtt2kafka.cpp
        )

#SET(PAHO_LIBS_STATIC paho-mqttpp3 paho-mqtt3cs paho-mqtt3as)
SET(PAHO_LIBS_SHARED paho-mqttpp3 paho-mqtt3c paho-mqtt3a)

add_executable(mqtt2kafka.cpp ${SRCS})
target_link_libraries(mqtt2kafka.cpp ${PAHO_LIBS_SHARED} ${ALL_LIBS})

INSTALL(TARGETS mqtt2kafka.cpp RUNTIME DESTINATION bin)

