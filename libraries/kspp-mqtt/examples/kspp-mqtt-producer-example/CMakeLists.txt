SET(SRCS 
  kspp-mqtt-producer-example.cpp
  )

add_executable(kspp-mqtt-producer-example ${SRCS})

target_link_libraries(kspp-mqtt-producer-example kspp-mqtt_s ${PAHO_LIBS_STATIC}  ${ALL_LIBS})
