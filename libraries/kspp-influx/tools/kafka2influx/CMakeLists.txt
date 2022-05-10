add_executable(kafka2influxdb kafka2influxdb.cpp)

target_link_libraries(kafka2influxdb kspp-influx_s ${ALL_LIBS})

INSTALL(TARGETS kafka2influxdb RUNTIME DESTINATION bin)
