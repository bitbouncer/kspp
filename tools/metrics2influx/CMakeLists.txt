add_executable(metrics2influx_v1 metrics2influx.cpp)

if (LINK_SHARED)
    target_link_libraries(metrics2influx_v1 kspp_influxdb_s ${CSI_LIBS_SHARED})
else ()
    target_link_libraries(metrics2influx_v1 kspp_influxdb_s ${CSI_LIBS_STATIC})
endif ()

INSTALL(TARGETS metrics2influx_v1 RUNTIME DESTINATION bin)
