add_executable(kafka2influxdb kafka2influxdb.cpp)

if (LINK_SHARED)
    target_link_libraries(kafka2influxdb kspp_influxdb_s ${CSI_LIBS_SHARED})
else ()
    target_link_libraries(kafka2influxdb kspp_influxdb_s ${CSI_LIBS_STATIC})
endif ()

INSTALL(TARGETS kafka2influxdb RUNTIME DESTINATION bin)
