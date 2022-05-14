include_directories("${CMAKE_CURRENT_SOURCE_DIR}")

IF (ENABLE_MQTT)
    add_subdirectory(kspp-mqtt)
ENDIF ()

IF (ENABLE_ELASTICSEARCH)
    add_subdirectory(kspp-es)
ENDIF ()

IF (ENABLE_INFLUXDB)
    add_subdirectory(kspp-influx)
ENDIF ()

IF (ENABLE_POSTGRES)
    add_subdirectory(kspp-pg)
ENDIF ()

IF (ENABLE_TDS)
    add_subdirectory(kspp-tds)
ENDIF ()

IF (ENABLE_ROCKSDB)
    #    add_subdirectory(kafka2rocksdb)
ENDIF ()

if (ENABLE_BITBOUNCER_STREAMING)
    add_subdirectory(kspp-bitbouncer)
ENDIF ()




