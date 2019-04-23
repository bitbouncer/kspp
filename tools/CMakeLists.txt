add_subdirectory(kspp_avrogencpp)

IF (ENABLE_INFLUXDB)
	add_subdirectory(kafka2influxdb)
ENDIF()

IF (ENABLE_POSTGRES)
add_subdirectory(postgres_connect)
ENDIF()

IF (ENABLE_ELASTICSEARCH)
    add_subdirectory(kafka2es)
ENDIF()

IF (ENABLE_TDS)
add_subdirectory(tds_connect)
ENDIF()

IF (ENABLE_ROCKSDB)
add_subdirectory(kafka2rocksdb)
ENDIF()

if (ENABLE_BITBOUNCER_STREAMING)
add_subdirectory(bitbouncer)
ENDIF()


