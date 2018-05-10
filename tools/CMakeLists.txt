add_subdirectory(kspp_avrogencpp)
#add_subdirectory(metrics2influx)
if (ENABLE_POSTGRES)
add_subdirectory(postgres_connect)
endif()

if (ENABLE_ELASTICSEARCH)
    add_subdirectory(elasticsearch_sink)
endif()


add_subdirectory(tds_connect)