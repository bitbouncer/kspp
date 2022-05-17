add_subdirectory(kspp_avrogencpp)
add_subdirectory(kafka2avro)
add_subdirectory(csv2avro)

IF (ENABLE_ROCKSDB)
    add_subdirectory(kafka2rocksdb)
ENDIF ()

add_subdirectory(kspp_protobuf_register_schema)
