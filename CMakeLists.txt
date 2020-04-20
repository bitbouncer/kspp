cmake_minimum_required(VERSION 3.5)
project(kspp)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

#FEATURES
option(ENABLE_ROCKSDB "enable rocksdb support" ON)
option(ENABLE_ZK "enable zk support" OFF)
option(ENABLE_POSTGRES "enable postgres support" ON)
option(ENABLE_INFLUXDB "enable influxdb support" ON)
option(ENABLE_TDS "enable tds support" ON)
option(ENABLE_ELASTICSEARCH "enable elastic search support" ON)
option(ENABLE_S3 "enable S3 support" ON)
option(ENABLE_KINESIS "enable KINESIS support" ON)
option(ENABLE_MQTT "enable MQTT support" ON)
option(ENABLE_GRPC "enable grpc support" ON)
option(ENABLE_BITBOUNCER_STREAMING "enable bitbouncer streaming support" ON)

option(BUILD_TOOLS "build tools" ON)
option(BUILD_SAMPLES "build examples" ON)
option(BUILD_STATIC_LIBS "enable static linking support" ON)
option(BUILD_SHARED_LIBS "enable static linking support" OFF)
option(BUILD_TESTS "build tests" ON)
option(LINK_SHARED "link shared" OFF)

set(KSPP_VERSION_MAJOR 1)
set(KSPP_VERSION_MINOR 0)

set(CMAKE_CXX_STANDARD 17)

#set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wpedantic -O2")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wpedantic -Wreturn-type -g -O2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -Wpedantic -Wreturn-type -g -O0")

# configure a header file to pass some of the CMake settings
# to the source code
configure_file(
        "${PROJECT_SOURCE_DIR}/kspp_config.h.in"
        "${PROJECT_BINARY_DIR}/kspp_config.h"
)
include_directories("${PROJECT_BINARY_DIR}")


if (ENABLE_POSTGRES)
    find_package(PostgreSQL REQUIRED)
    include_directories(${PostgreSQL_INCLUDE_DIRS})
endif ()

find_package(Boost COMPONENTS program_options system REQUIRED)

#it seems we cannot longer find boost 1.7.0 with cmake???
#set(BOOST_LIBS
#        ${Boost_PROGRAM_OPTIONS_LIBRARY}
#        ${Boost_SYSTEM_LIBRARY}
#        pthread
#        rt
#        c
#        )

set(BOOST_LIBS boost_program_options boost_iostreams boost_filesystem boost_system boost_regex z bz2 pthread rt c)


SET(CSI_INCLUDE_PATH ${Boost_INCLUDE_DIR})

if (ENABLE_ROCKSDB)
    find_package(RocksDB REQUIRED)
    SET(CSI_INCLUDE_PATH ${CSI_INCLUDE_PATH} ${ROCKSDB_INCLUDE_DIRS})
    SET(ROCKSDB_LIBS ${ROCKSDB_LIBRARIES} snappy bz2 z)
endif ()

find_package(rapidjson REQUIRED)
find_package(AvroCPP REQUIRED)

find_package(Snappy)
if (SNAPPY_FOUND)
    set(SNAPPY_PKG libsnappy)
    add_definitions(-DSNAPPY_CODEC_AVAILABLE)
    message("Enabled snappy codec")
else (SNAPPY_FOUND)
    set(SNAPPY_PKG "")
    set(SNAPPY_LIBRARIES "")
    message("Disabled snappy codec. libsnappy not found.")
endif (SNAPPY_FOUND)

SET(CSI_INCLUDE_PATH
        ${CSI_INCLUDE_PATH}
        ${RAPIDJSON_INCLUDEDIR}
        ${AVRO_INCLUDE_DIRS})

#        SET(AVRO_LIBS avrocpp_s curl)
SET(AVRO_LIBS ${AVRO_LIBRARIES} curl ${SNAPPY_LIBRARIES})

if (ENABLE_ZK)
    SET(CSI_INCLUDE_PATH
            ${CSI_INCLUDE_PATH}
            ${CMAKE_SOURCE_DIR}/../zk/src
            )
    file(GLOB zk_lib_files
            ${CMAKE_CURRENT_SOURCE_DIR}/../zk/src/*.*
            )
    add_library(csi-zk STATIC ${zk_lib_files})
    SET(LIBZK_LIBS csi-zk)
endif () #ZK


SET(LIBRDKAFKA_LIBS rdkafka++ rdkafka)
SET(PROMETHEUS_CPP_LIBS prometheus-cpp-core prometheus-cpp-push)

SET(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/lib)
SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin)
add_definitions(-g -D_FILE_OFFSET_BITS=64 -D_REENTRANT -DEXTERNAL_LOCKS -DMULTITHREAD)
add_definitions(-fPIC)
add_definitions(-Wno-deprecated)

include_directories(${CSI_INCLUDE_PATH} ${CMAKE_SOURCE_DIR}/include)
link_directories(${CSI_LIBRARY_PATH})

#TARGETS

file(GLOB LIB_SRCS
        src/*.cpp
        src/avro/*.cpp
        src/metrics/*.cpp
        src/internal/*.cpp
        src/internal/sources/*.cpp
        src/internal/sinks/*.cpp
        src/internal/serdes/*.cpp
        src/internal/hash/*.cpp
        src/internal/utils/*.cpp
        #        src/internal/grpc/*.cpp
        src/utils/*.cpp
        src/beta/*.cpp

        IF(ENABLE_ROCKSDB)
        src/internal/rocksdb/*.cpp
        endif ()

        if (ENABLE_S3)
        include/features/aws/*.h
        src/features/aws/*.cpp
        endif()
        #        src/connect/kafka_rest/kafka_rest_source.cpp
        )

file(GLOB KSPP_INCLUDE_FILES
        include/kspp/*.h
        include/kspp/avro/*.h
        include/kspp/connect/avro_file/*.h
        include/kspp/utils/*
        include/kspp/utils.h
        include/kspp/metrics/*.h
        include/kspp/sinks/*.h
        include/kspp/sources/*.h
        include/kspp/codecs/*.h
        include/kspp/processors/*.h
        include/kspp/state_stores/*.h
        include/kspp/internal/*.h
        include/kspp/internal/sources/*.h
        include/kspp/internal/sinks/*.h
        include/kspp/internal/serdes/*.h
        include/kspp/internal/hash/*.h
        include/kspp/internal/rocksdb/*.h
        include/kspp/internal/grpc/*.h
        include/kspp/beta/*.h
        )

if (ENABLE_S3)
    file(GLOB S3_LIB_SRCS
            include/features/aws/*.h
            src/features/aws/*.cpp)
    SET(AWS_S3_LIBS aws-cpp-sdk-s3 aws-cpp-sdk-core aws-c-event-stream aws-checksums aws-c-common)
    add_definitions(-DKSPP_S3)
endif ()

#EXTENSIONS

IF (ENABLE_ROCKSDB)
    file(GLOB ROCKDB_LIB_SRCS
     src/internal/rocksdb/*.cpp)
ENDIF ()

if (ENABLE_POSTGRES)
    file(GLOB POSTGRES_LIB_SRCS
            include/connect/postgres/*.h
            src/connect/postgres/*.cpp)
ENDIF ()

IF (ENABLE_TDS)
    file(GLOB TDS_LIB_SRCS
            include/connect/tds/*.h
            src/connect/tds/*.cpp)
    SET(TDS_LIBS sybdb)
ENDIF ()

IF (ENABLE_ELASTICSEARCH)
    file(GLOB ELASTICSEARCH_LIB_SRCS
            include/connect/elasticsearch/*.h
            src/connect/elasticsearch/*.cpp)
ENDIF ()

IF (ENABLE_INFLUXDB)
    file(GLOB INFLUXDB_LIB_SRCS
            include/connect/influxdb/*.h
            src/connect/influxdb/*.cpp)
ENDIF ()

if (ENABLE_KINESIS)
    file(GLOB KINESIS_LIB_SRCS
            include/connect/aws/*.h
            src/connect/aws/*.cpp)
    SET(AWS_KINESIS_LIBS aws-cpp-sdk-kinesis aws-cpp-sdk-s3 aws-cpp-sdk-core aws-c-event-stream aws-checksums aws-c-common)
endif () #ENABLE_KINESIS

if (ENABLE_MQTT)
    file(GLOB MQTT_LIB_SRCS
            include/connect/mqtt/*.h
            src/connect/mqtt/*.cpp)
    SET(MQTT_LIBS paho-mqttpp3 paho-mqtt3cs)
endif ()


if (ENABLE_GRPC)
    find_package(Protobuf REQUIRED)
    message(STATUS "Using protobuf ${protobuf_VERSION}")
    find_package(GRPC REQUIRED)

    SET(PROTOS ./proto/bb_streaming.proto)
    set(PROTO_SRC_DIR ${CMAKE_CURRENT_BINARY_DIR})

    PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS ${PROTOS})

    # Print path to generated files
    message("PROTO_SRCS = ${PROTO_SRCS}")
    message("PROTO_HDRS = ${PROTO_HDRS}")

    grpc_generate_cpp(GRPC_SRCS GRPC_HDRS ${PROTO_SRC_DIR} ${PROTOS})
    message("GRPC_SRCS = ${GRPC_SRCS}")
    message("GRPC_HDRS = ${GRPC_HDRS}")

    file(GLOB BITBOUNCER_GRPC_LIB_SRCS
            include/internal/grpc/*.h
            src/internal/grpc/*.cpp)

    if (BUILD_STATIC_LIBS)
        ADD_LIBRARY(kspp_grpc_s STATIC ${PROTO_SRCS} ${GRPC_SRCS} ${BITBOUNCER_GRPC_LIB_SRCS})
        INSTALL(TARGETS kspp_grpc_s
                LIBRARY DESTINATION lib
                ARCHIVE DESTINATION lib
                )
    endif ()

    if (BUILD_SHARED_LIBS)
        ADD_LIBRARY(kspp_grpc SHARED ${PROTO_SRCS} ${GRPC_SRCS} ${BITBOUNCER_GRPC_LIB_SRCS})
        INSTALL(TARGETS kspp_grpc
                LIBRARY DESTINATION lib
                ARCHIVE DESTINATION lib
                )
    endif ()

    file(GLOB BITBOUNCER_LIB_SRCS
            include/connect/bitbouncer/*.h)

    SET(BB_GRPC_LIBS_STATIC kspp_grpc_s gRPC::grpc++_reflection gRPC::grpc++ gRPC::grpc protobuf::libprotobuf)
    SET(BB_GRPC_LIBS_SHARED kspp_grpc gRPC::grpc++_reflection gRPC::grpc++ gRPC::grpc protobuf::libprotobuf)
endif () #ENABLE_GRPC


if (BUILD_STATIC_LIBS)
    ADD_LIBRARY(kspp_s STATIC ${LIB_SRCS} ${KSPP_INCLUDE_FILES} ${S3_LIB_SRCS} ${MQTT_LIB_SRCS} ${ROCKDB_LIB_SRCS} ${POSTGRES_LIB_SRCS} ${TDS_LIB_SRCS} ${ELASTICSEARCH_LIB_SRCS} ${INFLUXDB_LIB_SRCS} ${KINESIS_LIB_SRCS})
    INSTALL(TARGETS kspp_s
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib
            )
endif ()

if (BUILD_SHARED_LIBS)
    ADD_LIBRARY(kspp SHARED ${LIB_SRCS} ${KSPP_INCLUDE_FILES} ${S3_LIB_SRCS} ${MQTT_LIB_SRCS} ${ROCKDB_LIB_SRCS} ${POSTGRES_LIB_SRCS} ${TDS_LIB_SRCS} ${ELASTICSEARCH_LIB_SRCS} ${INFLUXDB_LIB_SRCS} ${KINESIS_LIB_SRCS})
    INSTALL(TARGETS kspp
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib
            )
endif ()

SET(EXT_LIBS_SHARED ${AVRO_LIBS} ${ROCKSDB_LIBS} ${LIBRDKAFKA_LIBS} ${LIBZK_LIBS} ${PROMETHEUS_CPP_LIBS} ${BOOST_LIBS} ${PostgreSQL_LIBRARIES} ${AWS_S3_LIBS} ${BB_GRPC_LIBS_SHARED} ${MQTT_LIBS} ${AWS_KINESIS_LIBS} ${TDS_LIBS} glog gflags crypto ssl stdc++fs)
SET(EXT_LIBS_STATIC ${AVRO_LIBS} ${ROCKSDB_LIBS} ${LIBRDKAFKA_LIBS} ${LIBZK_LIBS} ${PROMETHEUS_CPP_LIBS} ${BOOST_LIBS} ${PostgreSQL_LIBRARIES} ${AWS_S3_LIBS} ${BB_GRPC_LIBS_STATIC} ${MQTT_LIBS} ${AWS_KINESIS_LIBS} ${TDS_LIBS} glog gflags crypto ssl stdc++fs)
SET(CSI_LIBS_SHARED kspp ${EXT_LIBS_SHARED})
SET(CSI_LIBS_STATIC kspp_s ${EXT_LIBS_STATIC})

INSTALL(DIRECTORY ${CMAKE_SOURCE_DIR}/include/ DESTINATION include)

IF (BUILD_TOOLS)
    add_subdirectory(tools)
ENDIF ()

IF (BUILD_SAMPLES)
    add_subdirectory(examples)
ENDIF ()

IF (BUILD_TESTS)
    SET(EXECUTABLE_OUTPUT_PATH bin)
    include(CTest)
    enable_testing()
    add_subdirectory(tests)
ENDIF ()





