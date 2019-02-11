cmake_minimum_required(VERSION 2.8.11)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

option(ENABLE_ROCKSDB "enable rocksdb support" ON)
option(ENABLE_ZK "enable zk support" OFF)
option(ENABLE_POSTGRES "enable postgres support" ON)
option(ENABLE_INFLUXDB "enable influxdb support" ON)
option(ENABLE_TDS "enable tds support" ON)
option(ENABLE_ELASTICSEARCH "enable elastic search support" ON)
option(BUILD_TOOLS "build tools" ON)
option(BUILD_SAMPLES "build examples" ON)
option(BUILD_STATIC_LIBS "enable static linking support" ON)
option(BUILD_SHARED_LIBS "enable static linking support" OFF)
option(BUILD_TESTS "build tests" ON)
option(LINK_SHARED "link shared" OFF)

set(KSPP_VERSION_MAJOR 1)
set(KSPP_VERSION_MINOR 0)

set(CMAKE_CXX_STANDARD 14)

#set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wpedantic -O2")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wpedantic -g -O0")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")

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
    #PostgreSQL_LIBRARIES
    #PostgreSQL_VERSION_STRING
endif () #POSTGRES

find_package(Boost COMPONENTS program_options filesystem system REQUIRED)
set(BOOST_LIBS
        ${Boost_PROGRAM_OPTIONS_LIBRARY}
        ${Boost_FILESYSTEM_LIBRARY}
        ${Boost_SYSTEM_LIBRARY}
        pthread
        rt
        c
        )

SET(CSI_INCLUDE_PATH ${Boost_INCLUDE_DIR})

if (ENABLE_ROCKSDB)
    find_package(RocksDB REQUIRED)
    SET(CSI_INCLUDE_PATH ${CSI_INCLUDE_PATH} ${ROCKSDB_INCLUDE_DIRS})
    SET(ROCKSDB_LIBS ${ROCKSDB_LIBRARIES} snappy bz2 z)
endif ()

find_package(rapidjson REQUIRED)
find_package(AvroCPP REQUIRED)

SET(CSI_INCLUDE_PATH
        ${CSI_INCLUDE_PATH}
        ${RAPIDJSON_INCLUDEDIR}
        ${AVRO_INCLUDE_DIRS})

#        SET(AVRO_LIBS avrocpp_s curl)
SET(AVRO_LIBS ${AVRO_LIBRARIES} curl)


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
SET(PROMETHEUS_CPP_LIBS prometheus-cpp-core prometheus-cpp-pull prometheus-cpp-push)


SET(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/lib)
SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin)
add_definitions(-g -D_FILE_OFFSET_BITS=64 -D_REENTRANT -DEXTERNAL_LOCKS -DMULTITHREAD)
add_definitions(-fPIC)
add_definitions(-Wno-deprecated)
SET(EXT_LIBS_SHARED ${AVRO_LIBS} ${ROCKSDB_LIBS} ${LIBRDKAFKA_LIBS} ${LIBZK_LIBS} ${PROMETHEUS_CPP_LIBS} ${BOOST_LIBS} ${PostgreSQL_LIBRARIES} glog crypto ssl)
SET(EXT_LIBS_STATIC ${AVRO_LIBS} ${ROCKSDB_LIBS} ${LIBRDKAFKA_LIBS} ${LIBZK_LIBS} ${PROMETHEUS_CPP_LIBS} ${BOOST_LIBS} ${PostgreSQL_LIBRARIES} glog crypto ssl)

SET(CSI_LIBS_SHARED kspp ${EXT_LIBS_SHARED})
SET(CSI_LIBS_STATIC kspp_s ${EXT_LIBS_STATIC})

include_directories(${CSI_INCLUDE_PATH} ${CMAKE_SOURCE_DIR}/include)
link_directories(${CSI_LIBRARY_PATH})

#TARGETS

file(GLOB LIB_SRCS
        src/*.cpp
        src/avro/*.cpp
        src/metrics/*.cpp
        src/impl/*.cpp
        src/impl/sources/*.cpp
        src/impl/sinks/*.cpp
        src/impl/serdes/*.cpp
        src/impl/hash/*.cpp
        src/impl/utils/*.cpp
        src/utils/*.cpp
        src/beta/*.cpp
        #        src/connect/kafka_rest/kafka_rest_source.cpp
        )

file(GLOB KSPP_INCLUDE_FILES
        include/kspp/*.h
        include/kspp/avro/*.h
        include/kspp/connect/avro_file/*.h
        include/kspp/connect/kafka_rest/*.h
        include/kspp/utils/*
        include/kspp/utils.h
        include/kspp/metrics/*.h
        include/kspp/sinks/*.h
        include/kspp/sources/*.h
        include/kspp/codecs/*.h
        include/kspp/processors/*.h
        include/kspp/state_stores/*.h
        include/kspp/impl/*.h
        include/kspp/impl/sources/*.h
        include/kspp/impl/sinks/*.h
        include/kspp/impl/serdes/*.h
        include/kspp/impl/hash/*.h
        include/kspp/impl/rocksdb/*.h
        include/kspp/beta/*.h
        )

if (BUILD_STATIC_LIBS)
    ADD_LIBRARY(kspp_s STATIC ${LIB_SRCS} ${KSPP_INCLUDE_FILES})
    INSTALL(TARGETS kspp_s
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib
            )
endif ()

if (BUILD_SHARED_LIBS)
    ADD_LIBRARY(kspp SHARED ${LIB_SRCS} ${KSPP_INCLUDE_FILES})
    INSTALL(TARGETS kspp
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib
            )
endif ()

#EXTENSIONS

IF (ENABLE_ROCKSDB)
    file(GLOB ROCKDB_LIB_SRCS
            src/impl/rocksdb/*.cpp)

    if (BUILD_STATIC_LIBS)
        ADD_LIBRARY(kspp_rocksdb_s STATIC ${ROCKDB_LIB_SRCS})
        INSTALL(TARGETS kspp_rocksdb_s
                LIBRARY DESTINATION lib
                ARCHIVE DESTINATION lib
                )
    endif ()

    if (BUILD_SHARED_LIBS)
        ADD_LIBRARY(kspp_rocksdb SHARED ${ROCKDB_LIB_SRCS})
        INSTALL(TARGETS kspp_rocksdb
                LIBRARY DESTINATION lib
                ARCHIVE DESTINATION lib
                )
    endif ()
ENDIF ()

if (ENABLE_POSTGRES)
    file(GLOB POSTGRES_LIB_SRCS
            include/connect/postgres/*.h
            src/connect/postgres/*.cpp)

    if (BUILD_STATIC_LIBS)
        ADD_LIBRARY(kspp_pg_s STATIC ${POSTGRES_LIB_SRCS})
        INSTALL(TARGETS kspp_pg_s
                LIBRARY DESTINATION lib
                ARCHIVE DESTINATION lib
                )
    endif ()

    if (BUILD_SHARED_LIBS)
        ADD_LIBRARY(kspp_pg SHARED ${POSTGRES_LIB_SRCS})
        INSTALL(TARGETS kspp_pg
                LIBRARY DESTINATION lib
                ARCHIVE DESTINATION lib
                )
    endif ()
ENDIF ()

IF (ENABLE_TDS)
    file(GLOB TDS_LIB_SRCS
            include/connect/tds/*.h
            src/connect/tds/*.cpp)

    if (BUILD_STATIC_LIBS)
        ADD_LIBRARY(kspp_tds_s STATIC ${TDS_LIB_SRCS})
        INSTALL(TARGETS kspp_tds_s
                LIBRARY DESTINATION lib
                ARCHIVE DESTINATION lib
                )
    endif ()

    if (BUILD_SHARED_LIBS)
        ADD_LIBRARY(kspp_tds SHARED ${TDS_LIB_SRCS})
        INSTALL(TARGETS kspp_tds
                LIBRARY DESTINATION lib
                ARCHIVE DESTINATION lib
                )
    endif ()
ENDIF ()

IF (ENABLE_ELASTICSEARCH)
    file(GLOB ELASTICSEARCH_LIB_SRCS
            include/connect/elasticsearch/*.h
            src/connect/elasticsearch/*.cpp)

    if (BUILD_STATIC_LIBS)
        ADD_LIBRARY(kspp_es_s STATIC ${ELASTICSEARCH_LIB_SRCS})
        INSTALL(TARGETS kspp_es_s
                LIBRARY DESTINATION lib
                ARCHIVE DESTINATION lib
                )
    endif ()

    if (BUILD_SHARED_LIBS)
        ADD_LIBRARY(kspp_es SHARED ${ELASTICSEARCH_LIB_SRCS})
        INSTALL(TARGETS kspp_es
                LIBRARY DESTINATION lib
                ARCHIVE DESTINATION lib
                )
    endif ()
ENDIF ()

#INFLUXDB
IF (ENABLE_INFLUXDB)
    file(GLOB INFLUXDB_LIB_SRCS
            include/connect/influxdb/*.h
            src/connect/influxdb/*.cpp)

    if (BUILD_STATIC_LIBS)
        ADD_LIBRARY(kspp_influxdb_s STATIC ${INFLUXDB_LIB_SRCS})
        INSTALL(TARGETS kspp_influxdb_s
                LIBRARY DESTINATION lib
                ARCHIVE DESTINATION lib
                )
    endif ()

    if (BUILD_SHARED_LIBS)
        ADD_LIBRARY(kspp_influxdb SHARED ${INFLUXDB_LIB_SRCS})
        INSTALL(TARGETS kspp_influxdb
                LIBRARY DESTINATION lib
                ARCHIVE DESTINATION lib
                )
    endif ()
ENDIF ()


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



