cmake_minimum_required(VERSION 3.5)
project(kspp)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

#FEATURES
option(ENABLE_POSTGRES "enable postgres support" ON)
option(ENABLE_INFLUXDB "enable influxdb support" ON)
option(ENABLE_TDS "enable tds support" ON)
option(ENABLE_ELASTICSEARCH "enable elastic search support" ON)
option(ENABLE_MQTT "enable MQTT support" ON)
option(ENABLE_BITBOUNCER_STREAMING "enable bitbouncer streaming support" OFF)


option(ENABLE_ROCKSDB "enable rocksdb support" ON)
option(ENABLE_ZK "enable zk support" OFF)
option(ENABLE_S3 "enable S3 support" ON)
#option(ENABLE_KINESIS "enable KINESIS support" ON)


option(BUILD_TOOLS "build tools" ON)
option(BUILD_SAMPLES "build examples" ON)
option(BUILD_SHARED_LIBS "build shared libs" OFF)
option(BUILD_STATIC_LIBS "build static libs" ON)

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
#configure_file(
#        "${PROJECT_SOURCE_DIR}/kspp_config.h.in"
#        "${PROJECT_BINARY_DIR}/kspp_config.h"
#)

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

find_package(absl REQUIRED)

if (ENABLE_ROCKSDB)
    find_package(RocksDB REQUIRED)
    SET(CSI_INCLUDE_PATH ${CSI_INCLUDE_PATH} ${ROCKSDB_INCLUDE_DIRS})
    SET(ROCKSDB_LIBS ${ROCKSDB_LIBRARIES} snappy bz2 z dl)
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
        ${AVRO_INCLUDE_DIRS}
        )

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


SET(LIBRDKAFKA_LIBS rdkafka++ rdkafka crypto ssl lz4 zstd z)
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
        src/utils/*.cpp
        src/beta/*.cpp

        IF(ENABLE_ROCKSDB)
        src/internal/rocksdb/*.cpp
        endif ()
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
        if (ENABLE_ROCKSDB)
        include/kspp/internal/rocksdb/*.h
        endif()
        include/kspp/internal/grpc/*.h
        include/kspp/beta/*.h
        )

if (ENABLE_ROCKSDB)
    add_definitions(-DKSPP_ROCKSDB)
endif()

if (ENABLE_S3)
    add_definitions(-DKSPP_S3)
    file(GLOB S3_LIB_SRCS
            include/features/aws/*.h
            src/features/aws/*.cpp)
    SET(AWS_S3_LIBS aws-cpp-sdk-s3 aws-cpp-sdk-core aws-c-event-stream aws-checksums aws-c-common)
endif ()

IF (ENABLE_ROCKSDB)
    add_definitions(-DKSPP_ROCKSDB)
    file(GLOB ROCKDB_LIB_SRCS
     src/internal/rocksdb/*.cpp)
ENDIF()

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

if (ENABLE_GRPC)
 SET(BB_GRPC_LIBS gRPC::grpc++_reflection gRPC::grpc++ gRPC::grpc protobuf::libprotobuf gpr upb address_sorting re2 crypto ssl)
endif()

SET(ABSEIL_LIBS absl::time absl::status absl::synchronization absl::strings absl::str_format absl::memory absl::base)

SET(EXT_LIBS ${AVRO_LIBS} ${ROCKSDB_LIBS} ${LIBRDKAFKA_LIBS} ${LIBZK_LIBS} ${PROMETHEUS_CPP_LIBS} ${BOOST_LIBS} ${PostgreSQL_LIBRARIES} ${AWS_S3_LIBS} ${MQTT_LIBS} ${AWS_KINESIS_LIBS} ${TDS_LIBS} ${BB_GRPC_LIBS} ${ABSEIL_LIBS} gflags glog stdc++fs curl z zstd pthread)
#crypto ssl


if (LINK_SHARED)
    SET(ALL_LIBS ${BB_GRPC_LIBS_SHARED} kspp ${EXT_LIBS})
else()
    SET(ALL_LIBS ${BB_GRPC_LIBS_STATIC} kspp_s ${EXT_LIBS})
endif()

INSTALL(DIRECTORY ${CMAKE_SOURCE_DIR}/include/ DESTINATION include)

add_subdirectory(libraries)

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







