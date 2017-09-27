cmake_minimum_required(VERSION 2.8.11)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

option(ENABLE_ROCKSDB "enable rocksdb support" OFF)
option(ENABLE_ZK "enable zk support" OFF)
option(BUILD_SAMPLES "build examples" OFF)
option(BUILD_TESTS "build tests" OFF)

set(KSPP_VERSION_MAJOR 1)
set(KSPP_VERSION_MINOR 0)

set(CMAKE_CXX_STANDARD 14)

#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wpedantic -g -O0")
#set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wpedantic -O2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")

# configure a header file to pass some of the CMake settings
# to the source code
configure_file(
        "${PROJECT_SOURCE_DIR}/kspp_config.h.in"
        "${PROJECT_BINARY_DIR}/kspp_config.h"
)
include_directories("${PROJECT_BINARY_DIR}")

if (WIN32)
    SET(CSI_INCLUDE_PATH
            ${CMAKE_SOURCE_DIR}/../include
            ${CMAKE_SOURCE_DIR}/../zlib
            ${CMAKE_SOURCE_DIR}/../zlib/build
            ${CMAKE_SOURCE_DIR}/../lz4/lib
            ${CMAKE_SOURCE_DIR}/../openssl/include
            ${CMAKE_SOURCE_DIR}/../boost
            ${CMAKE_SOURCE_DIR}/../glog/src/windows
            ${CMAKE_SOURCE_DIR}/../glog/build
            ${CMAKE_SOURCE_DIR}/../avro/lang/c++/include
            ${CMAKE_SOURCE_DIR}/../curl/include
            ${CMAKE_SOURCE_DIR}/../rapidjson/include
            )
    SET(CSI_LIBRARY_PATH
            ${CMAKE_SOURCE_DIR}/../openssl
            ${CMAKE_SOURCE_DIR}/../zlib/build
            ${CMAKE_SOURCE_DIR}/../boost/stage/lib/$(Platform)/lib
            ${CMAKE_SOURCE_DIR}/../glog/build/$(Platform)
            ${CMAKE_SOURCE_DIR}/../curl/libs/$(Platform)
            ${CMAKE_SOURCE_DIR}/../avro/lang/c++/build
            )

    if (ENABLE_ROCKSDB)
        SET(CSI_INCLUDE_PATH ${CSI_INCLUDE_PATH} ${CMAKE_SOURCE_DIR}/../rocksdb/include)
        SET(CSI_LIBRARY_PATH ${CSI_LIBRARY_PATH} ${CMAKE_SOURCE_DIR}/../rocksdb/build)
    endif ()

    SET(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/lib/$(Platform))
    SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin/$(Platform))

    add_definitions(-D_FILE_OFFSET_BITS=64 -D_REENTRANT -DEXTERNAL_LOCKS -DMULTITHREAD)
    add_definitions(-DBOOST_ASIO_HAS_STD_CHRONO)
    add_definitions(-DMARCH_x86_64)
    add_definitions(-D_WIN32_WINNT=0x0602) # win8.1 to get nthll 64 bit
    add_definitions(-DWINVER=0x0601)
    add_definitions(-DCF_WINDOWS)
    add_definitions(/MP)
    add_definitions(/bigobj)
    add_definitions(-D_UNICODE)
    add_definitions(-DUNICODE)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
    add_definitions(-D_SCL_SECURE_NO_WARNINGS)
    add_definitions(-DSECURITY_WIN32)
    add_definitions(/wd4996) # 'strdup': The POSIX name for this item is deprecated
    add_definitions(/wd4197) # 'cf_atomic32' : top-level volatile in cast is ignored
    add_definitions(/wd4200) # nonstandard extension used : zero-sized array in struct/union
    add_definitions(/wd4267) # 'argument': conversion from 'size_t' to 'int', possible loss of data  (AVRO is filled with those)
    add_definitions(/wd4090) # warning C4090: 'function': different 'const' qualifiers  (librdkafka usage of openssl)
    add_definitions(-DBOOST_ALL_STATIC_LINK)
    add_definitions(-DLIBRDKAFKA_STATICLIB)
    add_definitions(-DCURL_STATICLIB)
    add_definitions(-DGLOG_NO_ABBREVIATED_SEVERITIES)

    if (ENABLE_ROCKSDB)
        SET(CSI_ROCKSDB_LIBS rocksdb rocksdblib Rpcrt4)
    endif ()

    SET(CSI_AVRO_LIBS avrocpp_s libcurl)
    SET(ZLIB_LIBS debug zlibstaticd optimized zlibstatic)
    SET(LIBRDKAFKA_LIBS librdkafkacpp librdkafka lz4)
    SET(EXT_LIBS ${LIBRDKAFKA_LIBS} ${CSI_AVRO_LIBS} ${CSI_ROCKSDB_LIBS} ${CSI_BOOST_LIBS} glog libssl libcrypto ${ZLIB_LIBS} crypt32 Ws2_32 legacy_stdio_definitions)
    SET(CSI_LIBS kspp ${EXT_LIBS})

elseif (APPLE)
    find_package(Boost COMPONENTS program_options filesystem system REQUIRED)
    set(BOOST_LIBS
            ${Boost_PROGRAM_OPTIONS_LIBRARY}
            ${Boost_FILESYSTEM_LIBRARY}
            ${Boost_SYSTEM_LIBRARY}
            pthread
            c
            )
    SET(AVRO_LIBS avrocpp curl)

    set(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/lib)
    SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin)
    if (ENABLE_ROCKSDB)
        SET(CSI_ROCKSDB_LIBS rocksdb_lite snappy bz2 z)
    endif ()
    SET(LIBRDKAFKA_LIBS rdkafka++ rdkafka)
    SET(EXT_LIBS ${LIBRDKAFKA_LIBS} ${CSI_ROCKSDB_LIBS} ${AVRO_LIBS} ${BOOST_LIBS} glog crypto ssl)
    SET(CSI_LIBS kspp ${EXT_LIBS})
    #END APPLE


else () #LINUX
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

    SET(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/lib)
    SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin)
    add_definitions(-D_FILE_OFFSET_BITS=64 -D_REENTRANT -DEXTERNAL_LOCKS -DMULTITHREAD)
    add_definitions(-fPIC)
    add_definitions(-Wno-deprecated)
    SET(EXT_LIBS ${AVRO_LIBS} ${ROCKSDB_LIBS} ${LIBRDKAFKA_LIBS} ${LIBZK_LIBS} ${BOOST_LIBS} glog crypto ssl)
    SET(CSI_LIBS kspp ${EXT_LIBS})
endif () #LINUX

include_directories(${CSI_INCLUDE_PATH} ${CMAKE_SOURCE_DIR}/include)
link_directories(${CSI_LIBRARY_PATH})

#EXTRA WINDOWS TARGETS 
if (WIN32)
    file(GLOB liblz4_files
            ${CMAKE_CURRENT_SOURCE_DIR}/../lz4/lib/*.c
            ${CMAKE_CURRENT_SOURCE_DIR}/../lz4/lib/*.h
            )
    add_library(lz4 STATIC ${liblz4_files})

    #LIBRDKAFA
    file(GLOB librdkafka_c_files ${CMAKE_CURRENT_SOURCE_DIR}/../librdkafka/src/*.c)
    list(REMOVE_ITEM librdkafka_c_files ${CMAKE_CURRENT_SOURCE_DIR}/../librdkafka/src/rdkafka_sasl_cyrus.c)
    file(GLOB librdkafka_c_h_files ${CMAKE_CURRENT_SOURCE_DIR}/../librdkafka/src/*.h)
    file(GLOB librdkafka_cpp_files ${CMAKE_CURRENT_SOURCE_DIR}/../librdkafka/src-cpp/*.cpp)
    file(GLOB librdkafka_cpp_h_files ${CMAKE_CURRENT_SOURCE_DIR}/../librdkafka/src-cpp/*.h)
    file(MAKE_DIRECTORY ${CMAKE_SOURCE_DIR}/../include)
    file(MAKE_DIRECTORY ${CMAKE_SOURCE_DIR}/../include/librdkafka)
    file(COPY ${librdkafka_cpp_h_files} DESTINATION ${CMAKE_SOURCE_DIR}/../include/librdkafka)
    file(COPY ${librdkafka_c_h_files} DESTINATION ${CMAKE_SOURCE_DIR}/../include/librdkafka)
    add_library(librdkafka STATIC ${librdkafka_c_files} ${librdkafka_c_h_files})
    add_library(librdkafkacpp STATIC ${librdkafka_cpp_files} ${librdkafka_cpp_h_files})
endif () #WIN32

#TARGETS

file(GLOB LIB_SRCS
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
        )

file(GLOB KSPP_INCLUDE_FILES
        include/kspp/*.h
        include/kspp/avro/*.h
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
        include/kspp/beta/*.h
        )

ADD_LIBRARY(kspp_s STATIC ${LIB_SRCS} ${KSPP_INCLUDE_FILES})
INSTALL(TARGETS kspp_s
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        )

ADD_LIBRARY(kspp SHARED ${LIB_SRCS} ${KSPP_INCLUDE_FILES})
TARGET_LINK_LIBRARIES(kspp ${EXT_LIBS})
INSTALL(TARGETS kspp
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        )

INSTALL(DIRECTORY ${CMAKE_SOURCE_DIR}/include/ DESTINATION include)

if (BUILD_SAMPLES)
    add_subdirectory(examples)
endif ()

if (BUILD_TESTS)
    SET(EXECUTABLE_OUTPUT_PATH bin)
    include(CTest)
    enable_testing()
    add_subdirectory(tests)
endif ()


