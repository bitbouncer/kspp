cmake_minimum_required(VERSION 2.8.11)

SET(CSI_INCLUDE_PATH 
${CMAKE_SOURCE_DIR}/../include
${CMAKE_SOURCE_DIR}/../rocksdb/include
)

if(WIN32)
SET(CSI_INCLUDE_PATH ${CSI_INCLUDE_PATH}
  ${CMAKE_SOURCE_DIR}/../zlib
  ${CMAKE_SOURCE_DIR}/../zlib/build
  ${CMAKE_SOURCE_DIR}/../openssl/include
  ${CMAKE_SOURCE_DIR}/../boost_1_62_0
)

SET(CSI_LIBRARY_PATH        
	${CMAKE_SOURCE_DIR}/../openssl
	${CMAKE_SOURCE_DIR}/../zlib/build
  ${CMAKE_SOURCE_DIR}/../rocksdb/build
	${CMAKE_SOURCE_DIR}/../boost_1_62_0/stage/lib/$(Platform)/lib
  ${CMAKE_SOURCE_DIR}/lib/$(Platform)
)
SET(LIBRARY_OUTPUT_PATH     ${CMAKE_SOURCE_DIR}/lib/$(Platform))
SET(EXECUTABLE_OUTPUT_PATH  ${CMAKE_SOURCE_DIR}/bin/$(Platform))
else()
set(LIBRARY_OUTPUT_PATH     ${CMAKE_SOURCE_DIR}/lib)
SET(EXECUTABLE_OUTPUT_PATH  ${CMAKE_SOURCE_DIR}/bin)
endif() 

#COMMON DEFINITIONS
add_definitions(-D_FILE_OFFSET_BITS=64 -D_REENTRANT -DEXTERNAL_LOCKS -DMULTITHREAD)
add_definitions(-DBOOST_ASIO_HAS_STD_CHRONO)

if(WIN32)
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
add_definitions(/wd4996) #'strdup': The POSIX name for this item is deprecated
add_definitions(/wd4197) # 'cf_atomic32' : top-level volatile in cast is ignored
add_definitions(/wd4200) #nonstandard extension used : zero-sized array in struct/union 
add_definitions(-DLIBRDKAFKA_STATICLIB)
add_definitions(-DBOOST_ALL_STATIC_LINK)
endif()

if(__LINUX__)
add_definitions(-D__LINUX__)
add_definitions(-fPIC)
add_definitions(-std=c++11)
add_definitions(-DBOOST_LOG_DYN_LINK)
add_definitions(-DBOOST_LOG_USE_NATIVE_SYSLOG)
add_definitions(-Wno-deprecated)
endif()

if(__DARWIN__)
add_definitions(-D__DARWIN__)
add_definitions(-fPIC)
add_definitions(-std=c++11)
include_directories(/opt/X11/include)
endif()

if(WIN32)
#SET(CSI_BOOST_LIBS)
#SET(CSI_LIBS librdkafkacpp librdkafka ${CSI_BOOST_LIBS} debug libprotobufd optimized libprotobuf rocksdb libssl64MT libcrypto64MT debug zlibstaticd optimized zlibstatic crypt32 Ws2_32 legacy_stdio_definitions)
SET(CSI_ROCKSDB_LIBS rocksdb rocksdblib Rpcrt4)
SET(ZLIB_LIBS debug zlibstaticd optimized zlibstatic)
SET(LIBRDKAFKA_LIBS librdkafkacpp librdkafka)
SET(CSI_LIBS kspp ${LIBRDKAFKA_LIBS} ${CSI_BOOST_LIBS} ${CSI_ROCKSDB_LIBS} libssl libcrypto ${ZLIB_LIBS} crypt32 Ws2_32 legacy_stdio_definitions)
endif()

if(__LINUX__)
set(CSI_BOOST_LIBS boost_log_setup boost_log boost_date_time boost_timer boost_thread boost_system boost_program_options boost_filesystem boost_regex boost_chrono boost_iostreams pthread rt c)
SET(CSI_ROCKSDB_LIBS rocksdb rocksdblib Rpcrt4)
SET(LIBRDKAFKA_LIBS librdkafkacpp librdkafka)
SET(CSI_LIBS kspp ${LIBRDKAFKA_LIBS} ${CSI_ROCKSDB_LIBS} ${CSI_BOOST_LIBS} crypto ssl)
endif()

#most likely wrong since I don't have a mac anymore...
#if(__DARWIN__)
#set(CSI_BOOST_LIBS boost_log_setup boost_log boost_date_time boost_timer boost_thread boost_system boost_program_options boost_filesystem boost_regex boost_chrono boost_iostreams pthread stdc++ c)
#endif()

include_directories(${CSI_INCLUDE_PATH} ${CMAKE_SOURCE_DIR})
link_directories(${CSI_LIBRARY_PATH})

#dirty fix to complile non supported static lib on windows...
if(WIN32)
file(GLOB librdkafka_files
  ${CMAKE_CURRENT_SOURCE_DIR}/../librdkafka/src/*.c
  ${CMAKE_CURRENT_SOURCE_DIR}/../librdkafka/src/*.h
  )

file(GLOB librdkafka_cpp_files
  ${CMAKE_CURRENT_SOURCE_DIR}/../librdkafka/src-cpp/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/../librdkafka/src-cpp/*.h
  )

list(REMOVE_ITEM librdkafka_files ${CMAKE_CURRENT_SOURCE_DIR}/../librdkafka/src/rdkafka_sasl.c)

add_library(librdkafka STATIC ${librdkafka_files})
add_library(librdkafkacpp STATIC ${librdkafka_cpp_files})
endif()

add_subdirectory(kspp)
add_subdirectory(examples)

