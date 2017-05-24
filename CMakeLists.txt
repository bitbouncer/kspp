cmake_minimum_required(VERSION 2.8.11)

option(ENABLE_AVRO    "enable avro support" OFF)
option(ENABLE_ROCKSDB "enable rocksdb support" OFF)
option(BUILD_SAMPLES  "build examples" OFF)
option(BUILD_TESTS    "build tests" OFF)

set(CMAKE_CXX_STANDARD 14)

if(WIN32)
SET(CSI_INCLUDE_PATH
  ${CMAKE_SOURCE_DIR}/../include
  ${CMAKE_SOURCE_DIR}/../zlib
  ${CMAKE_SOURCE_DIR}/../zlib/build
  ${CMAKE_SOURCE_DIR}/../lz4/lib
  ${CMAKE_SOURCE_DIR}/../openssl/include
  ${CMAKE_SOURCE_DIR}/../boost
)
SET(CSI_LIBRARY_PATH        
	${CMAKE_SOURCE_DIR}/../openssl
	${CMAKE_SOURCE_DIR}/../zlib/build
	${CMAKE_SOURCE_DIR}/../boost/stage/lib/$(Platform)/lib
)

if (ENABLE_ROCKSDB)
SET(CSI_INCLUDE_PATH ${CSI_INCLUDE_PATH} ${CMAKE_SOURCE_DIR}/../rocksdb/include) 
SET(CSI_LIBRARY_PATH ${CSI_LIBRARY_PATH} ${CMAKE_SOURCE_DIR}/../rocksdb/build) 
endif()

if (ENABLE_AVRO)	
SET(CSI_INCLUDE_PATH 
  ${CSI_INCLUDE_PATH} 
  ${CMAKE_SOURCE_DIR}/../avro/lang/c++/include 
  ${CMAKE_SOURCE_DIR}/../curl/include 
  ${CMAKE_SOURCE_DIR}/../rapidjson/include
  ${CMAKE_SOURCE_DIR}/../csi-async 
  ${CMAKE_SOURCE_DIR}/../csi-hcl-asio)
  
SET(CSI_LIBRARY_PATH 
  ${CSI_LIBRARY_PATH} 
  ${CMAKE_SOURCE_DIR}/../curl/libs/$(Platform) 
  ${CMAKE_SOURCE_DIR}/../avro/lang/c++/build)
endif()

SET(LIBRARY_OUTPUT_PATH     ${CMAKE_SOURCE_DIR}/lib/$(Platform))
SET(EXECUTABLE_OUTPUT_PATH  ${CMAKE_SOURCE_DIR}/bin/$(Platform))

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

if (ENABLE_ROCKSDB)
SET(CSI_ROCKSDB_LIBS rocksdb rocksdblib Rpcrt4)
endif()

if (ENABLE_AVRO)	
SET(CSI_AVRO_LIBS avrocpp_s libcurl)
endif()

SET(ZLIB_LIBS debug zlibstaticd optimized zlibstatic)
SET(LIBRDKAFKA_LIBS librdkafkacpp librdkafka lz4)

SET(CSI_LIBS kspp ${LIBRDKAFKA_LIBS} ${CSI_AVRO_LIBS} ${CSI_ROCKSDB_LIBS} ${CSI_BOOST_LIBS} libssl libcrypto ${ZLIB_LIBS} crypt32 Ws2_32 legacy_stdio_definitions)

elseif(APPLE)
add_definitions(-DBOOST_LOG_DYN_LINK)

set(LIBRARY_OUTPUT_PATH     ${CMAKE_SOURCE_DIR}/lib)
SET(EXECUTABLE_OUTPUT_PATH  ${CMAKE_SOURCE_DIR}/bin)

set(CSI_BOOST_LIBS boost_log_setup boost_log-mt boost_date_time boost_timer boost_thread-mt boost_system boost_program_options boost_filesystem boost_regex boost_chrono boost_iostreams pthread c)
if (ENABLE_ROCKSDB)
SET(CSI_ROCKSDB_LIBS rocksdb_lite snappy bz2 z)
endif()
SET(LIBRDKAFKA_LIBS rdkafka++ rdkafka)
SET(CSI_LIBS kspp ${LIBRDKAFKA_LIBS} ${CSI_ROCKSDB_LIBS} ${CSI_BOOST_LIBS} crypto ssl)
#END APPLE
else() #LINUX
find_package(Boost COMPONENTS log date_time timer thread program_options regex  system filesystem REQUIRED)

set(BOOST_LIBS 
  ${Boost_LOG_LIBRARY} 
  ${Boost_DATE_TIME_LIBRARY} 
  ${Boost_FILESYSTEM_LIBRARY} 
  ${Boost_TIMER_LIBRARY} 
  ${Boost_THREAD_LIBRARY} 
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  ${Boost_REGEXP_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
  ${Boost_FILESYSTEM_LIBRARY} 
  pthread 
  rt 
  c
)

SET(CSI_INCLUDE_PATH ${Boost_INCLUDE_DIR})

if (ENABLE_ROCKSDB)	
SET(CSI_INCLUDE_PATH ${CSI_INCLUDE_PATH} ${CMAKE_SOURCE_DIR}/../rocksdb/include)
SET(CSI_LIBRARY_PATH ${CSI_LIBRARY_PATH} ${CMAKE_SOURCE_DIR}/../rocksdb)
endif()

if (ENABLE_AVRO)	
SET(CSI_INCLUDE_PATH 
  ${CSI_INCLUDE_PATH} 
  ${CMAKE_SOURCE_DIR}/../avro/lang/c++/include 
  ${CMAKE_SOURCE_DIR}/../rapidjson/include
  ${CMAKE_SOURCE_DIR}/../csi-async 
  ${CMAKE_SOURCE_DIR}/../csi-hcl-asio
)
SET(CSI_LIBRARY_PATH ${CSI_LIBRARY_PATH} ${CMAKE_SOURCE_DIR}/../avro/lang/c++/build)
SET(CSI_AVRO_LIBS avrocpp_s curl)
endif() #AVRO

if (ENABLE_ROCKSDB)	
SET(CSI_ROCKSDB_LIBS rocksdb snappy bz2 z)
endif() #ROCKSDB

SET(LIBRARY_OUTPUT_PATH     ${CMAKE_SOURCE_DIR}/lib)
SET(EXECUTABLE_OUTPUT_PATH  ${CMAKE_SOURCE_DIR}/bin)
add_definitions(-D_FILE_OFFSET_BITS=64 -D_REENTRANT -DEXTERNAL_LOCKS -DMULTITHREAD)
add_definitions(-fPIC)
add_definitions(-DBOOST_LOG_DYN_LINK)
add_definitions(-Wno-deprecated)
SET(LIBRDKAFKA_LIBS rdkafka++ rdkafka)
SET(CSI_LIBS kspp ${CSI_AVRO_LIBS} ${CSI_ROCKSDB_LIBS} ${LIBRDKAFKA_LIBS} ${BOOST_LIBS} crypto ssl)
endif() #LINUX

include_directories(${CSI_INCLUDE_PATH} ${CMAKE_SOURCE_DIR})
link_directories(   ${CSI_LIBRARY_PATH})

#EXTRA WINDOWS TARGETS 
if(WIN32)
file(GLOB liblz4_files
  ${CMAKE_CURRENT_SOURCE_DIR}/../lz4/lib/*.c
  ${CMAKE_CURRENT_SOURCE_DIR}/../lz4/lib/*.h
  )
add_library(lz4 STATIC ${liblz4_files})

#LIBRDKAFA  
file(GLOB librdkafka_c_files        ${CMAKE_CURRENT_SOURCE_DIR}/../librdkafka/src/*.c)
list(REMOVE_ITEM librdkafka_c_files ${CMAKE_CURRENT_SOURCE_DIR}/../librdkafka/src/rdkafka_sasl_cyrus.c)
file(GLOB librdkafka_c_h_files      ${CMAKE_CURRENT_SOURCE_DIR}/../librdkafka/src/*.h)
file(GLOB librdkafka_cpp_files      ${CMAKE_CURRENT_SOURCE_DIR}/../librdkafka/src-cpp/*.cpp)
file(GLOB librdkafka_cpp_h_files    ${CMAKE_CURRENT_SOURCE_DIR}/../librdkafka/src-cpp/*.h)
file(MAKE_DIRECTORY ${CMAKE_SOURCE_DIR}/../include)
file(MAKE_DIRECTORY ${CMAKE_SOURCE_DIR}/../include/librdkafka)
file(COPY ${librdkafka_cpp_h_files} DESTINATION ${CMAKE_SOURCE_DIR}/../include/librdkafka)
file(COPY ${librdkafka_c_h_files} DESTINATION ${CMAKE_SOURCE_DIR}/../include/librdkafka)
add_library(librdkafka STATIC ${librdkafka_c_files} ${librdkafka_c_h_files})
add_library(librdkafkacpp STATIC ${librdkafka_cpp_files} ${librdkafka_cpp_h_files})
endif() #WIN32

#TARGETS
add_subdirectory(kspp)
if (ENABLE_AVRO)
add_subdirectory(tools)
endif()

if (BUILD_SAMPLES)
add_subdirectory(examples)
endif()

if (BUILD_TESTS)
SET(EXECUTABLE_OUTPUT_PATH  bin)
include(CTest)
enable_testing()
add_subdirectory(tests)
endif()


