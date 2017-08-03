file(GLOB LIB_SRCS
        metrics/*.cpp
        impl/*.cpp
        impl/sources/*.cpp
        impl/sinks/*.cpp
        impl/serdes/*.cpp
        impl/hash/*.cpp
        )

file(GLOB KSPP_INCLUDE_FILES
        app_info.h
        metrics.h
        kspp.h
        krecord.h
        kevent.h
        type_name.h
        topology_builder.h
        utils.h
        metrics/*.h
        sinks/*.h
        codecs/*.h
        processors/*.h
        state_stores/*.h
        impl/*.h
        impl/sources/*.h
        impl/sinks/*.h
        impl/serdes/*.h
        impl/hash/*.h
        )

if (ENABLE_AVRO)
    file(GLOB KSPP_AVRO_SRCS impl/serdes/avro/*.cpp)
    file(GLOB KSPP_AVRO_INCLUDE impl/serdes/avro/*.h)
endif ()

SET(KSPP_INCLUDE_FILES  ${KSPP_INCLUDE_FILES} ${KSPP_AVRO_INCLUDE} )

add_library(kspp_s STATIC ${LIB_SRCS} ${KSPP_AVRO_SRCS} ${KSPP_INCLUDE_FILES})

INSTALL(TARGETS kspp_s 
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

add_library(kspp SHARED ${LIB_SRCS} ${KSPP_AVRO_SRCS} ${KSPP_INCLUDE_FILES})

INSTALL(TARGETS kspp
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

INSTALL(DIRECTORY ${CMAKE_SOURCE_DIR}/kspp DESTINATION include)


