SET(SIMPLE_NAME kspp-bitbouncer)

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

SET(BB_GRPC_LIBS_STATIC kspp_grpc_s gRPC::grpc++_reflection gRPC::grpc++ gRPC::grpc protobuf::libprotobuf)
SET(BB_GRPC_LIBS_SHARED kspp_grpc gRPC::grpc++_reflection gRPC::grpc++ gRPC::grpc protobuf::libprotobuf)

file(GLOB LIB_SRCS *.cpp grpc/*.cpp)

file(GLOB LIB_HEADERS *.h grpc/*.h)


if (BUILD_STATIC_LIBS)
    ADD_LIBRARY(${SIMPLE_NAME}_s STATIC ${PROTO_SRCS} ${GRPC_SRCS} ${BITBOUNCER_GRPC_LIB_SRCS})
    INSTALL(TARGETS ${SIMPLE_NAME}_s
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib
            )
endif ()

if (BUILD_SHARED_LIBS)
    ADD_LIBRARY(${SIMPLE_NAME} SHARED ${PROTO_SRCS} ${GRPC_SRCS} ${BITBOUNCER_GRPC_LIB_SRCS})
    INSTALL(TARGETS ${SIMPLE_NAME}
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib
            )
endif ()

INSTALL(FILES ${LIB_HEADERS} DESTINATION include/${SIMPLE_NAME})

IF (BUILD_TOOLS)
    add_subdirectory(tools)
ENDIF ()


