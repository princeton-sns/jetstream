cmake_minimum_required (VERSION 2.4)

#######################################################
# Require ProtoBufs
include (FindProtobuf)
find_package (Protobuf REQUIRED)
include_directories (${PROTOBUF_INCLUDE_DIR})

#######################################################

set (PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set (PROTO_GEN_DIR ${PROTO_DIR})

file (GLOB PROTO_FILES RELATIVE ${PROTO_DIR} "*.proto")

foreach (PROTO_FILE ${PROTO_FILES})
  get_filename_component (PROTO_NAME_ABS ${PROTO_FILE} ABSOLUTE)
  get_filename_component (PROTO_NAME ${PROTO_FILE} NAME_WE)

  set (PROTO_SRC ${PROTO_GEN_DIR}/cpp/${PROTO_NAME}.pb.cc)
  set (PROTO_HDR ${PROTO_GEN_DIR}/cpp/${PROTO_NAME}.pb.h)
  set (PROTO_PY ${PROTO_GEN_DIR}/python/${PROTO_NAME}_pb2.cc)

  add_custom_command(
    OUTPUT "${PROTO_SRC}" "${PROTO_HDR}"
    COMMAND  ${PROTOBUF_PROTOC_EXECUTABLE}
    ARGS --cpp_out  ${PROTO_GEN_DIR}/cpp --python_out ${PROTO_GEN_DIR}/python --proto_path ${PROTO_DIR} ${PROTO_NAME_ABS}
    DEPENDS ${PROTO_NAME_ABS}
    COMMENT "Running C++ protocol buffer compiler on ${PROTO_NAME_ABS}"
    VERBATIM 
  )

  set (PROTO_SRCS ${PROTO_SRCS} ${PROTO_SRC})
  set (PROTO_HDRS ${PROTO_HDRS} ${PROTO_HDR})
  set (PROTO_PYS ${PROTO_PYS} ${PROTO_PY})

endforeach (PROTO_FILE ${PROTO_FILES})

add_library (${LIBRARY_NAME} ${PROTO_SRCS})
target_link_libraries (${LIBRARY_NAME} ${PROTOBUF_LIBRARY})
