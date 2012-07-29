# Jetstream Build Script
#
# http://www.cmake.org/cmake/help/cmake_tutorial.html

cmake_minimum_required (VERSION 2.4)

set (CMAKE_VERBOSE_MAKEFILE ON)
set (BUILD_SHARED_LIBS ON)

# Add Compiler Definitions
#set (CMAKE_CXX_FLAGS "-g -O0 -Wall")
add_definitions ( -g -O0 -Wall -Werror )

set (TOP_LEVEL ${CMAKE_CURRENT_SOURCE_DIR})
set (SRC_DIR ${TOP_LEVEL}/src)
set (BUILD_DIR ${TOP_LEVEL}/src)

#######################################################
# Project flags

project (jetstream)

set (jetstream_VERSION_MAJOR "0" CACHE STRING "Jetstream's major version" FORCE)
set (jetstream_VERSION_MINOR "0" CACHE STRING "Jetstream's minor version" FORCE)
set (jetstream_VERSION_REVISION "1" CACHE STRING "Jetstream's revision version" FORCE)

set (jetstream_VERSION "${jetstream_VERSION_MAJOR}.${jetstream_VERSION_MINOR}.${jetstream_VERSION_REVISION}"  CACHE STRING "Jetstream's complete version"  FORCE)

# Generate a configuration file with these settings
configure_file (${SRC_DIR}/utils/js_version.h.in ${SRC_DIR}/utils/js_version.h)


#######################################################

# Requires Boost
find_package (Boost COMPONENTS system REQUIRED) 
if (APPLE)
  set (Boost_USE_STATIC_LIBS ON)
  set (Boost_USE_MULTITHREADED ON) 
endif ()
include_directories (${Boost_INCLUDE_DIRS})

# Require ProtoBufs
include (FindProtobuf)
find_package (Protobuf REQUIRED)
include_directories (${PROTOBUF_INCLUDE_DIR})

message (STATUS "before=${INCLUDE_DIRECTORIES}")

#######################################################
# Set library locations and sources


set (LIBRARY_NAME jsutils)
add_subdirectory (${SRC_DIR}/utils)
link_directories (${BUILD_DIR}/utils)
set (LIBRARIES ${LIBRARIES} ${LIBRARY_NAME})

include_directories (${SRC_DIR}/utils)

message (STATUS "after=${INCLUDE_DIRECTORIES}")

set (LIBRARY_NAME jsproto)
add_subdirectory (${SRC_DIR}/proto)
link_directories (${BUILD_DIR}/proto)
set (LIBRARIES ${LIBRARIES} ${LIBRARY_NAME})

include_directories (${SRC_DIR}/proto/cpp ${SRC_DIR}/cube ${SRC_DIR}/dataplane)

set (LIBRARY_NAME jsdataplane)
add_subdirectory (${SRC_DIR}/dataplane)
link_directories (${BUILD_DIR}/dataplane)
set (LIBRARIES ${LIBRARIES} ${LIBRARY_NAME})

#######################################################
# Build executables

set (LIBRARIES ${LIBRARIES}
    ${Boost_LIBRARIES}
    ${PROTOBUF_LIBRARY}
)

message (STATUS "Linking libraries ${LIBRARIES}")

set (JS_NODE_DAEMON ${TOP_LEVEL}/src/dataplane/nodedataplane_daemon.cc)
add_executable (jsnoded ${JS_NODE_DAEMON})
add_dependencies (jsnoded ${LIBRARIES})
target_link_libraries (jsnoded ${LIBRARIES})

# install (TARGETS jetstream DESTINATION bin)
