# Jetstream Build Script
#
# http://www.cmake.org/cmake/help/cmake_tutorial.html

cmake_minimum_required (VERSION 2.4)
cmake_policy(SET CMP0003 NEW)

# Add Compiler Definitions.  Must be done before project or language set.

option(USE_CLANG "build application with clang" ON)
option(FULL_STATIC "build application maximally statically" OFF)

if(USE_CLANG)
    SET (CMAKE_C_COMPILER             "/usr/bin/clang")
    SET (CMAKE_CXX_COMPILER           "/usr/bin/clang++")
#    SET (CMAKE_AR      "/usr/bin/llvm-ar")
#    SET (CMAKE_LINKER  "/usr/bin/llvm-ld")
#    SET (CMAKE_NM      "/usr/bin/llvm-nm")
#    SET (CMAKE_OBJDUMP "/usr/bin/llvm-objdump")
#    SET (CMAKE_RANLIB  "/usr/bin/llvm-ranlib")
else (USE_CLANG)
    SET (CMAKE_C_COMPILER             "/usr/bin/gcc")
    SET (CMAKE_CXX_COMPILER           "/usr/bin/g++")
endif(USE_CLANG)

message (STATUS "Compiler is ${CMAKE_CXX_COMPILER}")

set (CMAKE_VERBOSE_MAKEFILE ON)

if (FULL_STATIC)
    set (BUILD_SHARED_LIBS OFF)
    message (STATUS "library suffixes were ${CMAKE_FIND_LIBRARY_SUFFIXES} now .a;.so")
    set(CMAKE_FIND_LIBRARY_SUFFIXES .a;.so)
    set(Boost_USE_STATIC_LIBS ON)
else (FULL_STATIC)
    set (BUILD_SHARED_LIBS ON)
    set(Boost_USE_STATIC_LIBS OFF)
endif (FULL_STATIC)

SET (CMAKE_C_FLAGS                "-Wall -Werror -std=c99 -DNO_SSL ")
SET (CMAKE_C_FLAGS_DEBUG          "-g -O0")
SET (CMAKE_C_FLAGS_RELEASE        "-O4 -DNDEBUG")
SET (CMAKE_C_FLAGS_RELWITHDEBINFO "-O2 -g")

SET (CMAKE_CXX_FLAGS                "-Wall -DNO_SSL -fPIC -DGTEST_HAS_TR1_TUPLE=0")
#  -std=c++11 -DBOOST_NO_CXX11_RVALUE_REFERENCES
#  -D BOOST_NO_CXX11_NUMERIC_LIMITS=1  -DBOOST_NO_CXX11_RVALUE_REFERENCES
SET (CMAKE_CXX_FLAGS_DEBUG          "-g -O0 -Werror")
SET (CMAKE_CXX_FLAGS_RELEASE        "-O2 -DNDEBUG")
SET (CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g")

# NOTE: can't use -Werror in release mode since it means asserts don't count as
# variable uses, and that results in spurious warnings/errors.
add_definitions (${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_DEBUG})

set (TOP_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set (SRC_DIR ${TOP_DIR}/src)
set (BUILD_DIR ${TOP_DIR}/src)

set (TESTLIB "js_tests") #name for the library containing our unit tests

#######################################################
# Project flags

project (jetstream)

if(CMAKE_COMPILER_IS_GNUCC)
  set(CMAKE_INCLUDE_SYSTEM_FLAG_C "-isystem ")
endif()

set (JETSTREAM_VERSION_MAJOR "0" CACHE STRING "Jetstream's major version" FORCE)
set (JETSTREAM_VERSION_MINOR "0" CACHE STRING "Jetstream's minor version" FORCE)
set (JETSTREAM_VERSION_REVISION "1" CACHE STRING "Jetstream's revision version" FORCE)

set (JETSTREAM_VERSION "${JETSTREAM_VERSION_MAJOR}.${JETSTREAM_VERSION_MINOR}.${JETSTREAM_VERSION_REVISION}"  CACHE STRING "Jetstream's complete version"  FORCE)

# Generate a configuration file with these settings
configure_file (${SRC_DIR}/utils/js_version.h.in ${SRC_DIR}/utils/js_version.h)

#######################################################
#   Finding libraries


# Requires Boost
find_package (Boost COMPONENTS system thread chrono regex program_options REQUIRED)
if (APPLE)
  set (Boost_USE_STATIC_LIBS ON)
  set (Boost_USE_MULTITHREADED ON)
endif ()


include_directories (SYSTEM ${Boost_INCLUDE_DIRS})

#message ("after adding boost, includes were ${INCLUDE_DIRECTORIES}")

#hack because something messes up the suffixes
if (FULL_STATIC)
    set(CMAKE_FIND_LIBRARY_SUFFIXES .a;.so)
endif (FULL_STATIC)

# Require ProtoBufs
include (FindProtobuf)
find_package (Protobuf REQUIRED)
include_directories (SYSTEM ${PROTOBUF_INCLUDE_DIR})

# Require Google Test
include (FindGTest)
find_package (GTest REQUIRED)
include_directories (SYSTEM ${GTEST_INCLUDE_DIR})

#  Google's glog logger
find_library(GLOG_LIB glog) #glog has no dependencies and so this is safe. --ASR

# SQL connectors
include_directories (/opt/local/include/mysql5-connector-cpp/driver /opt/local/include/mysql5-connector-cpp)


### Build Masstree
#

#add_custom_command(
#     OUTPUT ${SRC_DIR}/utils/libmtree.a
add_custom_target(masstree
   COMMENT "Building Masstree"
    make 
    COMMAND cp ${TOP_DIR}/masstree_source/libmtree.a  ${TOP_DIR}/masstree_source/libmtree.dylib  ${SRC_DIR}/utils/
    DEPENDS ${TOP_DIR}/masstree_source/*.cc ${TOP_DIR}/masstree_source/*.hh ${TOP_DIR}/masstree_source/*.h ${TOP_DIR}/masstree_source/*.c
    WORKING_DIRECTORY ${TOP_DIR}/masstree_source)


#######################################################
# Set library locations and sources


set (LIBRARY_NAME jsproto)
add_subdirectory (${SRC_DIR}/proto)
link_directories (${BUILD_DIR}/proto)
set (LIBRARIES ${LIBRARIES} ${LIBRARY_NAME})
include_directories (SYSTEM ${SRC_DIR}/proto/cpp)


set (LIBRARY_NAME jsutils)
add_subdirectory (${SRC_DIR}/utils)
link_directories (${BUILD_DIR}/utils)
add_dependencies (${LIBRARY_NAME} ${SRC_DIR}/utils/libmtree.a)
set (LIBRARIES ${LIBRARIES} ${LIBRARY_NAME})

include_directories (${SRC_DIR}/utils)

set (LIBRARY_NAME jsaggregates)
add_subdirectory (${SRC_DIR}/aggregates)
link_directories (${BUILD_DIR}/aggregates)
set (LIBRARIES ${LIBRARIES} ${LIBRARY_NAME})

include_directories (${SRC_DIR}/aggregates)

set (LIBRARY_NAME jsmysqludfs)
add_subdirectory (${SRC_DIR}/mysql_udfs)

#mysql udfs should not be used by other code
#link_directories (${BUILD_DIR}/mysql_udfs)
#set (LIBRARIES ${LIBRARIES} ${LIBRARY_NAME})
#include_directories (${SRC_DIR}/aggregates)

set (LIBRARY_NAME jscomm)
add_subdirectory (${SRC_DIR}/comm)
link_directories (${BUILD_DIR}/comm)
set (LIBRARIES ${LIBRARIES} ${LIBRARY_NAME})

include_directories (${SRC_DIR}/comm)


add_subdirectory (${SRC_DIR}/mongoose)
link_directories (${BUILD_DIR}/mongoose)
set (LIBRARIES ${LIBRARIES} mongoose)
include_directories (${SRC_DIR}/mongoose)


# Dependencies are tricky, because protobuf .h files autogenerated, so we need
#  to make sure it's built before other libraries which include it
include_directories (${SRC_DIR}/cube ${SRC_DIR}/dataplane)

set (LIBRARY_NAME jscube)
add_subdirectory (${SRC_DIR}/cube)
link_directories (${BUILD_DIR}/cube)
set (LIBRARIES ${LIBRARIES} ${LIBRARY_NAME})
include_directories (${SRC_DIR}/cube/mysql)
include_directories (${SRC_DIR}/cube/masstree)
add_dependencies (${LIBRARY_NAME} masstree)


set (LIBRARY_NAME jsdataplane)
add_subdirectory (${SRC_DIR}/dataplane)
add_dependencies (${LIBRARY_NAME} jsproto)
link_directories (${BUILD_DIR}/dataplane)
set (LIBRARIES ${LIBRARIES} ${LIBRARY_NAME})

#add_subdirectory (${SRC_DIR}/cube)


#######################################################
# Use CLang by default



#######################################################
# Build executables

set (LIBRARIES ${LIBRARIES}
    ${Boost_LIBRARIES}
    ${PROTOBUF_LIBRARY}
    ${GTEST_LIBRARY}
    ${GLOG_LIB}
)

message (STATUS "Linking libraries ${LIBRARIES}")

#####  The worker process, jsnoded
set (JSNODED jsnoded)

add_executable (${JSNODED} ${SRC_DIR}/dataplane/node_daemon.cc)
target_link_libraries (${JSNODED} ${CMAKE_DL_LIBS})
add_dependencies (${JSNODED} jsproto) #A js-specific entry to add our protobuf generated code
target_link_libraries (${JSNODED} ${LIBRARIES})




###################################################
# Build the unit tests

file (GLOB_RECURSE TESTS RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "src/test_*.cc")

set (TESTS ${SRC_DIR}/js_unit_tests.cc ${TESTS})

add_executable (js_unit_tests ${TESTS})
target_link_libraries (js_unit_tests ${CMAKE_DL_LIBS})
target_link_libraries (js_unit_tests ${LIBRARIES})
target_link_libraries (js_unit_tests ${LIBRARY_NAME}) #the js node library


file (GLOB_RECURSE OPS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "src/*_operator.cc")
foreach(OP ${OPS})
  get_filename_component(OP_NAME ${OP} NAME_WE)
  target_link_libraries (js_unit_tests  ${OP_NAME})
endforeach(OP)
