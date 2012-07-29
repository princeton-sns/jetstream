cmake_minimum_required (VERSION 2.4)

# set (CMAKE_VERBOSE_MAKEFILE ON)

# Make a library out of everything except daemons, which we 
# will build as separate executables

file (GLOB DAEMONS "*_daemon.cc")
file (GLOB LIB_SRCS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cc")
list (REMOVE_ITEM LIB_SRCS ${DAEMONS})

add_library (${LIBRARY_NAME} ${LIB_SRCS})
