cmake_minimum_required (VERSION 2.4)

# set (CMAKE_VERBOSE_MAKEFILE ON)

file (GLOB LIB_SRCS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cc")

add_library (jscube ${LIB_SRCS})
