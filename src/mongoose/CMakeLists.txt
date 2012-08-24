cmake_minimum_required (VERSION 2.4)

file (GLOB MONGOOSE_SRCS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.c")

add_library (mongoose ${MONGOOSE_SRCS})

