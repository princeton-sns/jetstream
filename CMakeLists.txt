# http://www.cmake.org/cmake/help/cmake_tutorial.html

cmake_minimum_required (VERSION 2.6)

project (jetstream)

set (jetstream_VERSION_MAJOR 0)
set (jetstream_VERSION_MINOR 1)

include_directories ("${PROJECT_BINARY_DIR}/src/proto/cpp/")
include_directories ("${PROJECT_BINARY_DIR}/src/proto/python/")

add_subdirectory (src) 
add_subdirectory (src/dataplane)
add_subdirectory (src/cube)

# add_executable (jsdatad jsdatad.cc)

# install (TARGETS jetstream DESTINATION bin)
