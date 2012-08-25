file (GLOB LIB_SRCS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cc")
add_library (jscube ${LIB_SRCS})
target_link_libraries (jscube ${Boost_LIBRARIES})


add_subdirectory (prototype/)

