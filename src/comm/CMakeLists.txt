file (GLOB LIB_SRCS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cc")

add_library (jscomm ${LIB_SRCS})
target_link_libraries (${LIBRARY_NAME} ${Boost_LIBRARIES})
target_link_libraries (${LIBRARY_NAME} jsproto)


