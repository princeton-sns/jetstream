file (GLOB LIB_SRCS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cc")

add_library (${LIBRARY_NAME} ${LIB_SRCS})
target_link_libraries (${LIBRARY_NAME} ${Boost_LIBRARIES})

#set(OLD_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
#set(CMAKE_FIND_LIBRARY_SUFFIXES .a;.so)

target_link_libraries (${LIBRARY_NAME} jsproto_s)
target_link_libraries (${LIBRARY_NAME} jsaggregates_s)
#target_link_libraries (${LIBRARY_NAME} ${GLOG_LIB})

#set(CMAKE_FIND_LIBRARY_SUFFIXES ${OLD_SUFFIXES})
