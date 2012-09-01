# Make a library out of everything except daemons and operators, which we 
# will build as separate executables

file (GLOB DAEMONS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*_daemon.cc")
file (GLOB OPS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*_operator.cc")
file (GLOB TESTS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "test*.cc")
file (GLOB LIB_SRCS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cc")

list (REMOVE_ITEM LIB_SRCS ${DAEMONS})
list (REMOVE_ITEM LIB_SRCS ${OPS})
list (REMOVE_ITEM LIB_SRCS ${TESTS})

add_library (${LIBRARY_NAME} ${LIB_SRCS}) #building the jetstream library
target_link_libraries (${LIBRARY_NAME} ${Boost_LIBRARIES})
target_link_libraries (${LIBRARY_NAME} ${GLOG_LIB})
target_link_libraries (${LIBRARY_NAME} jsutils)
target_link_libraries (${LIBRARY_NAME} jsproto)
target_link_libraries (${LIBRARY_NAME} jscomm)
target_link_libraries (${LIBRARY_NAME} mongoose)
target_link_libraries (${LIBRARY_NAME} jscube)


foreach(OP ${OPS})
  get_filename_component(OP_NAME ${OP} NAME_WE)
  add_library (${OP_NAME} ${OP})
  target_link_libraries (${OP_NAME} ${LIBRARY_NAME})
endforeach(OP)
