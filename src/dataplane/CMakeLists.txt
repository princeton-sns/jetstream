# Make a library out of everything except daemons and operators, which we 
# will build as separate executables

file (GLOB DAEMONS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*_daemon.cc")
file (GLOB OPS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*_operator.cc")
file (GLOB LIB_SRCS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cc")
list (REMOVE_ITEM LIB_SRCS ${DAEMONS})
list (REMOVE_ITEM LIB_SRCS ${OPS})

add_library (${LIBRARY_NAME} ${LIB_SRCS})

foreach(OP ${OPS})
  get_filename_component(OP_NAME ${OP} NAME_WE)
  add_library (${OP_NAME} ${OP})
  target_link_libraries (${OP_NAME} ${LIBRARY_NAME})
endforeach(OP)

add_executable (testloader ${SRC_DIR}/dataplane/testloader_daemon.cc)
#add_dependencies (testloader ${LIBRARY_NAME})
target_link_libraries (testloader ${LIBRARY_NAME})

