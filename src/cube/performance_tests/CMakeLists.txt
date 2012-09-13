add_executable (performance_test performance_test.cc)
target_link_libraries (performance_test ${Boost_LIBRARIES})
target_link_libraries (performance_test jsproto)
target_link_libraries (performance_test jscube)
target_link_libraries (performance_test ${GLOG_LIB})
target_link_libraries (performance_test ${MYSQL_LIBS})

