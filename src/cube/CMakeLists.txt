###### The MYSQL prototype cubes
if(APPLE)
find_library(MYSQL_CONN mysqlcppconn-static)
find_library(MYSQL_CLI mysql5/mysql/libmysqlclient.dylib)
set(MYSQL_LIBS ${MYSQL_CONN}  ${MYSQL_CLI})
else()
find_library(MYSQL_CONN NAMES mysqlcppconn HINTS /usr/lib/ /opt/local/lib/)
set(MYSQL_LIBS ${MYSQL_CONN})
endif()


file (GLOB LIB_SRCS1 RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cc")
file (GLOB LIB_SRCS2 RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "mysql/*.cc")
set(LIB_SRCS ${LIB_SRCS1} ${LIB_SRCS2})
add_library (jscube ${LIB_SRCS})
target_link_libraries (jscube ${Boost_LIBRARIES})
target_link_libraries (jscube jsproto)
target_link_libraries (jscube ${GLOG_LIB})
target_link_libraries (jscube ${MYSQL_LIBS})


#add_subdirectory (prototype/)
add_subdirectory (performance_tests/)

