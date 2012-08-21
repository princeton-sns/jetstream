#file (GLOB LIB_SRCS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cc")
#add_library (jscube ${LIB_SRCS})

###### The MYSQL prototype cubes
add_executable (protodb ${SRC_DIR}/cube/proto_db.cc)
if(APPLE)
find_library(MYSQL_CONN mysqlcppconn-static)
find_library(MYSQL_CLI mysql5/mysql/libmysqlclient.dylib)
target_link_libraries (protodb ${MYSQL_CONN})
target_link_libraries (protodb ${MYSQL_CLI}) 
else()
find_library(MYSQL_CONN NAMES mysqlcppconn HINTS /usr/lib/ /opt/local/lib/)
target_link_libraries (protodb ${MYSQL_CONN})
endif()
#target_link_libraries (protodb ${LIBRARIES})


add_executable (protodbfwriteonly ${SRC_DIR}/cube/proto_db_fwrite_only.cc)
