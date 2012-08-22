#file (GLOB LIB_SRCS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cc")
#add_library (jscube ${LIB_SRCS})

###### The MYSQL prototype cubes
if(APPLE)
find_library(MYSQL_CONN mysqlcppconn-static)
find_library(MYSQL_CLI mysql5/mysql/libmysqlclient.dylib)
set(MYSQL_LIBS ${MYSQL_CONN}  ${MYSQL_CLI})
else()
find_library(MYSQL_CONN NAMES mysqlcppconn HINTS /usr/lib/ /opt/local/lib/)
set(MYSQL_LIBS ${MYSQL_CONN})
endif()

add_executable (protodb ${SRC_DIR}/cube/proto_db.cc)
target_link_libraries (protodb ${MYSQL_LIBS})
add_executable (protodbbatch ${SRC_DIR}/cube/proto_db_batch.cc)
target_link_libraries (protodbbatch ${MYSQL_LIBS})


add_executable (protodbfwriteonly ${SRC_DIR}/cube/proto_db_fwrite_only.cc)
add_executable (protodbfwriteonlynp ${SRC_DIR}/cube/proto_db_fwrite_only_no_parse.cc)
