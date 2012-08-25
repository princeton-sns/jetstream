###### The MYSQL prototype cubes
if(APPLE)
find_library(MYSQL_CONN mysqlcppconn-static)
find_library(MYSQL_CLI mysql5/mysql/libmysqlclient.dylib)
set(MYSQL_LIBS ${MYSQL_CONN}  ${MYSQL_CLI})
else()
find_library(MYSQL_CONN NAMES mysqlcppconn HINTS /usr/lib/ /opt/local/lib/)
set(MYSQL_LIBS ${MYSQL_CONN})
endif()

add_executable (protodb proto_db.cc)
target_link_libraries (protodb ${MYSQL_LIBS})
add_executable (protodbbatch proto_db_batch.cc)
target_link_libraries (protodbbatch ${MYSQL_LIBS})


add_executable (protodbfwriteonly proto_db_fwrite_only.cc)
add_executable (protodbfwriteonlynp proto_db_fwrite_only_no_parse.cc)
