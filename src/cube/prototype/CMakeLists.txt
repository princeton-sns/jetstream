###### The MYSQL prototype cubes
add_executable (protodb proto_db.cc)
target_link_libraries (protodb ${MYSQL_LIBS})
add_executable (protodbbatch proto_db_batch.cc)
target_link_libraries (protodbbatch ${MYSQL_LIBS})


add_executable (protodbfwriteonly proto_db_fwrite_only.cc)
add_executable (protodbfwriteonlynp proto_db_fwrite_only_no_parse.cc)
