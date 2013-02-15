set (LIBRARY_NAME jsmysqludfs_merge)

find_path(MYSQL_INCLUDE mysql.h HINTS /usr/include /usr/include/mysql /opt/local/include /opt/local/include/mysql5/mysql/)
include_directories (SYSTEM ${MYSQL_INCLUDE})
add_subdirectory (${SRC_DIR}/mysql_udfs/merge)
