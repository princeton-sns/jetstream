#ifndef CUBE_6ITS9P4J
#define CUBE_6ITS9P4J

#include "../cube_impl.h"
#include "dimension.h"
#include "aggregate.h"
#include <boost/algorithm/string/join.hpp>

#include "mysql_connection.h"
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>

namespace jetstream {
namespace cube {
  
class MysqlCube : public DataCubeImpl<MysqlDimension, MysqlAggregate>{
  public: 
    MysqlCube(jetstream::CubeSchema _schema);
    MysqlCube(jetstream::CubeSchema _schema, string db_host, string db_user, string db_pass, string db_name);


    virtual bool insert_entry(jetstream::Tuple t);


    string create_sql();
    void create();
    void destroy();

    string get_table_name();
    vector<string> get_dimension_column_types();
    vector<string> get_aggregate_column_types();
  
  protected:
    boost::shared_ptr<sql::Connection> get_connection();
    void execute_sql(string sql);
    string get_insert_entry_prepared_sql();
    boost::shared_ptr<sql::PreparedStatement> get_insert_entry_prepared_statement();
    

  private:
    void init_connection();
    string db_host;
    string db_user;
    string db_pass;
    string db_name;
    boost::shared_ptr<sql::Connection> connection; 
    boost::shared_ptr<sql::Statement> statement;
    boost::shared_ptr<sql::PreparedStatement> insertEntryStatement;

};


} /* cube */
} /* jetstream */
#endif /* end of include guard: CUBE_6ITS9P4J */
