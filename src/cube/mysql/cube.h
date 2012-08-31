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


    virtual bool insertEntry(jetstream::Tuple t);


    string createSql();
    void create();
    void destroy();

    string getTableName();
    vector<string> getDimensionColumnTypes();
    vector<string> getAggregateColumnTypes();
  
  protected:
    boost::shared_ptr<sql::Connection> getConnection();
    void executeSql(string sql);
    string getInsertEntryPreparedSql();
    boost::shared_ptr<sql::PreparedStatement> getInsertEntryPreparedStatement();
    

  private:
    void initConnection();
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
