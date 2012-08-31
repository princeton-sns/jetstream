

#include "cube.h"

using namespace ::std;


jetstream::cube::MysqlCube::MysqlCube(jetstream::CubeSchema _schema) : 
      DataCubeImpl<MysqlDimension, MysqlAggregate>(_schema), 
      db_host("localhost"),
      db_user("root"),
      db_pass(""),
      db_name("test_cube")
      { initConnection(); }

      jetstream::cube::MysqlCube::MysqlCube(jetstream::CubeSchema _schema, string db_host, string db_user, string db_pass, string db_name) : 
      DataCubeImpl<MysqlDimension, MysqlAggregate>(_schema), 
      db_host(db_host),
      db_user(db_user),
      db_pass(db_pass),
      db_name(db_name)
      { initConnection(); }

void jetstream::cube::MysqlCube::initConnection()
{
  sql::Driver * driver = get_driver_instance();
  shared_ptr<sql::Connection> con(driver->connect(db_host, db_user, db_pass));
  connection = con;
  connection->setSchema(db_name);

  shared_ptr<sql::Statement> stmnt(connection->createStatement());
  statement = stmnt;
}

void jetstream::cube::MysqlCube::executeSql(string sql)
{
  statement->execute(sql);
}


boost::shared_ptr<sql::Connection> jetstream::cube::MysqlCube::getConnection()
{
  return connection;
}

string jetstream::cube::MysqlCube::createSql() {
  string sql = "CREATE TABLE `"+getTableName()+"` (";
  vector<string> pk;
  for(size_t i=0; i<dimensions.size(); i++)
  {
    vector<string> names = dimensions[i]->getColumnNames();
    vector<string> types = dimensions[i]->getColumnTypes();
    for (size_t j = 0; j < names.size(); j++) {
      sql += "`"+names[j]+"` " + types[j] + " NOT NULL,";
      pk.push_back("`"+names[j]+"`");
    }
  }

  for(size_t i=0; i<aggregates.size(); i++)
  {
    vector<string> names = aggregates[i]->getColumnNames();
    vector<string> types = aggregates[i]->getColumnTypes();
    for (size_t j = 0; j < names.size(); j++) {
      sql += "`"+names[j]+"` " + types[j] + " DEFAULT NULL,";
    }
  }
  sql += "PRIMARY KEY (";
  sql += boost::algorithm::join(pk, ", ");
  sql += ")";
  sql += ") ENGINE=MyISAM";
  return sql;

}

void jetstream::cube::MysqlCube::create()
{
  executeSql(createSql());
}

void jetstream::cube::MysqlCube::destroy()
{
  executeSql("DROP TABLE `"+getTableName()+"`");
}


string jetstream::cube::MysqlCube::getTableName()
{
  return name;
}

vector<string> jetstream::cube::MysqlCube::getDimensionColumnTypes()
{
  vector<string> cols;
  size_t i;
  for (i = 0; i < dimensions.size() ; i++) {
    shared_ptr<MysqlDimension> dim =dimensions[i];
    vector<string> tmp = dim->getColumnTypes();
    for (size_t j = 0; j < tmp.size(); j++) {
      cols.push_back(tmp[j]);
    }
  }
  return cols;
}

vector<string> jetstream::cube::MysqlCube::getAggregateColumnTypes()
{
  vector<string> cols;
  size_t i;
  for (i = 0; i < aggregates.size() ; i++) {
    shared_ptr<MysqlAggregate> agg =aggregates[i];
    vector<string> tmp = agg->getColumnTypes();
    for (size_t j = 0; j < tmp.size(); j++) {
      cols.push_back(tmp[j]);
    }
  }
  return cols;
}


string jetstream::cube::MysqlCube::getInsertEntryPreparedSql()
{
  vector<string> column_names;
  vector<string> column_values;
  vector<string> updates;
  for(size_t i=0; i<dimensions.size(); i++)
  {
    vector<string> names = dimensions[i]->getColumnNames();
    for (size_t j = 0; j < names.size(); j++) {
      column_names.push_back("`"+names[j]+"`");
      column_values.push_back("?");
      
    }
  }  
  
  for(size_t i=0; i<aggregates.size(); i++)
  {
    vector<string> names = aggregates[i]->getColumnNames();
    updates.push_back(aggregates[i]->getUpdateWithNewEntrySql());
    for (size_t j = 0; j < names.size(); j++) {
      column_names.push_back("`"+names[j]+"`");
      column_values.push_back("?");
    }
  }

  string sql = "INSERT INTO `"+getTableName()+"`";
  sql += " ("+boost::algorithm::join(column_names, ", ")+")";
  sql += "VALUES ("+boost::algorithm::join(column_values, ", ")+") ";
  sql += "ON DUPLICATE KEY UPDATE "+boost::algorithm::join(updates, ", ");
  return sql;
}

boost::shared_ptr<sql::PreparedStatement> jetstream::cube::MysqlCube::getInsertEntryPreparedStatement()
{
  if(!insertEntryStatement)
  {
    shared_ptr<sql::PreparedStatement> stmnt(getConnection()->prepareStatement(getInsertEntryPreparedSql()));
    insertEntryStatement = stmnt;
  }
  return insertEntryStatement;
}


bool jetstream::cube::MysqlCube::insertEntry(jetstream::Tuple t)
{
  boost::shared_ptr<sql::PreparedStatement> pstmt = getInsertEntryPreparedStatement();
  int tuple_index = 0;
  int field_index = 1;
  for(size_t i=0; i<dimensions.size(); i++)
  {
    dimensions[i]->setValueForInsertEntry(pstmt, t, tuple_index, field_index);
  }
  cout << "field index: " << field_index << endl;
  for(size_t i=0; i<aggregates.size(); i++)
  {
    aggregates[i]->setValueForInsertEntry(pstmt, t, tuple_index, field_index);
  }
  cout << "field index: " << field_index << endl;
  //TODO error handling
  pstmt->execute();
  return true;
}
