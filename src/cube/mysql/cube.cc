

#include "cube.h"

using namespace ::std;


jetstream::cube::MysqlCube::MysqlCube(jetstream::CubeSchema _schema) : 
      DataCubeImpl<MysqlDimension, MysqlAggregate>(_schema), 
      db_host("localhost"),
      db_user("root"),
      db_pass(""),
      db_name("test_cube")
      { init_connection(); }

      jetstream::cube::MysqlCube::MysqlCube(jetstream::CubeSchema _schema, string db_host, string db_user, string db_pass, string db_name) : 
      DataCubeImpl<MysqlDimension, MysqlAggregate>(_schema), 
      db_host(db_host),
      db_user(db_user),
      db_pass(db_pass),
      db_name(db_name)
      { init_connection(); }

void jetstream::cube::MysqlCube::init_connection()
{
  sql::Driver * driver = get_driver_instance();
  shared_ptr<sql::Connection> con(driver->connect(db_host, db_user, db_pass));
  connection = con;
  connection->setSchema(db_name);

  shared_ptr<sql::Statement> stmnt(connection->createStatement());
  statement = stmnt;
}

void jetstream::cube::MysqlCube::execute_sql(string sql)
{
  statement->execute(sql);
}


boost::shared_ptr<sql::Connection> jetstream::cube::MysqlCube::get_connection()
{
  return connection;
}

string jetstream::cube::MysqlCube::create_sql() {
  string sql = "CREATE TABLE `"+get_table_name()+"` (";
  vector<string> pk;
  for(size_t i=0; i<dimensions.size(); i++)
  {
    vector<string> names = dimensions[i]->get_column_names();
    vector<string> types = dimensions[i]->get_column_types();
    for (size_t j = 0; j < names.size(); j++) {
      sql += "`"+names[j]+"` " + types[j] + " NOT NULL,";
      pk.push_back("`"+names[j]+"`");
    }
  }

  for(size_t i=0; i<aggregates.size(); i++)
  {
    vector<string> names = aggregates[i]->get_column_names();
    vector<string> types = aggregates[i]->get_column_types();
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
  execute_sql(create_sql());
}

void jetstream::cube::MysqlCube::destroy()
{
  execute_sql("DROP TABLE `"+get_table_name()+"`");
}


string jetstream::cube::MysqlCube::get_table_name()
{
  return name;
}

vector<string> jetstream::cube::MysqlCube::get_dimension_column_types()
{
  vector<string> cols;
  size_t i;
  for (i = 0; i < dimensions.size() ; i++) {
    shared_ptr<MysqlDimension> dim =dimensions[i];
    vector<string> tmp = dim->get_column_types();
    for (size_t j = 0; j < tmp.size(); j++) {
      cols.push_back(tmp[j]);
    }
  }
  return cols;
}

vector<string> jetstream::cube::MysqlCube::get_aggregate_column_types()
{
  vector<string> cols;
  size_t i;
  for (i = 0; i < aggregates.size() ; i++) {
    shared_ptr<MysqlAggregate> agg =aggregates[i];
    vector<string> tmp = agg->get_column_types();
    for (size_t j = 0; j < tmp.size(); j++) {
      cols.push_back(tmp[j]);
    }
  }
  return cols;
}


string jetstream::cube::MysqlCube::get_insert_entry_prepared_sql()
{
  vector<string> column_names;
  vector<string> column_values;
  vector<string> updates;
  for(size_t i=0; i<dimensions.size(); i++)
  {
    vector<string> names = dimensions[i]->get_column_names();
    for (size_t j = 0; j < names.size(); j++) {
      column_names.push_back("`"+names[j]+"`");
      column_values.push_back("?");
      
    }
  }  
  
  for(size_t i=0; i<aggregates.size(); i++)
  {
    vector<string> names = aggregates[i]->get_column_names();
    updates.push_back(aggregates[i]->get_update_with_new_entry_sql());
    for (size_t j = 0; j < names.size(); j++) {
      column_names.push_back("`"+names[j]+"`");
      column_values.push_back("?");
    }
  }

  string sql = "INSERT INTO `"+get_table_name()+"`";
  sql += " ("+boost::algorithm::join(column_names, ", ")+")";
  sql += "VALUES ("+boost::algorithm::join(column_values, ", ")+") ";
  sql += "ON DUPLICATE KEY UPDATE "+boost::algorithm::join(updates, ", ");
  return sql;
}

boost::shared_ptr<sql::PreparedStatement> jetstream::cube::MysqlCube::get_insert_entry_prepared_statement()
{
  if(!insertEntryStatement)
  {
    shared_ptr<sql::PreparedStatement> stmnt(get_connection()->prepareStatement(get_insert_entry_prepared_sql()));
    insertEntryStatement = stmnt;
  }
  return insertEntryStatement;
}


bool jetstream::cube::MysqlCube::insert_entry(jetstream::Tuple t)
{
  boost::shared_ptr<sql::PreparedStatement> pstmt = get_insert_entry_prepared_statement();
  int tuple_index = 0;
  int field_index = 1;
  for(size_t i=0; i<dimensions.size(); i++)
  {
    dimensions[i]->set_value_for_insert_entry(pstmt, t, tuple_index, field_index);
  }
  
  for(size_t i=0; i<aggregates.size(); i++)
  {
    aggregates[i]->set_value_for_insert_entry(pstmt, t, tuple_index, field_index);
  }
  
  //TODO error handling
  pstmt->execute();
  return true;
}
