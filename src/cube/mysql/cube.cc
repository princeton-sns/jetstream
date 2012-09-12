

#include "mysql_cube.h"
#include "cube_iterator.h"
#include "cube_iterator_impl.h"

using namespace ::std;

jetstream::cube::MysqlCube::MysqlCube(jetstream::CubeSchema _schema, string db_host, string db_user, string db_pass, string db_name, size_t batch) : 
      DataCubeImpl<MysqlDimension, MysqlAggregate>(_schema), 
      db_host(db_host),
      db_user(db_user),
      db_pass(db_pass),
      db_name(db_name),
      batch(batch)
      { init_connection(); }

void jetstream::cube::MysqlCube::init_connection()
{
  sql::Driver * driver = get_driver_instance();
  shared_ptr<sql::Connection> con(driver->connect(db_host, db_user, db_pass));
  connection = con;
  connection->setSchema(db_name);

  shared_ptr<sql::Statement> stmnt(connection->createStatement());
  statement = stmnt;

  for (size_t i = 0; i < dimensions.size(); i++) {
    dimensions[i]->set_connection(connection);
  }
}

void jetstream::cube::MysqlCube::execute_sql(string sql)
{
  statement->execute(sql);
}

boost::shared_ptr<sql::ResultSet> jetstream::cube::MysqlCube::execute_query_sql(string sql) const
{
  boost::shared_ptr<sql::ResultSet> res(statement->executeQuery(sql));
  return res;
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
  execute_sql("DROP TABLE IF EXISTS `"+get_table_name()+"`");
}


string jetstream::cube::MysqlCube::get_table_name() const
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
  sql += "VALUES ";
  string vals =  "("+boost::algorithm::join(column_values, ", ")+")";
  numFieldsPerInsertEntryBatch = column_values.size();
  for(size_t i=0; i < (batch-1); i++) {
    sql += vals+", ";
  }
  sql += vals+" ";
  sql += "ON DUPLICATE KEY UPDATE "+boost::algorithm::join(updates, ", ");
  return sql;
}

string jetstream::cube::MysqlCube::get_insert_partial_aggregate_prepared_sql()
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
    updates.push_back(aggregates[i]->get_update_with_partial_aggregate_sql());
    for (size_t j = 0; j < names.size(); j++) {
      column_names.push_back("`"+names[j]+"`");
      column_values.push_back("?");
    }
  }

  string sql = "INSERT INTO `"+get_table_name()+"`";
  sql += " ("+boost::algorithm::join(column_names, ", ")+")";
  sql += "VALUES ";
  string vals =  "("+boost::algorithm::join(column_values, ", ")+")";
  numFieldsPerPartialAggregateBatch = column_values.size();
  for(size_t i=0; i < (batch-1); i++) {
    sql += vals+", ";
  }
  sql += vals+" ";
  sql += "ON DUPLICATE KEY UPDATE "+boost::algorithm::join(updates, ", ");
  return sql;
}

boost::shared_ptr<sql::PreparedStatement> jetstream::cube::MysqlCube::get_insert_entry_prepared_statement()
{
  if(!insertEntryStatement)
  {
    shared_ptr<sql::PreparedStatement> stmnt(get_connection()->prepareStatement(get_insert_entry_prepared_sql()));
    insertEntryStatement = stmnt;
    insertEntryCurrentBatch = 0;
  }
  return insertEntryStatement;
}

boost::shared_ptr<sql::PreparedStatement> jetstream::cube::MysqlCube::get_insert_partial_aggregate_prepared_statement()
{
  if(!insertPartialAggregateStatement)
  {
    shared_ptr<sql::PreparedStatement> stmnt(get_connection()->prepareStatement(get_insert_partial_aggregate_prepared_sql()));
    insertPartialAggregateStatement = stmnt;
    insertPartialAggregateCurrentBatch = 0;
  }
  return insertPartialAggregateStatement;
}

bool jetstream::cube::MysqlCube::insert_entry(jetstream::Tuple t)
{
  boost::shared_ptr<sql::PreparedStatement> pstmt = get_insert_entry_prepared_statement();
  int tuple_index = 0;
  int field_index = (insertEntryCurrentBatch*numFieldsPerInsertEntryBatch)+1;
  for(size_t i=0; i<dimensions.size(); i++)
  {
    dimensions[i]->set_value_for_insert(pstmt, t, tuple_index, field_index);
  }
  
  for(size_t i=0; i<aggregates.size(); i++)
  {
    aggregates[i]->set_value_for_insert_entry(pstmt, t, tuple_index, field_index);
  }
  
  ++insertEntryCurrentBatch;
  //TODO error handling
  if (insertEntryCurrentBatch >= batch)
  {
    pstmt->execute();
    insertEntryCurrentBatch = 0;
  }
  return true;
}

bool jetstream::cube::MysqlCube::insert_partial_aggregate(jetstream::Tuple t)
{
  boost::shared_ptr<sql::PreparedStatement> pstmt = get_insert_partial_aggregate_prepared_statement();
  int tuple_index = 0;
  int field_index = (insertPartialAggregateCurrentBatch*numFieldsPerPartialAggregateBatch)+1;
  for(size_t i=0; i<dimensions.size(); i++)
  {
    dimensions[i]->set_value_for_insert(pstmt, t, tuple_index, field_index);
  }
  
  for(size_t i=0; i<aggregates.size(); i++)
  {
    aggregates[i]->set_value_for_insert_partial_aggregate(pstmt, t, tuple_index, field_index);
  }
  
  ++insertPartialAggregateCurrentBatch;
  //TODO error handling
  if (insertPartialAggregateCurrentBatch >= batch)
  {
    pstmt->execute();
    insertPartialAggregateCurrentBatch = 0;
  }
  return true;
}

boost::shared_ptr<jetstream::Tuple> jetstream::cube::MysqlCube::get_cell_value_final(jetstream::Tuple t)
{
  return get_cell_value(t, true);
}


boost::shared_ptr<jetstream::Tuple> jetstream::cube::MysqlCube::get_cell_value_partial(jetstream::Tuple t)
{
  return get_cell_value(t, false);
}

boost::shared_ptr<jetstream::Tuple> jetstream::cube::MysqlCube::get_cell_value(jetstream::Tuple t, bool final)
{
  int tuple_index = 0;
  vector<string> where_clauses;
  for(size_t i=0; i<dimensions.size(); i++)
  {
    string where = dimensions[i]->get_where_clause_exact(t, tuple_index, false);
    where_clauses.push_back(where);
  }
  string sql = "SELECT * FROM `"+get_table_name()+"` WHERE "+boost::algorithm::join(where_clauses, " AND ");

  boost::shared_ptr<sql::ResultSet> res = execute_query_sql(sql);
  
  if(res->rowsCount() > 1)
  {
    LOG(FATAL) << "Something went wrong, fetching more than 1 row per cell";
  }
  if(!res->first())
  {
    boost::shared_ptr<jetstream::Tuple> res;
    return res;
  }

  return make_tuple_from_result_set(res, final);
}

boost::shared_ptr<jetstream::Tuple> jetstream::cube::MysqlCube::make_tuple_from_result_set(boost::shared_ptr<sql::ResultSet> res, bool final) {
  boost::shared_ptr<jetstream::Tuple> result = make_shared<jetstream::Tuple>();

  int column_index = 1;
  for(size_t i=0; i<dimensions.size(); i++)
  {
    dimensions[i]->populate_tuple(result, res, column_index);
  }
  for(size_t i=0; i<aggregates.size(); i++)
  {
    if(!final)
      aggregates[i]->populate_tuple_partial(result, res, column_index);
    else
      aggregates[i]->populate_tuple_final(result, res, column_index);
  }


  return result;
}

jetstream::cube::CubeIterator jetstream::cube::MysqlCube::slice_query(jetstream::Tuple min, jetstream::Tuple max, bool final)
{
  int tuple_index_min = 0;
  int tuple_index_max = 0;
  vector<string> where_clauses;
  
  for(size_t i=0; i<dimensions.size(); i++) {
    string where = dimensions[i]->get_where_clause_greater_than_eq(min, tuple_index_min, true);
    if(where.size() > 1)
      where_clauses.push_back(where);
    
    where = dimensions[i]->get_where_clause_less_than_eq(max, tuple_index_max, true);
    if(where.size() > 1)
      where_clauses.push_back(where);
  }
  string sql;
  if(where_clauses.size() > 0) {
    sql = "SELECT * FROM `"+get_table_name()+"` WHERE "+boost::algorithm::join(where_clauses, " AND ");
  }
  else {
    sql = "SELECT * FROM `"+get_table_name()+"`";
  }

  boost::shared_ptr<sql::ResultSet> res = execute_query_sql(sql);
  boost::shared_ptr<jetstream::cube::MysqlCubeIteratorImpl> impl;
  if(!res->next())  {
    impl = boost::make_shared<jetstream::cube::MysqlCubeIteratorImpl>(); 
  }
  else {
    impl = boost::make_shared<jetstream::cube::MysqlCubeIteratorImpl>(shared_from_this(), res, final); 
  }
  return CubeIterator(impl);
}
 
jetstream::cube::CubeIterator jetstream::cube::MysqlCube::end()
{
  boost::shared_ptr<jetstream::cube::MysqlCubeIteratorImpl> impl = MysqlCubeIteratorImpl::end();
  return CubeIterator(impl);
}

void jetstream::cube::MysqlCube::set_batch(size_t numBatch) {
  batch = numBatch;
}

size_t jetstream::cube::MysqlCube::num_leaf_cells() const
{
  string sql = "SELECT COUNT(*) FROM `"+get_table_name()+"`";

  boost::shared_ptr<sql::ResultSet> res = execute_query_sql(sql);
  
  if(res->rowsCount() != 1)
  {
    LOG(FATAL) << "Something went wrong, fetching more than 1 row per cell";
  }

  res->first();

  int sz = res->getInt(1);

  if(sz < 0)
  {
    LOG(FATAL) << "Something went wrong, got a negative count";
  }

  return (size_t) sz;
}
