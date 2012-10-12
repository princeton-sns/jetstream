

#include "mysql_cube.h"
#include "cube_iterator.h"
#include "cube_iterator_impl.h"

#include <glog/logging.h>

using namespace ::std;
using namespace jetstream::cube;

jetstream::cube::MysqlCube::MysqlCube (jetstream::CubeSchema const _schema,
                                       string _name,
                                       bool delete_if_exists,
                                       string db_host,
                                       string db_user,
                                       string db_pass,
                                       string db_name,
                                       size_t batch) :
  DataCubeImpl<MysqlDimension, MysqlAggregate>(_schema, _name, batch),
  db_host(db_host),
  db_user(db_user),
  db_pass(db_pass),
  db_name(db_name),
  batch(batch),
  insertCurrentBatch(0),
  assumeOnlyWriter(true)
  {

  init_connection();
  LOG(INFO) << "creating cube "<<db_name<< "."<< name <<
            (delete_if_exists ? " and deleting prior contents": ".");

  if (delete_if_exists) {
    destroy();
  }
}

void
jetstream::cube::MysqlCube::init_connection() {
  sql::Driver * driver = sql::mysql::get_driver_instance();

  sql::ConnectOptionsMap options; 
  options.insert( std::make_pair( "hostName", db_host)); 
  options.insert( std::make_pair( "userName", db_user)); 
  options.insert( std::make_pair( "password", db_pass)); 
  options.insert( std::make_pair( "CLIENT_MULTI_STATEMENTS", true ) ); 


  //shared_ptr<sql::Connection> con(driver->connect(db_host, db_user, db_pass));
  shared_ptr<sql::Connection> con(driver->connect(options));
  connection = con;
  connection->setSchema(db_name);

  shared_ptr<sql::Statement> stmnt(connection->createStatement());
  statement = stmnt;

  for (size_t i = 0; i < dimensions.size(); i++) {
    dimensions[i]->set_connection(connection);
  }
}

void
jetstream::cube::MysqlCube::execute_sql (const string &sql) const {
  try {
    statement->execute(sql);
  }
  catch (sql::SQLException &e) {
    LOG(WARNING) << "couldn't execute sql statement; " << e.what() <<
                 "\nStatement was " << sql;
  }
}

boost::shared_ptr<sql::ResultSet>
jetstream::cube::MysqlCube::execute_query_sql(const string &sql) const {
  try {
    boost::shared_ptr<sql::ResultSet> res(statement->executeQuery(sql));
    return res;
  }
  catch (sql::SQLException &e) {
    LOG(WARNING) << "couldn't execute sql statement; " << e.what() <<
                 "\nStatement was " << sql;
  }

  boost::shared_ptr<sql::ResultSet> no_results;
  return no_results;
}


boost::shared_ptr<sql::Connection>
jetstream::cube::MysqlCube::get_connection() const {
  return connection;
}

string jetstream::cube::MysqlCube::create_sql(bool aggregate_table) const {
  string sql;

  if(!aggregate_table)
    sql = "CREATE TABLE IF NOT EXISTS `"+get_table_name()+"` (";
  else
    sql = "CREATE TABLE IF NOT EXISTS `"+get_rollup_table_name()+"` (";

  vector<string> pk;

  for(size_t i=0; i<dimensions.size(); i++) {
    vector<string> names = dimensions[i]->get_column_names();
    vector<string> types = dimensions[i]->get_column_types();

    for (size_t j = 0; j < names.size(); j++) {
      sql += "`"+names[j]+"` " + types[j] + " NOT NULL,";
      pk.push_back("`"+names[j]+"`");
    }

    if(aggregate_table) {
      sql += "`"+dimensions[i]->get_rollup_level_column_name()+"` INT NOT NULL ,";
      pk.push_back("`"+dimensions[i]->get_rollup_level_column_name()+"`");

    }
  }

  for(size_t i=0; i<aggregates.size(); i++) {
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

  VLOG(1) << "Create statement: " << sql;
  return sql;

}

void jetstream::cube::MysqlCube::create() {
  execute_sql(create_sql(false));
  execute_sql(create_sql(true));
}

void jetstream::cube::MysqlCube::destroy() {
  execute_sql("DROP TABLE IF EXISTS `"+get_table_name()+"`");
  execute_sql("DROP TABLE IF EXISTS `"+get_rollup_table_name()+"`");
}


string jetstream::cube::MysqlCube::get_table_name() const {
  return name;
}

string jetstream::cube::MysqlCube::get_rollup_table_name() const {
  return name+"_rollup";
}

vector<string> jetstream::cube::MysqlCube::get_dimension_column_types() const {
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

vector<string> jetstream::cube::MysqlCube::get_aggregate_column_types() const {
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

string jetstream::cube::MysqlCube::get_select_cell_prepared_sql(size_t num_cells) {
  vector<string> where_clauses;

  for(size_t i=0; i<dimensions.size(); i++) {
    string where = dimensions[i]->get_where_clause_exact_prepared();
    where_clauses.push_back(where);
  }

  string single_cell_where ="("+ boost::algorithm::join(where_clauses, " AND ")+")";

  string sql = "SELECT * FROM `"+get_table_name()+"` WHERE ";

  assert(num_cells > 0);
  for(size_t i=0; i < (num_cells-1); i++) {
    sql += single_cell_where+" OR ";
  }

  sql += single_cell_where;
  return sql;
}

string jetstream::cube::MysqlCube::get_insert_prepared_sql(size_t batch) {
  vector<string> column_names;
  vector<string> column_values;
  vector<string> updates;

  for(size_t i=0; i<dimensions.size(); i++) {
    vector<string> names = dimensions[i]->get_column_names();

    for (size_t j = 0; j < names.size(); j++) {
      column_names.push_back("`"+names[j]+"`");
      column_values.push_back("?");

    }
  }

  for(size_t i=0; i<aggregates.size(); i++) {
    vector<string> names = aggregates[i]->get_column_names();
    updates.push_back(aggregates[i]->get_update_on_insert_sql());

    for (size_t j = 0; j < names.size(); j++) {
      column_names.push_back("`"+names[j]+"`");
      column_values.push_back("?");
    }
  }



  string sql = "INSERT INTO `"+get_table_name()+"`";
  sql += " ("+boost::algorithm::join(column_names, ", ")+")";
  sql += " VALUES ";
  string vals =  "("+boost::algorithm::join(column_values, ", ")+")";
  numFieldsPerBatch = column_values.size();

  assert(batch > 0);
  for(size_t i=0; i < (batch-1); i++) {
    sql += vals+", ";
  }

  sql += vals+" ";
  sql += "ON DUPLICATE KEY UPDATE "+boost::algorithm::join(updates, ", ");
  return sql;
}

boost::shared_ptr<sql::PreparedStatement> MysqlCube::create_prepared_statement(std::string sql) {
  try {
    shared_ptr<sql::PreparedStatement> stmnt(get_connection()->prepareStatement(sql));
    return stmnt;
  }
  catch (sql::SQLException &e) {
    LOG(WARNING) << "couldn't execute sql statement; " << e.what();
    LOG(WARNING) << "statement was " << sql;
    boost::shared_ptr<sql::PreparedStatement> p;
    return p;
  }
}

boost::shared_ptr<sql::PreparedStatement> MysqlCube::get_lock_prepared_statement(string table_name) {
  string key = "Lock|"+ table_name;

  if(preparedStatementCache.count(key) == 0) {
    string sql= "LOCK TABLES `"+table_name+"` WRITE" ;
    preparedStatementCache[key] = create_prepared_statement(sql); 
  }

  return preparedStatementCache[key];
}

boost::shared_ptr<sql::PreparedStatement> MysqlCube::get_unlock_prepared_statement() {
  string key = "unlock";

  if(preparedStatementCache.count(key) == 0) {
    string sql= "UNLOCK TABLES";
    preparedStatementCache[key] = create_prepared_statement(sql); 
  }

  return preparedStatementCache[key];
}

boost::shared_ptr<sql::PreparedStatement> MysqlCube::get_select_cell_prepared_statement(size_t batch, std::string unique_key) {
  string key = "selectCell|"+boost::lexical_cast<string>(batch)+"|"+unique_key;

  if(preparedStatementCache.count(key) == 0) {
    string sql= get_select_cell_prepared_sql(batch);
    preparedStatementCache[key] = create_prepared_statement(sql); 
  }

  return preparedStatementCache[key];
}
boost::shared_ptr<sql::PreparedStatement> MysqlCube::get_insert_prepared_statement(size_t batch) {
  string key = "insert_prepared_statement|"+boost::lexical_cast<string>(batch);

  if(preparedStatementCache.count(key) == 0) {
    string sql= get_insert_prepared_sql(batch);
    preparedStatementCache[key] = create_prepared_statement(sql); 
  }

  return preparedStatementCache[key];
}

bool jetstream::cube::MysqlCube::insert_entry(jetstream::Tuple const &t) {
  boost::shared_ptr<sql::PreparedStatement> pstmt = get_insert_prepared_statement(batch);
  int tuple_index = 0;
  int field_index = (insertCurrentBatch*numFieldsPerBatch)+1;

  for(size_t i=0; i<dimensions.size(); i++) {
    dimensions[i]->set_value_for_insert(pstmt, t, tuple_index, field_index);
  }

  for(size_t i=0; i<aggregates.size(); i++) {
    aggregates[i]->set_value_for_insert_entry(pstmt, t, tuple_index, field_index);
  }

  ++insertCurrentBatch;

  //TODO error handling
  if (insertCurrentBatch >= batch) {
    pstmt->execute();
    insertCurrentBatch = 0;
  }

  return true;
}

bool jetstream::cube::MysqlCube::insert_partial_aggregate(jetstream::Tuple const &t) {
  boost::shared_ptr<sql::PreparedStatement> pstmt = get_insert_prepared_statement(batch);
  int tuple_index = 0;
  int field_index = (insertCurrentBatch*numFieldsPerBatch)+1;

  for(size_t i=0; i<dimensions.size(); i++) {
    dimensions[i]->set_value_for_insert(pstmt, t, tuple_index, field_index);
  }

  for(size_t i=0; i<aggregates.size(); i++) {
    aggregates[i]->set_value_for_insert_partial_aggregate(pstmt, t, tuple_index, field_index);
  }

  ++insertCurrentBatch;

  //TODO error handling
  if (insertCurrentBatch >= batch) {
    pstmt->execute();
    insertCurrentBatch = 0;
  }

  return true;
}

void MysqlCube::save_tuple(jetstream::Tuple const &t, bool need_new_value, bool need_old_value, boost::shared_ptr<jetstream::Tuple> &new_tuple,boost::shared_ptr<jetstream::Tuple> &old_tuple) {
  boost::shared_ptr<sql::PreparedStatement> lock_stmt;
  boost::shared_ptr<sql::PreparedStatement> old_value_stmt;
  boost::shared_ptr<sql::PreparedStatement> insert_stmt; 
  boost::shared_ptr<sql::PreparedStatement> new_value_stmt;
  boost::shared_ptr<sql::PreparedStatement> unlock_stmt;

  /**** Setup statements *****/
  int field_index;
  
  if(!assumeOnlyWriter) {
    lock_stmt = get_lock_prepared_statement(get_table_name());
    unlock_stmt = get_unlock_prepared_statement();
  }

  if(need_old_value)
  {
    old_value_stmt = get_select_cell_prepared_statement(1, "old_value");
    field_index = 1;
    for(size_t i=0; i<dimensions.size(); i++) {
      dimensions[i]->set_value_for_insert_tuple(old_value_stmt, t, field_index);
    }
  }
  
  insert_stmt = get_insert_prepared_statement(1);
  field_index = 1;
  for(size_t i=0; i<dimensions.size(); i++) {
    dimensions[i]->set_value_for_insert_tuple(insert_stmt, t, field_index);
  }

  for(size_t i=0; i<aggregates.size(); i++) {
    aggregates[i]->set_value_for_insert_tuple(insert_stmt, t, field_index);
  }

  if(need_new_value)
  {
    field_index = 1;
    new_value_stmt = get_select_cell_prepared_statement(1, "new_value");
    for(size_t i=0; i<dimensions.size(); i++) {
      dimensions[i]->set_value_for_insert_tuple(new_value_stmt, t, field_index);
    }
  }


  /******** Execute statements *******/
  if(!assumeOnlyWriter) {
    lock_stmt->execute();
  }

  boost::shared_ptr<sql::ResultSet> old_value_results;
  if(need_old_value)
  {
    old_value_results.reset(old_value_stmt->executeQuery());
  }

  insert_stmt->execute();

  boost::shared_ptr<sql::ResultSet> new_value_results;
  if(need_new_value)
  {
    new_value_results.reset(new_value_stmt->executeQuery());
  }
  
  if(!assumeOnlyWriter) {
    unlock_stmt->execute();
  }

  /********* Populate tuples ******/
  if(need_old_value && old_value_results->first())
  {
    old_tuple = make_tuple_from_result_set(old_value_results, false);
  }
  else
  {
    old_tuple.reset();
  }
  
  if(need_new_value && new_value_results->first())
    new_tuple = make_tuple_from_result_set(new_value_results, false);
  else
    new_tuple.reset();
}


void MysqlCube::save_tuple_batch(std::vector<boost::shared_ptr<jetstream::Tuple> > tuple_store, 
       std::vector<bool> need_new_value_store, std::vector<bool> need_old_value_store, 
       std::list<boost::shared_ptr<jetstream::Tuple> > &new_tuple_list, std::list<boost::shared_ptr<jetstream::Tuple> > &old_tuple_list) {

  boost::shared_ptr<sql::PreparedStatement> lock_stmt;
  boost::shared_ptr<sql::PreparedStatement> old_value_stmt;
  boost::shared_ptr<sql::PreparedStatement> insert_stmt; 
  boost::shared_ptr<sql::PreparedStatement> new_value_stmt;
  boost::shared_ptr<sql::PreparedStatement> unlock_stmt;

  /**** Setup statements *****/
  int field_index;
  
  if(!assumeOnlyWriter) {
    lock_stmt = get_lock_prepared_statement(get_table_name());
    unlock_stmt = get_unlock_prepared_statement();
  }

  size_t count_old = std::count(need_old_value_store.begin(), need_old_value_store.end(), true);
  if(count_old > 0) {
    old_value_stmt = get_select_cell_prepared_statement(count_old, "old_value");
    field_index = 1;
    for(size_t ti = 0; ti<need_old_value_store.size(); ++ti) {
      if(need_old_value_store[ti]) {
        for(size_t i=0; i<dimensions.size(); i++) {
          dimensions[i]->set_value_for_insert_tuple(old_value_stmt, *(tuple_store[ti]), field_index);
        }
      }
    }
  }
    
  insert_stmt = get_insert_prepared_statement(tuple_store.size());

  field_index = 1;

  for(size_t ti = 0; ti< tuple_store.size(); ++ti) {
    for(size_t i=0; i<dimensions.size(); i++) {
      dimensions[i]->set_value_for_insert_tuple(insert_stmt, *(tuple_store[ti]), field_index);
    }

    for(size_t i=0; i<aggregates.size(); i++) {
      aggregates[i]->set_value_for_insert_tuple(insert_stmt, *(tuple_store[ti]), field_index);
    }
  }
  
  size_t count_new = std::count(need_new_value_store.begin(), need_new_value_store.end(), true);
  if(count_new > 0) {
    new_value_stmt = get_select_cell_prepared_statement(count_new, "new_value");
    field_index = 1;
    for(size_t ti = 0; ti<need_new_value_store.size(); ++ti) {
      if(need_new_value_store[ti]) {
        for(size_t i=0; i<dimensions.size(); i++) {
          dimensions[i]->set_value_for_insert_tuple(new_value_stmt, *(tuple_store[ti]), field_index);
        }
      }
    }
  }

  /******** Execute statements *******/
  if(!assumeOnlyWriter) {
    lock_stmt->execute();
  }

  boost::shared_ptr<sql::ResultSet> old_value_results;
  if(count_old > 0)
  {
    old_value_results.reset(old_value_stmt->executeQuery());
  }

  insert_stmt->execute();

  boost::shared_ptr<sql::ResultSet> new_value_results;
  if(count_new > 0)
  {
    new_value_results.reset(new_value_stmt->executeQuery());
  }
  
  if(!assumeOnlyWriter) {
    unlock_stmt->execute();
  }

  /********* Populate tuples ******/
  old_tuple_list.clear();
  if(count_old > 0) {
    while(old_value_results->next()) {
      boost::shared_ptr<jetstream::Tuple> old_tuple = make_tuple_from_result_set(old_value_results, false);
      old_tuple_list.push_back(old_tuple);
    }
  }

  new_tuple_list.clear();
  if(count_new > 0) {
    while(new_value_results->next()) {
      boost::shared_ptr<jetstream::Tuple> new_tuple = make_tuple_from_result_set(new_value_results, false);
      new_tuple_list.push_back(new_tuple);
    }
  }

}

boost::shared_ptr<jetstream::Tuple> jetstream::cube::MysqlCube::get_cell_value_final(jetstream::Tuple const &t) const {
  return get_cell_value(t, true);
}


boost::shared_ptr<jetstream::Tuple> jetstream::cube::MysqlCube::get_cell_value_partial(jetstream::Tuple const &t) const {
  return get_cell_value(t, false);
}

boost::shared_ptr<jetstream::Tuple> jetstream::cube::MysqlCube::get_cell_value(jetstream::Tuple const &t, bool final) const {
  int tuple_index = 0;
  vector<string> where_clauses;

  for(size_t i=0; i<dimensions.size(); i++) {
    string where = dimensions[i]->get_where_clause_exact(t, tuple_index, false);
    where_clauses.push_back(where);
  }

  string sql = "SELECT * FROM `"+get_table_name()+"` WHERE "+boost::algorithm::join(where_clauses, " AND ");

  boost::shared_ptr<sql::ResultSet> res = execute_query_sql(sql);

  if(res->rowsCount() > 1) {
    LOG(FATAL) << "Something went wrong, fetching more than 1 row per cell";
  }

  if(!res->first()) {
    boost::shared_ptr<jetstream::Tuple> res;
    return res;
  }

  return make_tuple_from_result_set(res, final);
}

boost::shared_ptr<jetstream::Tuple>
jetstream::cube::MysqlCube::make_tuple_from_result_set(boost::shared_ptr<sql::ResultSet> res, bool final, bool rollup) const {
  boost::shared_ptr<jetstream::Tuple> result = make_shared<jetstream::Tuple>();

  int column_index = 1;

  for(size_t i=0; i<dimensions.size(); i++) {
    dimensions[i]->populate_tuple(result, res, column_index);

    if(rollup) {
      jetstream::Element *elem = result->add_e();
      elem->set_i_val(res->getInt(column_index));
      ++column_index;
    }
  }

  for(size_t i=0; i<aggregates.size(); i++) {
    if(!final)
      aggregates[i]->populate_tuple_partial(result, res, column_index);
    else
      aggregates[i]->populate_tuple_final(result, res, column_index);
  }


  return result;
}

jetstream::cube::CubeIterator jetstream::cube::MysqlCube::slice_query(jetstream::Tuple const &min, jetstream::Tuple const &max, bool final, list<string> const &sort, size_t limit) const {
  string sql = "SELECT * FROM `"+get_table_name()+"`";

  sql += get_where_clause(min, max);
  sql += get_sort_clause(sort);
  sql += get_limit_clause(limit);

  return get_result_iterator(sql, final);
}

jetstream::cube::CubeIterator jetstream::cube::MysqlCube::rollup_slice_query(std::list<unsigned int> const &levels, jetstream::Tuple const &min, jetstream::Tuple const &max, bool final, list<string> const &sort, size_t limit) const {
  string sql = "SELECT * FROM `"+get_rollup_table_name()+"`";
  sql += get_where_clause(min, max, levels);
  sql += get_sort_clause(sort);
  sql += get_limit_clause(limit);
  return get_result_iterator(sql, final, true);
}


string MysqlCube::get_sort_clause(list<string> const &sort) const {
  string sort_sql = "";

  for(list<string>::const_iterator i = sort.begin(); i != sort.end(); i++) {
    string item = *i;

    if(i != sort.begin())
      sort_sql += ", ";
    else
      sort_sql = " ORDER BY ";

    if(item[0] == '-') {
      boost::shared_ptr<MysqlDimension> dim = get_dimension(item.erase(0,1));
      sort_sql += dim->get_base_column_name()+" DESC";
    }
    else {
      boost::shared_ptr<MysqlDimension> dim = get_dimension(item);
      sort_sql += dim->get_base_column_name()+" ASC";
    }
  }

  return sort_sql;
}

string MysqlCube::get_limit_clause(size_t limit) const {
  string limit_sql = "";

  if (limit > 0) {
    limit_sql = " LIMIT "+ boost::lexical_cast<string>(limit);
  }

  return limit_sql;
}

string MysqlCube::get_where_clause(jetstream::Tuple const &min, jetstream::Tuple const &max, std::list<unsigned int> const &levels) const {

  assert(levels.empty() || levels.size() == dimensions.size());
  int tuple_index_min = 0;
  int tuple_index_max = 0;
  vector<string> where_clauses;

  list<unsigned int>::const_iterator iLevel = levels.begin();

  for(size_t i=0; i<dimensions.size(); i++) {
    string where = dimensions[i]->get_where_clause_greater_than_eq(min, tuple_index_min, true);

    if(where.size() > 1)
      where_clauses.push_back(where);

    where = dimensions[i]->get_where_clause_less_than_eq(max, tuple_index_max, true);

    if(where.size() > 1)
      where_clauses.push_back(where);

    if(!levels.empty()) {
      where_clauses.push_back(dimensions[i]->get_rollup_level_column_name()+" = "+boost::lexical_cast<string>(*iLevel));
      iLevel++;
    }
  }

  string sql = "";

  if(where_clauses.size() > 0) {
    sql =" WHERE "+boost::algorithm::join(where_clauses, " AND ");
  }

  return sql;
}

jetstream::cube::CubeIterator MysqlCube::get_result_iterator(string sql, bool final, bool rollup) const {
  boost::shared_ptr<sql::ResultSet> res = execute_query_sql(sql);
  boost::shared_ptr<jetstream::cube::MysqlCubeIteratorImpl> impl;

  if(!res->next())  {
    impl = boost::make_shared<jetstream::cube::MysqlCubeIteratorImpl>();
  }
  else {
    impl = boost::make_shared<jetstream::cube::MysqlCubeIteratorImpl>(shared_from_this(), res, final, rollup);
  }

  return CubeIterator(impl);
}

jetstream::cube::CubeIterator jetstream::cube::MysqlCube::end() const {
  boost::shared_ptr<jetstream::cube::MysqlCubeIteratorImpl> impl = MysqlCubeIteratorImpl::end();
  return CubeIterator(impl);
}

void jetstream::cube::MysqlCube::set_batch(size_t numBatch) {
  batch = numBatch;
}

size_t jetstream::cube::MysqlCube::num_leaf_cells() const {
  string sql = "SELECT COUNT(*) FROM `"+get_table_name()+"`";

  boost::shared_ptr<sql::ResultSet> res = execute_query_sql(sql);

  if(res->rowsCount() != 1) {
    LOG(FATAL) << "Something went wrong, fetching more than 1 row per cell";
  }

  res->first();

  int sz = res->getInt(1);

  if(sz < 0) {
    LOG(FATAL) << "Something went wrong, got a negative count";
  }

  return (size_t) sz;
}

void
MysqlCube::do_rollup(std::list<unsigned int> const &levels, jetstream::Tuple const &min, jetstream::Tuple const& max) {
  assert(levels.size() == dimensions.size());

  vector<string> column_names;
  vector<string> select_clause;
  vector<string> groupby_clause;

  list<unsigned int>::const_iterator iLevel = levels.begin();

  for(size_t i=0; i<dimensions.size(); i++) {
    vector<string> names = dimensions[i]->get_column_names();

    for (size_t j = 0; j < names.size(); j++) {
      column_names.push_back("`"+names[j]+"`");
    }

    column_names.push_back(dimensions[i]->get_rollup_level_column_name());
    select_clause.push_back(dimensions[i]->get_select_clause_for_rollup(*iLevel));
    string groupby = dimensions[i]->get_groupby_clause_for_rollup(*iLevel);

    if(!groupby.empty()) {
      groupby_clause.push_back(groupby);
    }

    iLevel++;
  }

  for(size_t i=0; i<aggregates.size(); i++) {
    vector<string> names = aggregates[i]->get_column_names();

    for (size_t j = 0; j < names.size(); j++) {
      column_names.push_back("`"+names[j]+"`");
    }

    select_clause.push_back(aggregates[i]->get_select_clause_for_rollup());
  }

  string sql = "INSERT INTO `"+get_rollup_table_name()+"`";
  sql += " ("+boost::algorithm::join(column_names, ", ")+") ";
  sql += "SELECT "+boost::algorithm::join(select_clause, ", ");
  sql += "FROM "+get_table_name();
  sql += get_where_clause(min, max);

  if(!groupby_clause.empty()) {
    sql += " GROUP BY "+boost::algorithm::join(groupby_clause, ", ");
  }

  execute_sql(sql);
}
