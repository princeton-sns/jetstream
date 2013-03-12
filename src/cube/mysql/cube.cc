#include "mysql_cube.h"
#include "cube_iterator.h"
#include "cube_iterator_impl.h"

#include <glog/logging.h>

using namespace ::std;
using namespace jetstream::cube;

#define MYSQL_PROFILE 0
#define MYSQL_UNION_SELECT 0
#define MYSQL_TRANSACTIONS 0
#define MYSQL_INNODB 0
#define MYSQL_MAX_BATCH_PW_2 8

jetstream::cube::MysqlCube::MysqlCube (jetstream::CubeSchema const _schema,
                                       string _name,
                                       bool delete_if_exists, const NodeConfig &conf ) :
  DataCubeImpl<MysqlDimension, MysqlAggregate>(_schema, _name, conf) {

  init_connection();
  LOG(INFO) << "creating cube " << db_name << "." << name <<
            (delete_if_exists ? " and deleting prior contents": ".");

  if (delete_if_exists) {
    destroy();
  }
}

MysqlCube::~MysqlCube() {
  LOG(INFO) << "destroying cube "<<db_name<< "."<< name;

  if(is_frozen) {
    destroy();
  }
}

void
jetstream::cube::MysqlCube::init_connection() {
}

boost::shared_ptr<MysqlCube::ThreadConnection>
MysqlCube::get_uncached_connection(sql::Driver * driver) {
  shared_ptr<ThreadConnection> tc(new ThreadConnection());

  sql::ConnectOptionsMap options;
  options.insert( std::make_pair( "hostName", db_host));
  options.insert( std::make_pair( "userName", db_user));
  options.insert( std::make_pair( "password", db_pass));
  options.insert( std::make_pair( "CLIENT_MULTI_STATEMENTS", true ) );

  try {
    shared_ptr<sql::Connection> con(driver->connect(options));

    tc->connection = con;
    tc->connection->setSchema(db_name);
    shared_ptr<sql::Statement> stmnt(tc->connection->createStatement());
    tc->statement = stmnt;
  }
  catch (sql::SQLException &e) {
    LOG(FATAL) << e.what()<< "...perhaps the DB isn't running?";
  }

  return tc;
}

boost::shared_ptr<MysqlCube::ThreadConnection> MysqlCube::get_thread_connection() {
  boost::thread::id tid = boost::this_thread::get_id();
  shared_lock<boost::upgrade_mutex> lock(connectionLock);

  if(connectionPool.count(tid) == 0) {
    upgrade_lock<boost::upgrade_mutex> writeLock(connectionLock);
    VLOG(1) << "creating new SQL connection for thread " << tid;

    sql::Driver * driver = sql::mysql::get_driver_instance();

    boost::shared_ptr<MysqlCube::ThreadConnection> tc = get_uncached_connection(driver);
    connectionPool[tid] = tc;

    //TODO: make thread ends clear the connection pool entry. Tricky because this can be called from
    //constructor and destructor

#ifdef THREADPOOL_IS_STATIC
    boost::this_thread::at_thread_exit(boost::bind(&MysqlCube::on_thread_exit,  tid, tc));
#else

    boost::this_thread::at_thread_exit(boost::bind(&sql::Driver::threadEnd, driver));

    if(connectionPool.size() == 1) {
      for (size_t i = 0; i < dimensions.size(); i++) {
        dimensions[i]->set_connection(tc->connection);
      }
    }

#endif
  }

  if(connectionPool.count(tid) != 1) {
    // we just set this above so it ought to stay set
    LOG(ERROR) << "connectionPool.count(" << tid <<") was " << connectionPool.count(tid);
    LOG(FATAL) << "We should have just initialized that connection..."; //was assert, changed by asr 11/1/12
  }

  return connectionPool[tid];
}

boost::shared_ptr<sql::Connection> MysqlCube::get_connection() {
  return MysqlCube::get_thread_connection()->connection;
}


void MysqlCube::on_thread_exit(boost::thread::id tid, shared_ptr<ThreadConnection> tc) {
  unique_lock<boost::upgrade_mutex> lock(connectionLock);
  //TODO need to lock here
  connectionPool.erase(tid);
  sql::Driver * driver = sql::mysql::get_driver_instance();
  driver->threadEnd();
}

void
jetstream::cube::MysqlCube::execute_sql (const string &sql) {
  try {
    boost::shared_ptr<ThreadConnection> tc = get_thread_connection();
    VLOG(2) << "Executing sql: " << sql;
    tc->statement->execute(sql);
  }
  catch (sql::SQLException &e) {
    LOG(WARNING) << "couldn't execute sql statement; " << e.what() <<
                 "\nStatement was " << sql;
  }
}

boost::shared_ptr<sql::ResultSet>
jetstream::cube::MysqlCube::execute_query_sql(const string &sql) {
  try {
    boost::shared_ptr<ThreadConnection> tc = get_thread_connection();
    VLOG(2) << "Executing query: " << sql;
    boost::shared_ptr<sql::ResultSet> res(tc->statement->executeQuery(sql));
    return res;
  }
  catch (sql::SQLException &e) {
    LOG(WARNING) << "couldn't execute sql statement; " << e.what() <<
                 "\nStatement was " << sql;
  }

  boost::shared_ptr<sql::ResultSet> no_results;
  return no_results;
}

string jetstream::cube::MysqlCube::create_sql(bool aggregate_table) const {
  string sql;

  if(!aggregate_table)
    sql = "CREATE TABLE IF NOT EXISTS `" + get_table_name() + "` (";
  else
    sql = "CREATE TABLE IF NOT EXISTS `" + get_rollup_table_name() + "` (";

  vector<string> pk;

  for(size_t i = 0; i < dimensions.size(); i++) {
    vector<string> names = dimensions[i]->get_column_names();
    vector<string> types = dimensions[i]->get_column_types();

    for (size_t j = 0; j < names.size(); j++) {
      sql += "`" + names[j] + "` " + types[j] + " NOT NULL,";
      pk.push_back("`" + names[j] + "`");
    }

    sql += "`" + dimensions[i]->get_rollup_level_column_name() + "` INT NOT NULL DEFAULT "+boost::lexical_cast<string>(dimensions[i]->leaf_level())+" ,";
    pk.push_back("`" + dimensions[i]->get_rollup_level_column_name() + "`");
  }

  for(size_t i = 0; i < aggregates.size(); i++) {
    vector<string> names = aggregates[i]->get_column_names();
    vector<string> types = aggregates[i]->get_column_types();

    for (size_t j = 0; j < names.size(); j++) {
      sql += "`" + names[j] + "` " + types[j] + " DEFAULT NULL,";
    }
  }

  sql += "PRIMARY KEY (";
  sql += boost::algorithm::join(pk, ", ");
  sql += ")";
  sql += ") CHARACTER SET utf8";
#if MYSQL_INNODB
  sql += " ENGINE=InnoDB";
#else
  sql += " ENGINE=MyISAM";
#endif

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

string jetstream::cube::MysqlCube::get_select_cell_prepared_sql(size_t num_cells, size_t echo) {
  vector<string> where_clauses;

  vector<string> sub_select_columns;
  vector<string> sub_select_null_columns;
  vector<string> on_clauses;
  string echo_select = "";

  for(size_t i=0; i<echo; ++i) {
    string str_i = boost::lexical_cast<string>(i);
    echo_select += "jt.`e"+str_i+"`, ";
    sub_select_columns.push_back("? as `e"+str_i+"`");
    sub_select_null_columns.push_back("NULL");
  }

  for(size_t i=0; i<dimensions.size(); ++i) {
    vector<string> col_names= dimensions[i]->get_column_names();

    for(vector<string>::iterator i_col_names = col_names.begin(); i_col_names != col_names.end(); ++i_col_names) {
      sub_select_columns.push_back("? as `"+*i_col_names+"`");
      sub_select_null_columns.push_back("NULL");
      on_clauses.push_back("mt.`"+*i_col_names+"` = jt.`"+*i_col_names+"`");
    }

    sub_select_columns.push_back("? as `"+dimensions[i]->get_rollup_level_column_name()+"`");
    sub_select_null_columns.push_back("NULL");
    on_clauses.push_back("mt.`"+dimensions[i]->get_rollup_level_column_name()+"` = jt.`"+dimensions[i]->get_rollup_level_column_name()+"`");
  }

  string sub_select_row = "SELECT "+boost::algorithm::join(sub_select_columns, " , ");
  string sub_select_null_row = "SELECT "+boost::algorithm::join(sub_select_null_columns, " , ");
  string on_clause = "("+boost::algorithm::join(on_clauses, " AND ")+")";



  assert(num_cells > 0);
  string sub_select = "(";

  if (num_cells > 1) {
    for(size_t i=0; i < (num_cells-1); i++) {
      sub_select += sub_select_row+" UNION ALL ";
    }

    sub_select += sub_select_row;
  }
  else {
    sub_select += sub_select_row + " UNION ALL "+ sub_select_null_row;
  }

  sub_select +=")";

  string sql = "SELECT "+echo_select+" mt.* FROM `"+get_table_name()+"` mt INNER JOIN "+sub_select+" jt ON "+ on_clause;
  return sql;
}

/*string jetstream::cube::MysqlCube::get_select_cell_prepared_sql(size_t num_cells) {
  vector<string> where_clauses;

  for(size_t i=0; i<dimensions.size(); i++) {
    string where = dimensions[i]->get_where_clause_exact_prepared();
    where_clauses.push_back(where);
    where_clauses.push_back("`"+dimensions[i]->get_rollup_level_column_name()+"` = ?");
  }

  string single_cell_where ="("+ boost::algorithm::join(where_clauses, " AND ")+")";

  }

  string sql = "SELECT * FROM `"+get_table_name()+"` WHERE ";

  assert(num_cells > 0);
  for(size_t i=0; i < (num_cells-1); i++) {
    sql += single_cell_where+" OR ";
  }

  sql += single_cell_where;
  return sql;
}*/

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

    column_names.push_back(dimensions[i]->get_rollup_level_column_name());
    column_values.push_back("?");
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

  assert(batch > 0);

  for(size_t i=0; i < (batch-1); i++) {
    sql += vals+", ";
  }

  sql += vals+" ";
  sql += "ON DUPLICATE KEY UPDATE "+boost::algorithm::join(updates, ", ");
  VLOG(1) << "prepared insert statement: " << sql;
  return sql;
}

boost::shared_ptr<sql::PreparedStatement> MysqlCube::create_prepared_statement(std::string sql, size_t batch) {
  try {
    VLOG(2) << "Create Prepared Statement sql: " << sql;
    boost::shared_ptr<ThreadConnection> tc = get_thread_connection();
    shared_ptr<sql::PreparedStatement> stmnt(tc->connection->prepareStatement(sql));
    return stmnt;
  }
  catch (sql::SQLException &e) {
    LOG(WARNING) << "couldn't execute sql statement (batch=" << batch <<"); " << e.what();
    LOG(WARNING) << "statement was " << sql;
    boost::shared_ptr<sql::PreparedStatement> p;
    return p;
  }
}

boost::shared_ptr<sql::PreparedStatement> MysqlCube::get_lock_prepared_statement(string table_name) {
  string key = "Lock|"+ table_name; //Note we don't add cube name to key since it's redundant with table name

  boost::shared_ptr<ThreadConnection> tc = get_thread_connection();

  if(tc->preparedStatementCache.count(key) == 0) {
    string sql= "LOCK TABLES `"+table_name+"` WRITE" ;
    tc->preparedStatementCache[key] = create_prepared_statement(sql);
  }

  return tc->preparedStatementCache[key];
}

boost::shared_ptr<sql::PreparedStatement> MysqlCube::get_unlock_prepared_statement() {
  string key = "unlock";

  boost::shared_ptr<ThreadConnection> tc = get_thread_connection();

  if(tc->preparedStatementCache.count(key) == 0) {
    string sql= "UNLOCK TABLES";
    tc->preparedStatementCache[key] = create_prepared_statement(sql);
  }

  return tc->preparedStatementCache[key];
}

boost::shared_ptr<sql::PreparedStatement> MysqlCube::get_select_cell_prepared_statement(size_t batch, size_t echo, std::string unique_key) {
  string key = "selectCell|" + name +"|"+ boost::lexical_cast<string>(batch)+"|"+boost::lexical_cast<string>(echo)+"|"+unique_key;

  boost::shared_ptr<ThreadConnection> tc = get_thread_connection();

  if(tc->preparedStatementCache.count(key) == 0) {
    string sql= get_select_cell_prepared_sql(batch, echo);
    tc->preparedStatementCache[key] = create_prepared_statement(sql, batch);
  }

  return tc->preparedStatementCache[key];
}
boost::shared_ptr<sql::PreparedStatement> MysqlCube::get_insert_prepared_statement(size_t batch) {
  string key = "ins_prepared|" + name +"|"+boost::lexical_cast<string>(batch);

  boost::shared_ptr<ThreadConnection> tc = get_thread_connection();

  if(tc->preparedStatementCache.count(key) == 0) {
    string sql= get_insert_prepared_sql(batch);
    tc->preparedStatementCache[key] = create_prepared_statement(sql, batch);
  }

  return tc->preparedStatementCache[key];
}

void MysqlCube::save_tuple(jetstream::Tuple const &t, bool need_new_value, bool need_old_value, boost::shared_ptr<jetstream::Tuple> &new_tuple,boost::shared_ptr<jetstream::Tuple> &old_tuple) {
  vector<unsigned int> levels;

  for(size_t i=0; i<dimensions.size(); i++) {
    levels.push_back(dimensions[i]->leaf_level());
  }

  save_tuple(t, levels, need_new_value, need_old_value, new_tuple, old_tuple);
}

void MysqlCube::save_tuple(jetstream::Tuple const &t, vector<unsigned int> levels, bool need_new_value, bool need_old_value, boost::shared_ptr<jetstream::Tuple> &new_tuple,boost::shared_ptr<jetstream::Tuple> &old_tuple) {
  boost::shared_ptr<sql::PreparedStatement> old_value_stmt;
  boost::shared_ptr<sql::PreparedStatement> insert_stmt;
  boost::shared_ptr<sql::PreparedStatement> new_value_stmt;

  /**** Setup statements *****/
  int field_index;

  if(need_old_value) {
    old_value_stmt = get_select_cell_prepared_statement(1,0, "old_value");
    field_index = 1;

    for(size_t i=0; i<dimensions.size(); i++) {
      dimensions[i]->set_value_for_insert_tuple(old_value_stmt, t, field_index);
      old_value_stmt->setInt(field_index, levels[i]);
      ++field_index;
    }
  }

  insert_stmt = get_insert_prepared_statement(1);
  field_index = 1;

//  LOG(INFO) << "Saving tuple; statement is " << insert_stmt->;
  for(size_t i=0; i<dimensions.size(); i++) {
    dimensions[i]->set_value_for_insert_tuple(insert_stmt, t, field_index);
    insert_stmt->setInt(field_index, levels[i]);
    ++field_index;
  }

  for(size_t i=0; i<aggregates.size(); i++) {
    aggregates[i]->set_value_for_insert_tuple(insert_stmt, t, field_index);
  }

  if(need_new_value) {
    field_index = 1;
    new_value_stmt = get_select_cell_prepared_statement(1, 0, "new_value");

    for(size_t i=0; i<dimensions.size(); i++) {
      dimensions[i]->set_value_for_insert_tuple(new_value_stmt, t, field_index);
      new_value_stmt->setInt(field_index, levels[i]);
      ++field_index;
    }
  }


  /******** Execute statements *******/
#if  MYSQL_TRANSACTIONS 
  #if MYSQL_INNODB
    execute_sql("START TRANSACTION");
  #else
    execute_sql("LOCK TABLES `"+get_table_name()+"` WRITE");
  #endif
#endif


  boost::shared_ptr<sql::ResultSet> old_value_results;

  if(need_old_value) {
    old_value_results.reset(old_value_stmt->executeQuery());
  }

  insert_stmt->execute();

  boost::shared_ptr<sql::ResultSet> new_value_results;

  if(need_new_value) {
    new_value_results.reset(new_value_stmt->executeQuery());
  }
#if  MYSQL_TRANSACTIONS 
  #if MYSQL_INNODB
    execute_sql("COMMIT");
  #else
    execute_sql("UNLOCK TABLES");
  #endif
#endif
  
  /********* Populate tuples ******/
  if(need_old_value && old_value_results->first()) {
    old_tuple = make_tuple_from_result_set(old_value_results, 1, false);
  }
  else {
    old_tuple.reset();
  }

  if(need_new_value && new_value_results->first())
    new_tuple = make_tuple_from_result_set(new_value_results, 1, false);
  else
    new_tuple.reset();
}

void MysqlCube::save_tuple_batch(const std::vector<boost::shared_ptr<jetstream::Tuple> > &tuple_store,
                                 const std::vector<bool> &need_new_value_store, const std::vector<bool> &need_old_value_store,
                                 std::vector<boost::shared_ptr<jetstream::Tuple> > &new_tuple_store, std::vector<boost::shared_ptr<jetstream::Tuple> > &old_tuple_store)

{
  std::vector<boost::shared_ptr<std::vector<unsigned int> > > levels_store;
  boost::shared_ptr<std::vector<unsigned int> > levels = make_shared<std::vector<unsigned int> >();

  for(size_t i=0; i<dimensions.size(); i++) {
    levels->push_back(dimensions[i]->leaf_level());
  }

  for(size_t ti = 0; ti < tuple_store.size(); ++ti) {
    levels_store.push_back(levels);
  }

  return save_tuple_batch(tuple_store, levels_store, need_new_value_store, need_old_value_store, new_tuple_store, old_tuple_store);
}

unsigned int MysqlCube::round_down_to_power_of_two(size_t number, size_t max_power) {
  size_t  power = 0;

  while (number > 1) {
    number = number >> 1;
    power++;
  }

  return (power < max_power ? (1 << power): (1 << max_power));
}

void MysqlCube::save_tuple_batch(const std::vector<boost::shared_ptr<jetstream::Tuple> > &tuple_store,
                                 const std::vector<boost::shared_ptr<std::vector<unsigned int> > > &levels_store,
                                 const std::vector<bool> &need_new_value_store, const std::vector<bool> &need_old_value_store,
                                 std::vector<boost::shared_ptr<jetstream::Tuple> > &new_tuple_store, std::vector<boost::shared_ptr<jetstream::Tuple> > &old_tuple_store) {

#if MYSQL_PROFILE
  msec_t start = get_msec();
#endif
  assert(new_tuple_store.size() == tuple_store.size());
  assert(old_tuple_store.size() == tuple_store.size());

#if MYSQL_UNION_SELECT
  size_t count_old = std::count(need_old_value_store.begin(), need_old_value_store.end(), true);
  size_t count_new = std::count(need_new_value_store.begin(), need_new_value_store.end(), true);
#endif
  /**** Setup statements *****/
  int field_index;


  /*size_t num_placeholders = num_dimensions() + num_aggregates();
  size_t  power_placeholders = 0;
  while (num_placeholders > 1) {
    num_placeholders = num_placeholders >> 1;
    power_placeholders++;
  }
  power_placeholders++;*/


#if MYSQL_PROFILE
  msec_t post_prepare_old = get_msec();
#endif

 
#if MYSQL_PROFILE
  msec_t post_prepare_insert = get_msec();
#endif



  /******** Execute statements *******/
#if MYSQL_PROFILE
  msec_t post_prepare = get_msec();
#endif


#if MYSQL_UNION_SELECT
  std::vector<boost::shared_ptr<sql::ResultSet> > old_value_results;
  if(count_old > 0) {

    size_t store_index = 0;
    size_t count_left = count_old;

    while (count_left > 0 ) {
      size_t count_iter = round_down_to_power_of_two(count_left,  MYSQL_MAX_BATCH_PW_2);
      count_left -= count_iter;

      boost::shared_ptr<sql::PreparedStatement> old_value_stmt = get_select_cell_prepared_statement(count_iter, 1, "old_value");
      field_index = 1;

      size_t added  = 0;

      while (added < count_iter) {
        if(need_old_value_store[store_index]) {
          old_value_stmt->setInt(field_index++, store_index);

          for(size_t i=0; i<dimensions.size(); i++) {
            dimensions[i]->set_value_for_insert_tuple(old_value_stmt, *(tuple_store[store_index]), field_index);
            old_value_stmt->setInt(field_index++, (*(levels_store[store_index]))[i]);
          }

          added++;
        }

        store_index++;
      }

      boost::shared_ptr<sql::ResultSet> pOldVal(old_value_stmt->executeQuery());
      old_value_results.push_back(pOldVal);
    }
  }
#else
  for(size_t i=0; i<need_old_value_store.size(); i++) {
    if(need_old_value_store[i])
    {
      boost::shared_ptr<jetstream::Tuple> res_tuple = get_cell_value(*(tuple_store[i]), *(levels_store[i]), false);
      old_tuple_store[i] = res_tuple;
    }
  }
#endif

#if MYSQL_PROFILE
  msec_t post_old_val = get_msec();
#endif

  size_t count_insert_left = tuple_store.size();
  size_t insert_store_index = 0;
  size_t count_insert_tally = 0;

#if  MYSQL_TRANSACTIONS 
  #if MYSQL_INNODB
    execute_sql("START TRANSACTION");
  #else
    execute_sql("LOCK TABLES `"+get_table_name()+"` WRITE");
  #endif
#endif


  while(count_insert_left > 0) {
    size_t count_insert_iter = round_down_to_power_of_two(count_insert_left,  MYSQL_MAX_BATCH_PW_2);
    count_insert_left -= count_insert_iter;
    count_insert_tally += count_insert_iter;

    boost::shared_ptr<sql::PreparedStatement> insert_stmt = get_insert_prepared_statement(count_insert_iter);

    //msec_t start_time_insert = get_msec();
    field_index = 1;

    for(; insert_store_index < count_insert_tally; ++insert_store_index) {
      LOG_IF(FATAL, levels_store.size() <= insert_store_index) << "levels_store only has " << levels_store.size()
          << " entries; index is " << insert_store_index;
      LOG_IF(FATAL, !levels_store[insert_store_index])<<"levels_store["<<insert_store_index<<"] undefined";
      LOG_IF(FATAL, (*(levels_store[insert_store_index])).size() != dimensions.size())  << "levels_store["<<insert_store_index<<"] does not have the right size. It's size is"
          <<  (*(levels_store[insert_store_index])).size() << " should be "<< dimensions.size();

      for(size_t i=0; i<dimensions.size(); i++) {
        dimensions[i]->set_value_for_insert_tuple(insert_stmt, *(tuple_store[insert_store_index]), field_index);

        insert_stmt->setInt(field_index, (*(levels_store[insert_store_index]))[i]);
        ++field_index;
      }

      for(size_t i=0; i<aggregates.size(); i++) {
        aggregates[i]->set_value_for_insert_tuple(insert_stmt, *(tuple_store[insert_store_index]), field_index);
      }
    }
  
    insert_stmt->execute();
    //LOG(INFO) << "Inserted "<< count_insert_iter << "tuples rate = "<< (double) count_insert_iter/(get_msec() - start_time_insert)<<" tuples/ms"; 
  }


  VLOG(2) << "incrementing version in save_tuple_batch, now " << version;
  version ++;  //next insert will have higher version numbers

#if MYSQL_TRANSACTIONS 
  #if MYSQL_INNODB
    execute_sql("COMMIT");
  #else
    execute_sql("UNLOCK TABLES");
  #endif
#endif

#if MYSQL_PROFILE
  msec_t post_insert = get_msec();
#endif

#if MYSQL_UNION_SELECT
  std::vector<boost::shared_ptr<sql::ResultSet> > new_value_results;
  if(count_new > 0) {

    size_t store_index = 0;
    size_t count_left = count_new;

    while(count_left > 0) {
      size_t count_iter = round_down_to_power_of_two(count_left,  MYSQL_MAX_BATCH_PW_2);
      count_left -= count_iter;

      boost::shared_ptr<sql::PreparedStatement> new_value_stmt = get_select_cell_prepared_statement(count_iter, 1, "new_value");
      field_index = 1;

      size_t added = 0;

      while (added < count_iter) {
        if(need_new_value_store[store_index]) {
          new_value_stmt->setInt(field_index++, store_index);

          for(size_t i=0; i<dimensions.size(); i++) {
            dimensions[i]->set_value_for_insert_tuple(new_value_stmt, *(tuple_store[store_index]), field_index);
            new_value_stmt->setInt(field_index++, (*(levels_store[store_index]))[i]);
          }

          added++;
        }

        store_index++;
      }

      boost::shared_ptr<sql::ResultSet> pNewVal(new_value_stmt->executeQuery());
      new_value_results.push_back(pNewVal);
    }
  }

#else
  for(size_t i=0; i<need_new_value_store.size(); i++) {
    if(need_new_value_store[i])
    {
      boost::shared_ptr<jetstream::Tuple> res_tuple = get_cell_value(*(tuple_store[i]), *(levels_store[i]), false);
      new_tuple_store[i] = res_tuple;
    }
  }
#endif

#if MYSQL_PROFILE
  msec_t post_new_val = get_msec();
#endif



  /********* Populate tuples ******/

#if MYSQL_UNION_SELECT
  for (std::vector<boost::shared_ptr<sql::ResultSet> >::iterator i_old_value_results= old_value_results.begin();
       i_old_value_results != old_value_results.end(); ++i_old_value_results) {
    while ((*i_old_value_results)->next()) {
      int index = (*i_old_value_results)->getInt(1);
      boost::shared_ptr<jetstream::Tuple> old_tuple = make_tuple_from_result_set((*i_old_value_results), 2, false);
      old_tuple_store[index] = old_tuple;
    }
  }

  for (std::vector<boost::shared_ptr<sql::ResultSet> >::iterator i_new_value_results= new_value_results.begin();
       i_new_value_results != new_value_results.end(); ++i_new_value_results) {
    while ((*i_new_value_results)->next()) {
      int index = (*i_new_value_results)->getInt(1);
      boost::shared_ptr<jetstream::Tuple> new_tuple = make_tuple_from_result_set((*i_new_value_results), 2, false);
      new_tuple_store[index] = new_tuple;
    }
  }
#endif

#if MYSQL_PROFILE
  msec_t post_populate = get_msec();

  LOG_IF(INFO, post_populate - start > 500)<< "Flush stats: total " << post_populate-start
      << " preparing old "<< post_prepare_old - start
      << " insert statement  "<< post_prepare_insert - post_prepare_old  << " (" << tuple_store.size() << ")"
      << " preparing new  "<< post_prepare - post_prepare_insert
      << " preparing total: "<< post_prepare - start
      << " exec old val: "<< post_old_val-post_prepare
      << " exec insert: " << post_insert - post_old_val
      << " exec new val: " << post_new_val - post_insert
      << " populating: " << post_populate - post_new_val;
#endif
  /*
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
  */
}

 boost::shared_ptr<sql::PreparedStatement> jetstream::cube::MysqlCube::get_select_one_cell_prepared_statement() {
  string key = "select_one_cell_prepared|"+name;

  boost::shared_ptr<ThreadConnection> tc = get_thread_connection();
  if(tc->preparedStatementCache.count(key) == 0) {
    vector<string> where_clauses;

    for(size_t i=0; i<dimensions.size(); i++) {
      string where = dimensions[i]->get_where_clause_exact_prepared();
      where_clauses.push_back(where);
      where_clauses.push_back("`"+dimensions[i]->get_rollup_level_column_name()+"` = ?");
    }

    string single_cell_where ="("+ boost::algorithm::join(where_clauses, " AND ")+")";

    string sql = "SELECT * FROM `"+get_table_name()+"` WHERE "+single_cell_where;
    tc->preparedStatementCache[key] = create_prepared_statement(sql);
  }
  return tc->preparedStatementCache[key];
}

boost::shared_ptr<jetstream::Tuple> jetstream::cube::MysqlCube::get_cell_value(jetstream::Tuple const &t, std::vector<unsigned int> const &levels, bool final) const {
  boost::shared_ptr<sql::PreparedStatement> select_stmt;
  
  select_stmt =  const_cast<MysqlCube *>(this)->get_select_one_cell_prepared_statement();
  
  int field_index = 1;

  for(size_t i=0; i<dimensions.size(); i++) {
    dimensions[i]->set_value_for_insert_tuple(select_stmt, t, field_index);
    select_stmt->setInt(field_index, levels[i]);
    ++field_index;
  }

  boost::shared_ptr<sql::ResultSet> res;
  res.reset(select_stmt->executeQuery());


  if(res->rowsCount() > 1) {
    LOG(FATAL) << "Something went wrong, fetching more than 1 row per cell";
  }

  if(!res->first()) {
    boost::shared_ptr<jetstream::Tuple> empty;
    return empty;
  }

  return make_tuple_from_result_set(res, 1, final);
}



boost::shared_ptr<jetstream::Tuple>
jetstream::cube::MysqlCube::make_tuple_from_result_set(boost::shared_ptr<sql::ResultSet> res, int column_index /*= 1 */, bool final, bool rollup) const {
  boost::shared_ptr<jetstream::Tuple> result = make_shared<jetstream::Tuple>();

  for(size_t i=0; i<dimensions.size(); i++) {
    dimensions[i]->populate_tuple(result, res, column_index);

    //jetstream::Element *elem = result->add_e();
    //elem->set_i_val(res->getInt(column_index));
    ++column_index; //skip the level column
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

  VLOG(2) << "in slice_query";
  string sql = "SELECT * FROM `"+get_table_name()+"`";

  sql += get_where_clause(min, max);
  sql += get_sort_clause(sort);
  sql += get_limit_clause(limit);
  VLOG(2) << "in slice_query; query is " << sql;
  return get_result_iterator(sql, final);
}

jetstream::cube::CubeIterator
jetstream::cube::MysqlCube::rollup_slice_query(std::vector<unsigned int> const &levels,
    jetstream::Tuple const &min,
    jetstream::Tuple const &max,
    bool final,
    list<string> const &sort,
    size_t limit) const {
  string sql = "SELECT * FROM `"+get_rollup_table_name()+"`";
  sql += get_where_clause(min, max, levels);
  sql += get_sort_clause(sort);
  sql += get_limit_clause(limit);
  VLOG(1) << "in rollup_slice_query; query is " << sql;
  
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

    string suffix = " ASC";

    if(item[0] == '-') {
      suffix = " DESC";
      item = item.erase(0,1);
    }

    if (has_dimension(item)) {
      boost::shared_ptr<MysqlDimension> dim = get_dimension(item);
      sort_sql += dim->get_base_column_name()+suffix;
    }
    else if(has_aggregate(item)) {
      boost::shared_ptr<MysqlAggregate> agg = get_aggregate(item);
      sort_sql += agg->get_base_column_name()+suffix;
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

string MysqlCube::get_where_clause(jetstream::Tuple const &min, jetstream::Tuple const &max, std::vector<unsigned int> const &levels) const {

  assert(levels.empty() || levels.size() == dimensions.size());
  int tuple_index_min = 0;
  int tuple_index_max = 0;
  vector<string> where_clauses;

  vector<unsigned int>::const_iterator iLevel = levels.begin();

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
  boost::shared_ptr<sql::ResultSet> res = const_cast<MysqlCube *>(this)->execute_query_sql(sql);
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

size_t jetstream::cube::MysqlCube::num_leaf_cells() const {
  string sql = "SELECT COUNT(*) FROM `"+get_table_name()+"`";

  boost::shared_ptr<sql::ResultSet> res = const_cast<MysqlCube *>(this)->execute_query_sql(sql);

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
MysqlCube::do_rollup(std::vector<unsigned int> const &levels, jetstream::Tuple const &min, jetstream::Tuple const& max) {
  assert(levels.size() == dimensions.size());

  vector<string> column_names;
  vector<string> select_clause;
  vector<string> groupby_clause;

  vector<unsigned int>::const_iterator iLevel = levels.begin();

  for(size_t i=0; i<dimensions.size(); i++) {
    vector<string> names = dimensions[i]->get_column_names();

    for (size_t j = 0; j < names.size(); j++) {
      column_names.push_back("`"+names[j]+"`");
    }

    column_names.push_back(dimensions[i]->get_rollup_level_column_name());
    select_clause.push_back(dimensions[i]->get_select_clause_for_rollup(*iLevel));

    if(*iLevel > 0) {
      string groupby = dimensions[i]->get_groupby_clause_for_rollup(*iLevel);

      if(!groupby.empty()) {
        groupby_clause.push_back(groupby);
      }
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

  string sql = "REPLACE INTO `"+get_rollup_table_name()+"`";
  sql += " ("+boost::algorithm::join(column_names, ", ")+") ";
  sql += "SELECT "+boost::algorithm::join(select_clause, ", ");
  sql += " FROM "+get_table_name();
  sql += get_where_clause(min, max);

  if(!groupby_clause.empty()) {
    sql += " GROUP BY "+boost::algorithm::join(groupby_clause, ", ");
  }

//  sql += " ON DUPLICATE KEY UPDATE";
  VLOG(1) << "in rollup_slice_query; query is " << sql;

  execute_sql(sql);
}



boost::shared_ptr<std::map<std::string, int> >
MysqlCube::list_sql_cubes() {

  boost::shared_ptr<map<string,int> > cubeList(new map<string,int>());

  sql::Driver * driver = sql::mysql::get_driver_instance();
  boost::shared_ptr<MysqlCube::ThreadConnection> tconn =  MysqlCube::get_uncached_connection(driver);
  sql::ResultSet * res = tconn->statement->executeQuery("SHOW TABLES");

  while (res->next()) {
    string s = res->getString(1);
    cout << s << endl;

    if (s.substr(s.length() - 8).rfind("_rollup") != string::npos) {
      (*cubeList)[s] = 1;
    }
  }

  return cubeList;
}


void MysqlCube::set_db_params(string host, string user, string pass, string name) {
  db_host = host;
  db_user = user;
  db_pass = pass;
  db_name = name;
}

string MysqlCube::db_host;
string MysqlCube::db_user;
string MysqlCube::db_pass;
string MysqlCube::db_name;

#ifdef THREADPOOL_IS_STATIC
std::map<boost::thread::id, boost::shared_ptr<MysqlCube::ThreadConnection> >
MysqlCube::connectionPool;
boost::upgrade_mutex MysqlCube::connectionLock;
#endif
