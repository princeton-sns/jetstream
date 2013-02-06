#ifndef CUBE_6ITS9P4J
#define CUBE_6ITS9P4J

#include "../cube_impl.h"
#include "../cube_iterator.h"
#include "dimension.h"
#include "aggregate.h"
#include <boost/algorithm/string/join.hpp>

#include "mysql_driver.h"
#include "mysql_connection.h"
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <boost/enable_shared_from_this.hpp>


#undef THREADPOOL_IS_STATIC

namespace jetstream {
namespace cube {

class MysqlCube : public DataCubeImpl<MysqlDimension, MysqlAggregate>, public boost::enable_shared_from_this<MysqlCube> {
  public:
    friend class MysqlCubeIteratorImpl;

    MysqlCube (jetstream::CubeSchema const _schema,
               string _name,
               bool overwrite_if_present, const NodeConfig &conf = NodeConfig());

    class ThreadConnection{
      public:
        boost::shared_ptr<sql::Connection> connection;
        boost::shared_ptr<sql::Statement> statement;
        std::map<std::string, boost::shared_ptr<sql::PreparedStatement> > preparedStatementCache;
    };



   virtual void save_tuple(jetstream::Tuple const &t, bool need_new_value, bool need_old_value, boost::shared_ptr<jetstream::Tuple> &new_tuple,boost::shared_ptr<jetstream::Tuple> &old_tuple);
  
   virtual void save_tuple_batch(std::vector<boost::shared_ptr<jetstream::Tuple> > tuple_store, 
       std::vector<bool> need_new_value_store, std::vector<bool> need_old_value_store, 
       std::list<boost::shared_ptr<jetstream::Tuple> > &new_tuple_list, std::list<boost::shared_ptr<jetstream::Tuple> > &old_tuple_list);
/*
    virtual size_t 
      insert_tuple(jetstream::Tuple const &t, bool batch, bool need_new_value, bool need_old_value);
    virtual size_t 
      update_batched_tuple(size_t pos, jetstream::Tuple const &t, bool batch, bool need_new_value, bool need_old_value);
*/
    // virtual bool insert_full_aggregate(jetstream::Tuple t);

    virtual boost::shared_ptr<jetstream::Tuple> get_cell_value_final(jetstream::Tuple const &t) const;
    virtual boost::shared_ptr<jetstream::Tuple> get_cell_value_partial(jetstream::Tuple const &t) const ;
    virtual boost::shared_ptr<jetstream::Tuple> get_cell_value(jetstream::Tuple const &t, bool final = true) const;

    virtual CubeIterator 
      slice_query(jetstream::Tuple const &min, jetstream::Tuple const &max, bool final = true, 
        list<string> const &sort = list<string>(), size_t limit = 0) const;
    virtual CubeIterator 
      rollup_slice_query(std::vector<unsigned int> const &levels, jetstream::Tuple const &min, 
        jetstream::Tuple const &max, bool final = true, list<string> const &sort = list<string>(), size_t limit = 0) const;
    
    virtual CubeIterator end() const;

    virtual size_t num_leaf_cells() const;

    void create();
    void destroy();

    string get_table_name() const;
    string get_rollup_table_name() const;
    vector<string> get_dimension_column_types() const;
    vector<string> get_aggregate_column_types() const;

    string create_sql(bool aggregate_table = false) const;

    virtual void
    do_rollup(std::vector<unsigned int> const &levels,jetstream::Tuple const &min, jetstream::Tuple const& max);

    virtual ~MysqlCube();

  protected:
    /*
    void insert_one(jetstream::Tuple const &t);
    size_t insert_batch(jetstream::Tuple const &t);
    void flush_batch();*/


    string get_sort_clause(list<string> const &sort) const;
    string get_limit_clause(size_t limit) const;
    string get_where_clause(jetstream::Tuple const &min,
                            jetstream::Tuple const &max,
                            std::vector<unsigned int> const &levels = std::vector<unsigned int>()) const;
    CubeIterator get_result_iterator(string sql, bool final, bool rollup=false) const;



    string get_insert_prepared_sql(size_t batch);
    string get_select_cell_prepared_sql(size_t num_cells);

    boost::shared_ptr<sql::PreparedStatement> get_insert_prepared_statement(size_t batch);
    boost::shared_ptr<sql::PreparedStatement> get_select_cell_prepared_statement(size_t batch, std::string unique_key="");
    boost::shared_ptr<sql::PreparedStatement> get_lock_prepared_statement(string table_name);
    boost::shared_ptr<sql::PreparedStatement> get_unlock_prepared_statement();
    boost::shared_ptr<sql::PreparedStatement> create_prepared_statement(std::string sql);
    boost::shared_ptr<jetstream::Tuple> make_tuple_from_result_set(boost::shared_ptr<sql::ResultSet> res, bool final, bool rollup=false) const;

  private:
    static void init_connection();
    static string db_host;
    static string db_user;
    static string db_pass;
    static string db_name;

    bool assumeOnlyWriter;

    boost::shared_ptr<sql::ResultSet> slice_result_set;
    bool slice_final;
  
#ifdef THREADPOOL_IS_STATIC
#define THREADPOOL_STATIC static
#else
#define THREADPOOL_STATIC 
#endif
  
    THREADPOOL_STATIC std::map<boost::thread::id, boost::shared_ptr<ThreadConnection> > connectionPool;
    THREADPOOL_STATIC boost::upgrade_mutex connectionLock;
  
  public:

    static void set_db_params(string db_host="localhost",
                       string db_user="root",
                       string db_pass="",
                       string db_name="test_cube");
    THREADPOOL_STATIC boost::shared_ptr<ThreadConnection> get_thread_connection();
    THREADPOOL_STATIC boost::shared_ptr<sql::Connection> get_connection();
    THREADPOOL_STATIC void execute_sql(string const &sql);
    THREADPOOL_STATIC boost::shared_ptr<sql::ResultSet> execute_query_sql(string const &sql);  
    THREADPOOL_STATIC void on_thread_exit(boost::thread::id tid, shared_ptr<ThreadConnection> tc);

    static boost::shared_ptr<std::map<std::string, int> > list_sql_cubes();
    static boost::shared_ptr<ThreadConnection>  get_uncached_connection(sql::Driver * driver);
};


} /* cube */
} /* jetstream */
#endif /* end of include guard: CUBE_6ITS9P4J */
