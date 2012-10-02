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

namespace jetstream {
namespace cube {

class MysqlCube : public DataCubeImpl<MysqlDimension, MysqlAggregate>, public boost::enable_shared_from_this<MysqlCube> {
  public:
    friend class MysqlCubeIteratorImpl;

    MysqlCube (jetstream::CubeSchema const _schema,
               string _name,
               bool overwrite_if_present,
               string db_host="localhost",
               string db_user="root",
               string db_pass="",
               string db_name="test_cube",
               size_t batch=1);


    virtual bool insert_entry(jetstream::Tuple const &t);
    virtual bool insert_partial_aggregate(jetstream::Tuple const &t);
    // virtual bool insert_full_aggregate(jetstream::Tuple t);

    virtual boost::shared_ptr<jetstream::Tuple> get_cell_value_final(jetstream::Tuple const &t) const;
    virtual boost::shared_ptr<jetstream::Tuple> get_cell_value_partial(jetstream::Tuple const &t) const ;
    virtual boost::shared_ptr<jetstream::Tuple> get_cell_value(jetstream::Tuple const &t, bool final = true) const;

    virtual CubeIterator slice_query(jetstream::Tuple const &min, jetstream::Tuple const &max, bool final = true, list<string> const &sort = list<string>(), size_t limit = 0) const;
    virtual CubeIterator rollup_slice_query(std::list<unsigned int> const &levels, jetstream::Tuple const &min, jetstream::Tuple const &max, bool final = true, list<string> const &sort = list<string>(), size_t limit = 0) const;
    virtual CubeIterator end() const;

    virtual size_t num_leaf_cells() const;

    void create();
    void destroy();

    string get_table_name() const;
    string get_rollup_table_name() const;
    vector<string> get_dimension_column_types() const;
    vector<string> get_aggregate_column_types() const;

    void set_batch(size_t numBatch);

    string create_sql(bool aggregate_table = false) const;

    virtual void
    do_rollup(std::list<unsigned int> const &levels,jetstream::Tuple const &min, jetstream::Tuple const& max);

    virtual ~MysqlCube() {
      if(is_frozen) {
        destroy();
      }
    }

  protected:

    string get_sort_clause(list<string> const &sort) const;
    string get_limit_clause(size_t limit) const;
    string get_where_clause(jetstream::Tuple const &min, jetstream::Tuple const &max,  std::list<unsigned int> const &levels = std::list<unsigned int>()) const;
    CubeIterator get_result_iterator(string sql, bool final, bool rollup=false) const;


    boost::shared_ptr<sql::Connection> get_connection() const;
    void execute_sql(string const &sql) const;
    boost::shared_ptr<sql::ResultSet> execute_query_sql(string const &sql) const;

    string get_insert_entry_prepared_sql();
    string get_insert_partial_aggregate_prepared_sql();

    boost::shared_ptr<sql::PreparedStatement> get_insert_entry_prepared_statement();
    boost::shared_ptr<sql::PreparedStatement> get_insert_partial_aggregate_prepared_statement();
    boost::shared_ptr<jetstream::Tuple> make_tuple_from_result_set(boost::shared_ptr<sql::ResultSet> res, bool final, bool rollup=false) const;

  private:
    void init_connection();
    string db_host;
    string db_user;
    string db_pass;
    string db_name;

    boost::shared_ptr<sql::Connection> connection;
    boost::shared_ptr<sql::Statement> statement;

    size_t batch;

    size_t insertEntryCurrentBatch;
    size_t insertPartialAggregateCurrentBatch;

    size_t numFieldsPerInsertEntryBatch;
    size_t numFieldsPerPartialAggregateBatch;

    boost::shared_ptr<sql::PreparedStatement> insertEntryStatement;
    boost::shared_ptr<sql::PreparedStatement> insertPartialAggregateStatement;

    boost::shared_ptr<sql::ResultSet> slice_result_set;
    bool slice_final;

};


} /* cube */
} /* jetstream */
#endif /* end of include guard: CUBE_6ITS9P4J */
