#ifndef AGGREGATE_COUNT_Q8TYGR7Q
#define AGGREGATE_COUNT_Q8TYGR7Q

#include "aggregate.h"

namespace jetstream {
namespace cube {

/**
 * @brief Aggregate for simple counts
 *
 * format of input tuples:
 *
 * single entry: (none)
 * partial aggregate: [count of items]
 */
class MysqlAggregateCount: public MysqlAggregate {
  public:
    MysqlAggregateCount() : MysqlAggregate() {};

    vector<string> get_column_types() const;

    string get_update_on_insert_sql() const;

    virtual void insert_default_values_for_full_tuple(jetstream::Tuple &t) const;
    size_t number_tuple_elements() const;
  
    virtual void set_value_for_insert_tuple(
      shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t,
      int &field_index) const;

    void set_value_for_insert_entry(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &tuple_index, int &field_index) const;


    void set_value_for_insert_partial_aggregate(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &tuple_index, int &field_index) const;

    void populate_tuple_final(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const ;

    void populate_tuple_partial(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const ;
    
    virtual string get_select_clause_for_rollup() const;
  protected:
    virtual void merge_full_tuple_into(jetstream::Tuple &into, jetstream::Tuple const &update) const;

};


} /* cube */
} /* jetstream */


#endif /* end of include guard: AGGREGATE_COUNT_Q8TYGR7Q */
