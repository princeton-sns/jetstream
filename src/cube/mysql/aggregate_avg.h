#ifndef AGGREGATE_AVG_Q8TYGR7Q
#define AGGREGATE_AVG_Q8TYGR7Q

#include "aggregate.h"
#include <glog/logging.h>

namespace jetstream {
namespace cube {

/**
 * @brief Aggregate for avgs
 *
 * input tuple format:
 *
 * single entry: [value to be averaged as int or double]
 * partial aggregate: [sum as int or double] [count of items in sum as int]
 *
 */
class MysqlAggregateAvg: public MysqlAggregate {
  public:
    MysqlAggregateAvg(jetstream::CubeSchema_Aggregate _schema) : MysqlAggregate(_schema) {};

    vector<string> get_column_types() const;

    vector<string> get_column_names() const;
    string get_update_on_insert_sql() const;

    virtual void insert_default_values_for_full_tuple(jetstream::Tuple &t) const;
    size_t number_tuple_elements() const;
    
    virtual void set_value_for_insert_tuple(
      shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t,
      int &field_index) const;
    
    void set_value_for_insert_entry(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &tuple_index, int &field_index) const;

    void set_value_for_insert_partial_aggregate(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &tuple_index, int &field_index) const;

    virtual void populate_tuple_final(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const ;

    void populate_tuple_partial(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const ;
    
    virtual string get_select_clause_for_rollup() const;

  protected:
    virtual void merge_full_tuple_into(jetstream::Tuple &into, jetstream::Tuple const &update) const;
    void merge_sum(jetstream::Element * into, jetstream::Element * const update) const;
    
    virtual void set_value(
      shared_ptr<sql::PreparedStatement> pstmt,int &field_index, jetstream::Element *const sum) const;
    
    virtual void set_value(
      shared_ptr<sql::PreparedStatement> pstmt,int &field_index, jetstream::Element *const sum, jetstream::Element *const count) const;
};

} /* cube */
} /* jetstream */


#endif /* end of include guard: AGGREGATE_AVG_Q8TYGR7Q */
