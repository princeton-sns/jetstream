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
    string get_update_with_new_entry_sql() const;

    string get_update_with_partial_aggregate_sql() const;

    void set_value_for_insert_entry(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &tuple_index, int &field_index) const;

    void set_value_for_insert_partial_aggregate(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &tuple_index, int &field_index) const;

    virtual void populate_tuple_final(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const ;

    void populate_tuple_partial(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const ;
};

} /* cube */
} /* jetstream */


#endif /* end of include guard: AGGREGATE_AVG_Q8TYGR7Q */
