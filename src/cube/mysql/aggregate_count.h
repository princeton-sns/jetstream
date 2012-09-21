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
    MysqlAggregateCount(jetstream::CubeSchema_Aggregate _schema) : MysqlAggregate(_schema) {};

    vector<string> get_column_types() const;

    string get_update_with_new_entry_sql() const;


    string get_update_with_partial_aggregate_sql() const;

    void set_value_for_insert_entry(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &tuple_index, int &field_index) const;


    void set_value_for_insert_partial_aggregate(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &tuple_index, int &field_index) const;






    void populate_tuple_final(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const ;

    void populate_tuple_partial(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const ;

};


} /* cube */
} /* jetstream */


#endif /* end of include guard: AGGREGATE_COUNT_Q8TYGR7Q */
