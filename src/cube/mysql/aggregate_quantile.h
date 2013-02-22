#ifndef AGGREGATE_QUANT_Q8TYGR7Q
#define AGGREGATE_QUANT_Q8TYGR7Q

#include "aggregate.h"
#include "cm_sketch.h"
#include "quantile_est.h"
#include "js_utils.h"
#include <glog/logging.h>


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

template<class Aggregate>
class MysqlAggregateQuantile: public MysqlAggregate {
  public:
    MysqlAggregateQuantile() : MysqlAggregate() {};

    vector<string> get_column_types() const;

    string get_update_on_insert_sql() const;

    virtual void insert_default_values_for_full_tuple(jetstream::Tuple &t) const;
    size_t number_tuple_elements() const;

    virtual void set_value_for_insert_tuple(
      shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t,
      int &field_index);

    void populate_tuple_final(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const ;

    void populate_tuple_partial(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const ;

    virtual string get_select_clause_for_rollup() const;
  protected:
    virtual void merge_full_tuple_into(jetstream::Tuple &into, jetstream::Tuple const &update) const;

};

template <class Aggregate>
vector<string>  MysqlAggregateQuantile<Aggregate>::get_column_types() const {
  vector<string> decl;
  decl.push_back("BLOB");
  return decl;
}

template <class AggregateClass>
void MysqlAggregateQuantile<AggregateClass>::merge_full_tuple_into(jetstream::Tuple &into, jetstream::Tuple const &update) const {
  //!!!!need to serialize and unserialize here!!!!!
  jetstream::JSSummary  *sum_into = into.mutable_e(tuple_indexes[0])->mutable_summary();
  const jetstream::JSSummary  & sum_update = update.e(tuple_indexes[0]).summary();

  merge_summary<AggregateClass>(*sum_into, sum_update);
}

template <class Aggregate>
string MysqlAggregateQuantile<Aggregate>::get_update_on_insert_sql() const {
  assert(0);
  return "";
}
template<>
string MysqlAggregateQuantile<jetstream::LogHistogram>::get_update_on_insert_sql() const ;

template<>
string MysqlAggregateQuantile<jetstream::ReservoirSample>::get_update_on_insert_sql() const;
template<>
string MysqlAggregateQuantile<jetstream::CMMultiSketch>::get_update_on_insert_sql() const;

template<class Aggregate>
void MysqlAggregateQuantile<Aggregate>::insert_default_values_for_full_tuple(jetstream::Tuple &t) const {
}

template<class Aggregate>
size_t  MysqlAggregateQuantile<Aggregate>::number_tuple_elements() const
{
  return 1;
}


template<class Aggregate>
void
MysqlAggregateQuantile<Aggregate>::set_value_for_insert_tuple ( shared_ptr<sql::PreparedStatement> pstmt,
                                                   jetstream::Tuple const &t,
                                                   int &field_index)   {
  if(tuple_indexes.size() != 1)
  {
    LOG(FATAL) << "Wrong number of input tuple indexes for "<< name;
  }
  const JSSummary & sum = t.e(tuple_indexes[0]).summary();
  pstmt->setString(field_index, sum.SerializeAsString());
//  cout << "putting tuple  " << jetstream::fmt(t) << " into DB" << endl;
  field_index += 1;
}

template<class Aggregate>
void MysqlAggregateQuantile<Aggregate>::populate_tuple_final(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
  string s = resultset->getString(column_index);
  ++column_index;
  jetstream::Element * elem = t->add_e();
  jetstream::JSSummary *sum =  elem->mutable_summary();
  sum->ParseFromString(s);
//  cout << "pulled tuple  " << jetstream::fmt(*t) << " from DB" << endl;

}

template<class Aggregate>
void MysqlAggregateQuantile<Aggregate>::populate_tuple_partial(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
  populate_tuple_final(t, resultset, column_index);
}

template<class Aggregate>
string MysqlAggregateQuantile<Aggregate>::get_select_clause_for_rollup() const {
  assert(0);
  return "";
}

template<>
string MysqlAggregateQuantile<jetstream::LogHistogram>::get_select_clause_for_rollup() const;

template<>
string MysqlAggregateQuantile<jetstream::ReservoirSample>::get_select_clause_for_rollup() const;

template<>
string MysqlAggregateQuantile<jetstream::CMMultiSketch>::get_select_clause_for_rollup() const;






} /* cube */
} /* jetstream */


#endif /* end of include guard: AGGREGATE_QUANT_Q8TYGR7Q */
