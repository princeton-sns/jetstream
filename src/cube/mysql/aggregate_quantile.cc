#include "aggregate_quantile.h"

#include <glog/logging.h>
#include "js_utils.h"

using namespace std;
using namespace jetstream::cube;

template<>
string MysqlAggregateQuantile<jetstream::LogHistogram>::get_select_clause_for_rollup() const {
  return "merge_histogram(`"+get_base_column_name()+"`)";
}
template<>
string MysqlAggregateQuantile<jetstream::ReservoirSample>::get_select_clause_for_rollup() const {
  return "merge_reservoir_sample(`"+get_base_column_name()+"`)";
}
template<>
string MysqlAggregateQuantile<jetstream::CMMultiSketch>::get_select_clause_for_rollup() const {
  return "merge_histogram(`"+get_base_column_name()+"`)";
}

template<>
string MysqlAggregateQuantile<jetstream::LogHistogram>::get_update_on_insert_sql() const {
  string sql = "`"+get_base_column_name()+"` = merge_pair_histogram(`"+get_base_column_name()+"`, VALUES(`"+get_base_column_name()+"`))";
  return sql;
}
template<>
string MysqlAggregateQuantile<jetstream::ReservoirSample>::get_update_on_insert_sql() const {
  string sql = "`"+get_base_column_name()+"` = merge_pair_reservoir_sample(`"+get_base_column_name()+"`, VALUES(`"+get_base_column_name()+"`))";
  return sql;
}
template<>
string MysqlAggregateQuantile<jetstream::CMMultiSketch>::get_update_on_insert_sql() const {
  string sql = "`"+get_base_column_name()+"` = merge_pair_sketch(`"+get_base_column_name()+"`, VALUES(`"+get_base_column_name()+"`))";
  return sql;
}


