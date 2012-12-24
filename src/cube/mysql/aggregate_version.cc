

#include "aggregate_version.h"

#include <glog/logging.h>
#include "js_utils.h"

using namespace std;
using namespace jetstream::cube;

vector<string>
MysqlAggregateVersion::get_column_types() const {
  vector<string> decl;
  decl.push_back("BIGINT");
  return decl;
}


void
MysqlAggregateVersion::merge_full_tuple_into(jetstream::Tuple &into, jetstream::Tuple const &update) const {
  u_int64_t update_v = update.version();
  if (update_v > into.version())
    into.set_version(update_v);
}

string
MysqlAggregateVersion::get_update_on_insert_sql() const {
  string sql = "`"+get_base_column_name()+"` = `"+get_base_column_name()+"` + VALUES(`"+get_base_column_name()+"`)";
  return sql;
}

void
MysqlAggregateVersion::insert_default_values_for_full_tuple(jetstream::Tuple &t) const {
  //do nothing here; this is just initialization which we don't need
}

size_t
MysqlAggregateVersion::number_tuple_elements() const {
  return 0;
}


void
MysqlAggregateVersion::set_value_for_insert_tuple ( shared_ptr<sql::PreparedStatement> pstmt,
                                                    const jetstream::Tuple &t,
                                                    int &field_index) {
  pstmt->setUInt64(field_index, next_update_id++);
  field_index += 1;
  return;
}


void
MysqlAggregateVersion::populate_tuple_final ( boost::shared_ptr<jetstream::Tuple> t,
                                              boost::shared_ptr<sql::ResultSet> resultset,
                                              int &column_index) const {
  uint64_t v = resultset->getInt(column_index);
  ++column_index;
  t->set_version(v);
}

void
MysqlAggregateVersion::populate_tuple_partial ( boost::shared_ptr<jetstream::Tuple> t,
                                                boost::shared_ptr<sql::ResultSet> resultset,
                                                int &column_index) const {
  populate_tuple_final(t, resultset, column_index);
}

string
MysqlAggregateVersion::get_select_clause_for_rollup() const {
  return "MAX("+get_base_column_name()+")";
}
