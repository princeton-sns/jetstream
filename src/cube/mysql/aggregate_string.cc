#include "aggregate_string.h"

#include <glog/logging.h>

using namespace std;
using namespace jetstream::cube;

vector<string>  MysqlAggregateString::get_column_types() const {
  vector<string> decl;
  decl.push_back("VARCHAR(255)");
  return decl;
}
void MysqlAggregateString::merge_full_tuple_into(jetstream::Tuple &into, jetstream::Tuple const &update) const {
  (into.mutable_e(tuple_indexes[0]))->set_s_val(update.e(tuple_indexes[0]).s_val());
}

string MysqlAggregateString::get_update_on_insert_sql() const {
  string sql = "`"+get_base_column_name()+"` = VALUES(`"+get_base_column_name()+"`)";
  return sql;
}

void MysqlAggregateString::insert_default_values_for_full_tuple(jetstream::Tuple &t) const {  
}

size_t  MysqlAggregateString::number_tuple_elements() const
{
  return 1;
}


void
MysqlAggregateString::set_value_for_insert_tuple ( shared_ptr<sql::PreparedStatement> pstmt,
                                                   jetstream::Tuple const &t,
                                                   int &field_index)   {
  if(tuple_indexes.size() != 1)
  {
    LOG(FATAL) << "Wrong number of input tuple indexes for "<< name;
  }
  pstmt->setString(field_index, t.e(tuple_indexes[0]).s_val());
  field_index += 1;
}

void MysqlAggregateString::populate_tuple_final(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
  string s = resultset->getString(column_index);
  ++column_index;
  jetstream::Element * elem = t->add_e();
  elem->set_s_val(s);
}

void MysqlAggregateString::populate_tuple_partial(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
  populate_tuple_final(t, resultset, column_index);
}

string MysqlAggregateString::get_select_clause_for_rollup() const {
  return "GROUP_CONCAT(DISTINCT `"+get_base_column_name()+"`)";
}

