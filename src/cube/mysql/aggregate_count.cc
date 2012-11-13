#include "aggregate_count.h"

#include <glog/logging.h>
#include "js_utils.h"

using namespace std;
using namespace jetstream::cube;

vector<string>  MysqlAggregateCount::get_column_types() const {
  vector<string> decl;
  decl.push_back("INT");
  return decl;
}
void MysqlAggregateCount::merge_full_tuple_into(jetstream::Tuple &into, jetstream::Tuple const &update) const {
  jetstream::Element * const e_count_update = const_cast<jetstream::Tuple &>(update).mutable_e(tuple_indexes[0]);
  jetstream::Element * e_count_into = into.mutable_e(tuple_indexes[0]);
  e_count_into->set_i_val(e_count_into->i_val()+e_count_update->i_val());
}

string MysqlAggregateCount::get_update_on_insert_sql() const {
  string sql = "`"+get_base_column_name()+"` = `"+get_base_column_name()+"` + VALUES(`"+get_base_column_name()+"`)";
  return sql;
}

void MysqlAggregateCount::insert_default_values_for_full_tuple(jetstream::Tuple &t) const {  
  jetstream::Element * e_count = t.mutable_e(tuple_indexes[0]);
  if(!e_count->has_i_val())
  {
    e_count->set_i_val(1);
  }
}

size_t  MysqlAggregateCount::number_tuple_elements() const
{
  return 1;
}


void MysqlAggregateCount::set_value_for_insert_tuple(shared_ptr<sql::PreparedStatement> pstmt,  const jetstream::Tuple &t, int &field_index) const {
  if(tuple_indexes.size() != 1)
  {
    LOG(FATAL) << "Wrong number of input tuple indexes for "<< name;
  }
  if(t.e_size()-1 >= (int) tuple_indexes[0])
  {
    const jetstream::Element&  e = t.e(tuple_indexes[0]);
    if(e.has_i_val()) {
      pstmt->setInt(field_index, e.i_val());
      field_index += 1;
      return;
    } else
    LOG(FATAL) << "Expected field "<< tuple_indexes[0] << " to be an int corresponding to "<< name <<
      "\n. Tuple was " << jetstream::fmt(t);
  }
  else
  {
    pstmt->setInt(field_index, 1);
    field_index += 1;
    return;
  }
}


void MysqlAggregateCount::populate_tuple_final(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
  int count = resultset->getInt(column_index);
  ++column_index;
  jetstream::Element * elem = t->add_e();
  elem->set_i_val(count);
}

void MysqlAggregateCount::populate_tuple_partial(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
  populate_tuple_final(t, resultset, column_index);
}

string MysqlAggregateCount::get_select_clause_for_rollup() const {
  return "SUM("+get_base_column_name()+")";
}

