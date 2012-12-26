#include "aggregate_avg.h"

using namespace std;
using namespace jetstream::cube;


vector<string>  MysqlAggregateAvg::get_column_types() const {
  vector<string> decl;
  decl.push_back("INT");
  decl.push_back("INT");
  return decl;
}

vector<string> MysqlAggregateAvg::get_column_names() const {
  vector<string> decl;
  decl.push_back(get_base_column_name()+"_sum");
  decl.push_back(get_base_column_name()+"_count");
  return decl;
}

string MysqlAggregateAvg::get_update_on_insert_sql() const {
  //VALUES() allow you to incorporate the value of the new entry as it would be if the entry was inserted as a new row;
  string sql = "`"+get_base_column_name()+"_sum` = `"+get_base_column_name()+"_sum` + VALUES(`"+get_base_column_name()+"_sum`), ";
  sql += "`"+get_base_column_name()+"_count` = `"+get_base_column_name()+"_count` +  VALUES(`"+get_base_column_name()+"_count`)";
  return sql;
}

void MysqlAggregateAvg::insert_default_values_for_full_tuple(jetstream::Tuple &t) const {
  jetstream::Element * e_count = t.mutable_e(tuple_indexes[1]);

  if(!e_count->has_i_val()) {
    e_count->set_i_val(1);
  }
}

size_t MysqlAggregateAvg::number_tuple_elements() const {
  return 2;
}

void MysqlAggregateAvg::merge_full_tuple_into(jetstream::Tuple &into, jetstream::Tuple const &update) const {
  jetstream::Element * const e_sum_update = const_cast<jetstream::Tuple &>(update).mutable_e(tuple_indexes[0]);
  jetstream::Element * e_sum_into = into.mutable_e(tuple_indexes[0]);
  merge_sum(e_sum_into, e_sum_update);

  if(update.e_size()-1 >= (int)tuple_indexes[1]) {
    jetstream::Element * const e_count_update = const_cast<jetstream::Tuple &>(update).mutable_e(tuple_indexes[1]);
    jetstream::Element * e_count_into = into.mutable_e(tuple_indexes[1]);
    e_count_into->set_i_val(e_count_into->i_val()+e_count_update->i_val());
  }
  else {
    jetstream::Element * e_count_into = into.mutable_e(tuple_indexes[1]);
    e_count_into->set_i_val(e_count_into->i_val()+1);

  }
}

void
MysqlAggregateAvg::set_value_for_insert_tuple( shared_ptr<sql::PreparedStatement> pstmt,
                                               jetstream::Tuple const &t,
                                               int &field_index)  {
  if(tuple_indexes.size() != 2) {
    LOG(FATAL) << "Wrong number of input tuple indexes for "<< name;
  }

  if(t.e_size()-1 >= (int)tuple_indexes[1]) {
    jetstream::Element * const e_sum = const_cast<jetstream::Tuple &>(t).mutable_e(tuple_indexes[0]);
    jetstream::Element * const e_count = const_cast<jetstream::Tuple &>(t).mutable_e(tuple_indexes[1]);
    set_value(pstmt, field_index, e_sum, e_count);
  }
  else {
    jetstream::Element * const e_sum = const_cast<jetstream::Tuple &>(t).mutable_e(tuple_indexes[0]);
    set_value(pstmt, field_index, e_sum);
  }
}

void MysqlAggregateAvg::merge_sum(jetstream::Element * into, jetstream::Element * const update) const {
  if(into->has_i_val())
    into->set_i_val(into->i_val() + update->i_val());
  else
    into->set_d_val(into->d_val() + update->d_val());
}

void MysqlAggregateAvg::set_value(shared_ptr<sql::PreparedStatement> pstmt, int &field_index, jetstream::Element *const sum) const {
  if(sum->has_i_val()) {
    pstmt->setInt(field_index, sum->i_val());
    pstmt->setInt(field_index+1, 1);
    field_index += 2;
    return;
  }

  if(sum->has_d_val()) {
    pstmt->setDouble(field_index, sum->d_val());
    pstmt->setInt(field_index+1, 1);
    field_index += 2;
    return;
  }

  LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
}

void MysqlAggregateAvg::set_value(shared_ptr<sql::PreparedStatement> pstmt, int &field_index, jetstream::Element *const sum, jetstream::Element *const count) const {
  if(!count->has_i_val()) {
    LOG(FATAL) << "Count not properly formatted when processing tuple for field "<< name;
    return;
  }

  if(sum->has_i_val()) {
    pstmt->setInt(field_index, sum->i_val());
    pstmt->setInt(field_index+1, count->i_val());
    field_index += 2;
    return;
  }

  if(sum->has_d_val()) {
    pstmt->setDouble(field_index, sum->d_val());
    pstmt->setInt(field_index+1,  count->i_val());
    field_index += 2;
    return;
  }

  LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
}



void MysqlAggregateAvg::populate_tuple_final(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
  int sum = resultset->getInt(column_index);
  int count = resultset->getInt(column_index+1);
  column_index += 2;
  jetstream::Element * elem = t->add_e();
  elem->set_i_val(sum/count);
  elem->set_d_val((float)sum/(float)count);
}

void MysqlAggregateAvg::populate_tuple_partial(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
  int sum = resultset->getInt(column_index);
  int count = resultset->getInt(column_index+1);
  column_index += 2;
  jetstream::Element * elem = t->add_e();
  elem->set_i_val(sum);
  elem->set_d_val(sum);
  elem = t->add_e();
  elem->set_i_val(count);
}


string MysqlAggregateAvg::get_select_clause_for_rollup() const {
  return "SUM("+get_base_column_name()+"_sum), SUM("+get_base_column_name()+"_count)";
}

