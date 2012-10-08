#include "dimension_int.h"

using namespace std;
using namespace jetstream::cube;


jetstream::DataCube::DimensionKey MysqlDimensionInt::get_key(Tuple const &t) const
{
  assert(tuple_indexes.size() == 1);
  jetstream::Element * const e = const_cast<jetstream::Tuple &>(t).mutable_e(tuple_indexes[0]);

  if(e->has_d_val()) {
    return boost::lexical_cast<string>(e->i_val());
  }

  LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
}

vector<string>  MysqlDimensionInt::get_column_types() const {
  vector<string> decl;
  decl.push_back("INT");
  return decl;
}

vector<string> MysqlDimensionInt::get_default_value() const {
  vector<string> decl;
  decl.push_back("0");
  return decl;
}

void MysqlDimensionInt::set_value_for_insert_tuple(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &field_index) const {
  if(tuple_indexes.size() != 1)
    LOG(FATAL) << "Wrong number of tuple indexes for field "<< name;

  jetstream::Element * const e = const_cast<jetstream::Tuple &>(t).mutable_e(tuple_indexes[0]);

  if(e->has_i_val()) {
    pstmt->setInt(field_index, e->i_val());
    field_index += 1;
    return;
  }

  LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
}

void MysqlDimensionInt::set_value_for_insert(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &tuple_index, int &field_index) const {
  jetstream::Element * const e = const_cast<jetstream::Tuple &>(t).mutable_e(tuple_index);

  if(e->has_i_val()) {
    pstmt->setInt(field_index, e->i_val());
    tuple_index += 1;
    field_index += 1;
    return;
  }

  LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
}

string MysqlDimensionInt::get_where_clause(jetstream::Tuple const &t, int &tuple_index, string op, bool is_optional) const {
  jetstream::Element e = t.e(tuple_index);

  if(e.has_i_val()) {
    tuple_index += 1;
    return "`"+get_base_column_name() + "` "+ op +" "+boost::lexical_cast<std::string>(e.i_val());
  }

  if(!is_optional)
    LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;

  tuple_index += 1;
  return "";
}

void MysqlDimensionInt::populate_tuple(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
  jetstream::Element *elem = t->add_e();
  elem->set_i_val(resultset->getInt(column_index));
  ++column_index;
}

