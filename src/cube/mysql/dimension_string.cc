#include "dimension_string.h"

using namespace std;
using namespace jetstream::cube;

vector<string> MysqlDimensionString::get_column_types() const {
  vector<string> decl;
  //TODO: 255?
  decl.push_back("VARCHAR(255)");
  return decl;
}


vector<string> MysqlDimensionString::get_default_value() const {
  vector<string> decl;
  decl.push_back("0");
  return decl;
}

void MysqlDimensionString::set_value_for_insert(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &tuple_index, int &field_index) const {
  jetstream::Element * const e = const_cast<jetstream::Tuple &>(t).mutable_e(tuple_index);

  if(e->has_s_val()) {
    pstmt->setString(field_index, e->s_val());
    tuple_index += 1;
    field_index += 1;
    return;
  }

  LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
}

string MysqlDimensionString::get_where_clause(jetstream::Tuple const &t, int &tuple_index, string op, bool is_optional) const {
  jetstream::Element e = t.e(tuple_index);

  if(e.has_s_val()) {
    tuple_index += 1;
    sql::mysql::MySQL_Connection* mysql_conn = dynamic_cast<sql::mysql::MySQL_Connection*>(get_connection().get());
    string str = mysql_conn->escapeString(e.s_val());
    return "`"+get_base_column_name() + "` "+ op +" \""+str+"\"";
  }

  if(!is_optional)
    LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;

  tuple_index += 1;
  return "";
}

void MysqlDimensionString::populate_tuple(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
  jetstream::Element *elem = t->add_e();
  elem->set_s_val(resultset->getString(column_index));
  ++column_index;
}

