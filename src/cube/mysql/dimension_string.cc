
#include "mysql_connection.h"

#include "mysql_cube.h"
#include "dimension_string.h"


using namespace std;
using namespace jetstream::cube;

jetstream::DataCube::DimensionKey MysqlDimensionString::get_key(const Tuple &t) const
{
  assert(tuple_indexes.size() ==  1);
  if ( (int) tuple_indexes[0] >= t.e_size())
    LOG(FATAL) << "no element " << tuple_indexes[0] << " of " << fmt(t)<< " (len == " << t.e_size() << ")";

  const jetstream::Element& e = t.e(tuple_indexes[0]);

  if(e.has_s_val()) {
    if(e.s_val().size() > 254) {
      return e.s_val().substr(0, 254);
    }
    else {
      return e.s_val();
    }
  }
  else
    LOG(FATAL) << "Expected a string element for field "<< name << " in tuple "<< fmt(t);
  return "";
}

void MysqlDimensionString::get_key(Tuple const &t, std::ostringstream &ostr) const
{
  const jetstream::Element& e = t.e(tuple_indexes[0]);
  if(e.has_s_val()) {
    if(e.s_val().size() > 254) {
      ostr << e.s_val().substr(0, 254);
    }
    else {
      ostr << e.s_val();
    }
    return;
  }
  LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
}


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

void MysqlDimensionString::set_value_for_insert_tuple(boost::shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &field_index) const {
  if(tuple_indexes.size() != 1)
    LOG(FATAL) << "Wrong number of tuple indexes for field "<< name;

  jetstream::Element * const e = const_cast<jetstream::Tuple &>(t).mutable_e(tuple_indexes[0]);

  if(e->has_s_val()) {
    if(e->s_val().size() > 254) {
      LOG_FIRST_N(ERROR, 10) << "String given to cube too long. Truncating. (Only first 10 reported)";
      pstmt->setString(field_index, e->s_val().substr(0, 254));
    }
    else {
      pstmt->setString(field_index, e->s_val());
    }
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
    std::string str = mysql_conn->escapeString(e.s_val());

//    boost::shared_ptr<sql::Connection> sqlconn = cube::MysqlCube::get_s_connection();
 //   sql::mysql::MySQL_Connection* mysql_conn = dynamic_cast<sql::mysql::MySQL_Connection*>(sqlconn.get());
//    sql::mysql::MySQL_Connection conn;
//    string str = conn.escapeString(e.s_val());
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

