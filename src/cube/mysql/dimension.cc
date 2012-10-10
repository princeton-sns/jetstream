#include "dimension.h"
#include <boost/algorithm/string.hpp>

using namespace std;
using namespace jetstream::cube;



string MysqlDimension::get_where_clause_exact_prepared() const {
  vector<string> sql;
  vector<string> col_names = get_column_names();
  for(vector<string>::iterator col_name = col_names.begin(); col_name != col_names.end(); ++col_name)
  {
    sql.push_back("`"+*col_name+"` = ? ");
  }
  return boost::algorithm::join(sql, " AND ");
}

  string MysqlDimension::get_where_clause_exact(jetstream::Tuple const &t, int &tuple_index, bool is_optional) const {
  return get_where_clause(t, tuple_index, " = ", is_optional);
}

string MysqlDimension::get_where_clause_greater_than_eq(jetstream::Tuple const &t, int &tuple_index, bool is_optional) const {
  return get_where_clause(t, tuple_index, " >= ", is_optional);
}

string  MysqlDimension::get_where_clause_less_than_eq(jetstream::Tuple const &t, int &tuple_index, bool is_optional) const {
  return get_where_clause(t, tuple_index, " <= ", is_optional);
}

string  MysqlDimension::get_base_column_name() const {
  return name;
}

string  MysqlDimension::get_rollup_level_column_name() const {
  return name+"_level";
}

vector<string>  MysqlDimension::get_column_names() const {
  vector<string> decl;
  decl.push_back(get_base_column_name());
  return decl;
}

void  MysqlDimension::set_connection(shared_ptr<sql::Connection> con) {
  connection = con;
}

shared_ptr<sql::Connection>  MysqlDimension::get_connection() const {
  return connection;
}


