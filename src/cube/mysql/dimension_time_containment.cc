#include "dimension_time_containment.h"
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/join.hpp>

using namespace std;
using namespace jetstream::cube;

unsigned int const  MysqlDimensionTimeContainment::SECS_PER_LEVEL[] = {INT_MAX, 3600, 1800, 600, 300, 60, 30, 10, 5, 1};
unsigned int const  MysqlDimensionTimeContainment::MAX_LEVEL = sizeof(MysqlDimensionTimeContainment::SECS_PER_LEVEL)/sizeof(unsigned int);


jetstream::DataCube::DimensionKey MysqlDimensionTimeContainment::get_key(Tuple const &t) const {
  assert(tuple_indexes.size() == 1);
  jetstream::Element * const e = const_cast<jetstream::Tuple &>(t).mutable_e(tuple_indexes[0]);

  if(e->has_t_val()) {
    return boost::lexical_cast<string>(e->t_val());
  }

  LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
}

void
MysqlDimensionTimeContainment::get_key(Tuple const &t, std::ostringstream &ostr) const {
  const jetstream::Element& e = t.e(tuple_indexes[0]);
  if(e.has_t_val()) {
    ostr << e.t_val();
    return;
  }
  LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
}


vector<string>
MysqlDimensionTimeContainment::get_column_names() const {
  vector<string> decl;
  decl.push_back(get_base_column_name()+"_start");
  return decl;
}


vector<string>
MysqlDimensionTimeContainment::get_column_types() const {
  vector<string> decl;
  decl.push_back("INT");
  return decl;
}

void
MysqlDimensionTimeContainment::set_value_for_insert_tuple(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &field_index) const {
  if(tuple_indexes.size() != 1)
    LOG(FATAL) << "Wrong number of tuple indexes for field "<< name;

  jetstream::Element * const e = const_cast<jetstream::Tuple &>(t).mutable_e(tuple_indexes[0]);

  if(e->has_t_val()) {
    pstmt->setInt(field_index, e->t_val());
    field_index += 1;
    return;
  }

  LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
}


string
MysqlDimensionTimeContainment::get_where_clause(jetstream::Tuple const &t, int &tuple_index, string op, bool is_optional) const {
  jetstream::Element e = t.e(tuple_index);

  if(e.has_t_val()) {
    tuple_index += 1;
    return "`"+get_base_column_name() + "` "+ op +" "+boost::lexical_cast<std::string>(e.t_val());
  }

  if(!is_optional)
    LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;

  tuple_index += 1;
  return "";
}

void
MysqlDimensionTimeContainment::populate_tuple(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
  jetstream::Element *elem = t->add_e();
  elem->set_t_val(resultset->getInt(column_index));
  ++column_index;
}

string
MysqlDimensionTimeContainment::get_select_clause_for_rollup(unsigned int const level) const {
  vector<string>cols = get_column_names();
  string start_time_col_name = cols[0];
  unsigned int secs_in_level = MysqlDimensionTimeContainment::SECS_PER_LEVEL[level];
  string secs_in_level_str =  boost::lexical_cast<std::string>(secs_in_level);
  string start_time_for_group_sql = "( (`"+start_time_col_name+"`/"+secs_in_level_str+") * "+secs_in_level_str+")";
  string level_str = boost::lexical_cast<std::string>((level > MysqlDimensionTimeContainment::MAX_LEVEL ? MysqlDimensionTimeContainment::MAX_LEVEL: level));
  return start_time_for_group_sql+", "+level_str;
}

string MysqlDimensionTimeContainment::get_groupby_clause_for_rollup(unsigned int const level) const {
  vector<string>cols = get_column_names();
  string start_time_col_name = cols[0];
  unsigned int secs_in_level = MysqlDimensionTimeContainment::SECS_PER_LEVEL[level];
  string secs_in_level_str =  boost::lexical_cast<std::string>(secs_in_level);
  string start_time_for_group_sql = "( (`"+start_time_col_name+"`/"+secs_in_level_str+") * "+secs_in_level_str+")";
  return start_time_for_group_sql;
}
