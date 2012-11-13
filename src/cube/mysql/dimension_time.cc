#include "dimension_time.h"

using namespace std;
using namespace jetstream::cube;


jetstream::DataCube::DimensionKey MysqlDimensionTime::get_key(const  Tuple &t) const
{
  assert(tuple_indexes.size() == 1);
  const jetstream::Element &  e = t.e(tuple_indexes[0]);

  if(e.has_t_val()) {
    struct tm temptm;
    char timestring[30];
    time_t clock = e.t_val();
    localtime_r(&clock, &temptm);
    strftime(timestring, sizeof(timestring)-1, "%Y-%m-%d %H:%M:%S", &temptm);
    return timestring;
  }

  LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
}

vector<string> MysqlDimensionTime::get_column_names() const {
  //this should be the leaf. No need for agg_level column
  //that goes in rollup table. to be done later;
  vector<string> decl;
  decl.push_back(get_base_column_name()+"");
  //decl.push_back(get_base_column_name()+"_agg_level");
  return decl;
}

vector<string> MysqlDimensionTime::get_default_value() const {
  vector<string> decl;
  decl.push_back("\"0000-00-00 00:00:00\"");
  return decl;
}

vector<string> MysqlDimensionTime::get_column_types() const {
  vector<string> decl;
  decl.push_back("DATETIME");
  //decl.push_back("INT");
  return decl;
}

void MysqlDimensionTime::set_value_for_insert_tuple(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &field_index) const {
  if(tuple_indexes.size() != 1)
    LOG(FATAL) << "Wrong number of tuple indexes for field "<< name;

  jetstream::Element * const e = const_cast<jetstream::Tuple &>(t).mutable_e(tuple_indexes[0]);
  if(e->has_t_val()) {
    struct tm temptm;
    char timestring[30];
    time_t clock = e->t_val();
    localtime_r(&clock, &temptm);
    strftime(timestring, sizeof(timestring)-1, "%Y-%m-%d %H:%M:%S", &temptm);
    pstmt->setString(field_index, timestring);
    field_index += 1;
    return;
  }

  LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
}

string MysqlDimensionTime::get_where_clause(jetstream::Tuple const &t, int &tuple_index, string op, bool is_optional) const {
  jetstream::Element e = t.e(tuple_index);

  if(e.has_t_val()) {
    struct tm temptm;
    char timestring[30];
    time_t clock = e.t_val();
    localtime_r(&clock, &temptm);
    strftime(timestring, sizeof(timestring)-1, "%Y-%m-%d %H:%M:%S", &temptm);
    tuple_index += 1;
    return "`"+get_base_column_name() + "` "+ op +" \""+timestring+"\"";
  }

  if(!is_optional)
    LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;

  tuple_index += 1;
  return "";
}

void MysqlDimensionTime::populate_tuple(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
  jetstream::Element *elem = t->add_e();
  string timestring = resultset->getString(column_index);
  struct tm temptm;
  temptm.tm_isdst = -1; //not filled in by strptime. Make mktime figure it out
  
  if(strptime(timestring.c_str(), "%Y-%m-%d %H:%M:%S", &temptm) != NULL) {
    elem->set_t_val(mktime(&temptm));
  }
  else {
    LOG(FATAL)<<"Error in time conversion";
  }

  ++column_index;
}
