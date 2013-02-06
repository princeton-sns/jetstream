#include "dimension_time_containment.h"
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/join.hpp>

using namespace std;
using namespace jetstream::cube;


jetstream::DataCube::DimensionKey MysqlDimensionTimeContainment::get_key(Tuple const &t) const {
  assert(tuple_indexes.size() == 1);
  jetstream::Element * const e = const_cast<jetstream::Tuple &>(t).mutable_e(tuple_indexes[0]);

  if(e->has_t_val()) {
    struct tm temptm;
    char timestring[30];
    time_t clock = e->t_val();
    gmtime_r(&clock, &temptm);
    strftime(timestring, sizeof(timestring)-1, "%Y-%m-%d %H:%M:%S", &temptm);
    return timestring;
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
  decl.push_back(get_base_column_name()+"_duration");
  return decl;
}


vector<string>
MysqlDimensionTimeContainment::get_column_types() const {
  vector<string> decl;
  decl.push_back("DATETIME");
  decl.push_back("SMALLINT"); //secs in bucket
  return decl;
}

void
MysqlDimensionTimeContainment::set_value_for_insert_tuple(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &field_index) const {
  if(tuple_indexes.size() != 1)
    LOG(FATAL) << "Wrong number of tuple indexes for field "<< name;

  jetstream::Element * const e = const_cast<jetstream::Tuple &>(t).mutable_e(tuple_indexes[0]);

  if(e->has_t_val()) {
    struct tm temptm;
    time_t clock = e->t_val();
    char timestring[30];
    gmtime_r(&clock, &temptm); 
    strftime(timestring, sizeof(timestring)-1, "%Y-%m-%d %H:%M:%S", &temptm);
    
    pstmt->setString(field_index, timestring);
    pstmt->setInt(field_index+1, temptm.tm_year+1900);
    pstmt->setInt(field_index+2, temptm.tm_mon+1);
    pstmt->setInt(field_index+3, temptm.tm_mday);
    pstmt->setInt(field_index+4, temptm.tm_hour);
    pstmt->setInt(field_index+5, temptm.tm_min);
    pstmt->setInt(field_index+6, temptm.tm_sec);
    field_index += 7;
    return;
  }

  LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
}


string
MysqlDimensionTimeContainment::get_where_clause(jetstream::Tuple const &t, int &tuple_index, string op, bool is_optional) const {
  jetstream::Element e = t.e(tuple_index);

  if(e.has_t_val()) {
    struct tm temptm;
    char timestring[30];
    time_t clock = e.t_val();
    gmtime_r(&clock, &temptm);
    strftime(timestring, sizeof(timestring)-1, "%Y-%m-%d %H:%M:%S", &temptm);
    tuple_index += 1;
    return "`"+get_base_column_name() + "_start` "+ op +" \""+timestring+"\"";
  }

  if(!is_optional)
    LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;

  tuple_index += 1;
  return "";
}

void
MysqlDimensionTimeContainment::populate_tuple(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
  jetstream::Element *elem = t->add_e();
  
  string timestring = resultset->getString(column_index);
  struct tm temptm;
  //temptm.tm_isdst = -1; //not filled in by strptime. Make mktime figure it out
  
  if(strptime(timestring.c_str(), "%Y-%m-%d %H:%M:%S", &temptm) != NULL) {
    elem->set_t_val(timegm(&temptm));
  }
  else {
    LOG(FATAL)<<"Error in time conversion";
  }

  column_index += 1;
  return;
}

string 
MysqlDimensionTimeContainment::get_select_clause_for_rollup(unsigned int const level) const {
  vector<string>cols = get_column_names();
  return cols[0] +","+ cols[1];
/*
  string sel = boost::algorithm::join(vals, ", ");
  string time_sel = "CONCAT("+time_vals[0]+", \"-\", "+time_vals[1]+", \"-\", "+time_vals[2]+", \" \", "+time_vals[3]+", \":\", "+time_vals[4]+", \":\", "+time_vals[5]+")";
  return time_sel+","+sel+", "+boost::lexical_cast<std::string>((level > MysqlDimensionTimeHierarchy::LEVEL_SECOND? MysqlDimensionTimeHierarchy::LEVEL_SECOND:level));*/

}
    
string MysqlDimensionTimeContainment::get_groupby_clause_for_rollup(unsigned int const level) const {
  vector<string>cols = get_column_names();
  vector<string>vals;
  
  //TODO HERE
  
  return boost::algorithm::join(vals, ", ");
}
