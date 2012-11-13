#include "dimension_time_hierarchy.h"
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/join.hpp>

using namespace std;
using namespace jetstream::cube;

unsigned int const MysqlDimensionTimeHierarchy::LEVEL_YEAR = 1;
unsigned int const MysqlDimensionTimeHierarchy::LEVEL_MONTH = 2;
unsigned int const MysqlDimensionTimeHierarchy::LEVEL_DAY = 3;
unsigned int const MysqlDimensionTimeHierarchy::LEVEL_HOUR = 4;
unsigned int const MysqlDimensionTimeHierarchy::LEVEL_MINUTE = 5;
unsigned int const MysqlDimensionTimeHierarchy::LEVEL_SECOND = 6;

jetstream::DataCube::DimensionKey MysqlDimensionTimeHierarchy::get_key(Tuple const &t) const
{
  assert(tuple_indexes.size() == 1);
  jetstream::Element * const e = const_cast<jetstream::Tuple &>(t).mutable_e(tuple_indexes[0]);

  if(e->has_t_val()) {
    struct tm temptm;
    char timestring[30];
    time_t clock = e->t_val();
    localtime_r(&clock, &temptm);
    strftime(timestring, sizeof(timestring)-1, "%Y-%m-%d %H:%M:%S", &temptm);
    return timestring;
  }

  LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
}


vector<string> MysqlDimensionTimeHierarchy::get_column_names() const {
  vector<string> decl;
  decl.push_back(get_base_column_name()+"_year");
  decl.push_back(get_base_column_name()+"_month");
  decl.push_back(get_base_column_name()+"_day");
  decl.push_back(get_base_column_name()+"_hour");
  decl.push_back(get_base_column_name()+"_minute");
  decl.push_back(get_base_column_name()+"_second");
  return decl;
}


vector<string> MysqlDimensionTimeHierarchy::get_column_types() const {
  vector<string> decl;
  decl.push_back("SMALLINT"); //Y
  decl.push_back("TINYINT"); //M
  decl.push_back("TINYINT"); //D
  decl.push_back("TINYINT"); //H
  decl.push_back("TINYINT"); //M
  decl.push_back("TINYINT");  //S
  return decl;
}

void MysqlDimensionTimeHierarchy::set_value_for_insert_tuple(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &field_index) const {
  if(tuple_indexes.size() != 1)
    LOG(FATAL) << "Wrong number of tuple indexes for field "<< name;

  jetstream::Element * const e = const_cast<jetstream::Tuple &>(t).mutable_e(tuple_indexes[0]);

  if(e->has_t_val()) {
    struct tm temptm;
    time_t clock = e->t_val();
    localtime_r(&clock, &temptm);
    pstmt->setInt(field_index, temptm.tm_year);
    pstmt->setInt(field_index+1, temptm.tm_mon);
    pstmt->setInt(field_index+2, temptm.tm_mday);
    pstmt->setInt(field_index+3, temptm.tm_hour);
    pstmt->setInt(field_index+4, temptm.tm_min);
    pstmt->setInt(field_index+5, temptm.tm_sec);
    field_index += 6;
    return;
  }

  LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
}


string MysqlDimensionTimeHierarchy::get_where_clause(jetstream::Tuple const &t, int &tuple_index, string op, bool is_optional) const {
  jetstream::Element e = t.e(tuple_index);

  if(e.has_t_val()) {
    struct tm temptm;
    time_t clock = e.t_val();
    localtime_r(&clock, &temptm);
    string sql = "`"+get_base_column_name() + "_year` "+ op +" "+boost::lexical_cast<string>(temptm.tm_year)+" AND ";
    sql += "`"+get_base_column_name() + "_month` "+ op +" "+boost::lexical_cast<string>(temptm.tm_mon)+" AND ";
    sql += "`"+get_base_column_name() + "_day` "+ op +" "+boost::lexical_cast<string>(temptm.tm_mday)+" AND ";
    sql += "`"+get_base_column_name() + "_hour` "+ op +" "+boost::lexical_cast<string>(temptm.tm_hour)+" AND ";
    sql += "`"+get_base_column_name() + "_minute` "+ op +" "+boost::lexical_cast<string>(temptm.tm_min)+" AND ";
    sql += "`"+get_base_column_name() + "_second` "+ op +" "+boost::lexical_cast<string>(temptm.tm_sec);
    tuple_index += 1;
    return sql;
  }

  if(!is_optional)
    LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;

  tuple_index += 1;
  return "";
}

void MysqlDimensionTimeHierarchy::populate_tuple(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
  jetstream::Element *elem = t->add_e();
  struct tm temptm;
  temptm.tm_year = resultset->getInt(column_index);
  temptm.tm_mon = resultset->getInt(column_index+1);
  temptm.tm_mday = resultset->getInt(column_index+2);
  temptm.tm_hour = resultset->getInt(column_index+3);
  temptm.tm_min = resultset->getInt(column_index+4);
  temptm.tm_sec = resultset->getInt(column_index+5);
  temptm.tm_isdst = -1; //Make mktime figure it out

  time_t time = mktime(&temptm);
  if (time > 0) {
    elem->set_t_val(time);
  }
  else {
    LOG(FATAL)<<"Error in time conversion";
  }

  column_index+=6;
}

string MysqlDimensionTimeHierarchy::get_select_clause_for_rollup(unsigned int const level) const {

  vector<string>cols = get_column_names();
  vector<string>vals;
  
  for(unsigned int i=0; i<6;++i)
  {
    if(i<level)
    {
      vals.push_back(cols[i]);
    }
    else
    {
      vals.push_back("0");
    }
  }
  string sel = boost::algorithm::join(vals, ", ");
  return sel+", "+boost::lexical_cast<std::string>((level > MysqlDimensionTimeHierarchy::LEVEL_SECOND? MysqlDimensionTimeHierarchy::LEVEL_SECOND:level));

}
    
string MysqlDimensionTimeHierarchy::get_groupby_clause_for_rollup(unsigned int const level) const {
  vector<string>cols = get_column_names();
  vector<string>vals;
  
  for(unsigned int i=0; i<level;++i)
  {
    vals.push_back(cols[i]);
  }
  
  return boost::algorithm::join(vals, ", ");
}
