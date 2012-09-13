#ifndef DIMENSION_TIME_Q8TYGR7Q
#define DIMENSION_TIME_Q8TYGR7Q

#include "dimension.h"
#include <time.h>
#include <stdlib.h>

namespace jetstream {
namespace cube {
  
class MysqlDimensionTime: public MysqlDimension{
  public:
    MysqlDimensionTime(jetstream::CubeSchema_Dimension _schema) : MysqlDimension(_schema){};

    vector<string> get_column_names()
    {
      //this should be the leaf. No need for agg_level column
      //that goes in rollup table. to be done later;
      vector<string> decl;
      decl.push_back(get_base_column_name()+"");
      //decl.push_back(get_base_column_name()+"_agg_level");
      return decl;
    }

    vector<string> get_column_types()
    {
      vector<string> decl;
      decl.push_back("DATETIME");
      //decl.push_back("INT");
      return decl;
    }
    
    void set_value_for_insert(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &tuple_index, int &field_index)
    {
      jetstream::Element e = t.e(tuple_index);
      if(e.has_t_val())
      {
        struct tm temptm;
        char timestring[30];
        time_t clock = e.t_val();
        localtime_r(&clock, &temptm);
        strftime(timestring, sizeof(timestring)-1, "%Y-%m-%d %H:%M:%S", &temptm);
        pstmt->setString(field_index, timestring);
        tuple_index += 1;
        field_index += 1;
        return;
      }
      LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
    }

    string get_where_clause(jetstream::Tuple const &t, int &tuple_index, string op, bool is_optional=true) {
      jetstream::Element e = t.e(tuple_index);
      if(e.has_t_val())
      {
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
    
    virtual void populate_tuple(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index)
    {
      jetstream::Element *elem = t->add_e();
      string timestring = resultset->getString(column_index);
      struct tm temptm;
      if(strptime(timestring.c_str(), "%Y-%m-%d %H:%M:%S", &temptm) != NULL)
      {
        elem->set_t_val(mktime(&temptm));
      }
      else
      {
        LOG(FATAL)<<"Error in time conversion";
      }
      ++column_index;
    }
      
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: DIMENSION_TIME_Q8TYGR7Q */
