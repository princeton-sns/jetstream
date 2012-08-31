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

    vector<string> getColumnNames()
    {
      //this should be the leaf. No need for agg_level column
      //that goes in rollup table. to be done later;
      vector<string> decl;
      decl.push_back(getBaseColumnName()+"");
      //decl.push_back(getBaseColumnName()+"_agg_level");
      return decl;
    }

    vector<string> getColumnTypes()
    {
      vector<string> decl;
      decl.push_back("DATETIME");
      //decl.push_back("INT");
      return decl;
    }
    
    void setValueForInsertEntry(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple t, int &tuple_index, int &field_index)
    {
      jetstream::Element e = t.e(tuple_index);
      if(e.has_t_val())
      {
        struct tm temptm;
        char timestring[20];
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
      
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: DIMENSION_TIME_Q8TYGR7Q */
