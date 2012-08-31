#ifndef DIMENSION_Q8TYGR7Q
#define DIMENSION_Q8TYGR7Q

#include "../dimension.h"
#include <glog/logging.h>

#include <cppconn/prepared_statement.h>

namespace jetstream {
namespace cube {
  
class MysqlDimension: public Dimension{
  public:
    MysqlDimension(jetstream::CubeSchema_Dimension _schema) : Dimension(_schema){};


    virtual void setValueForInsertEntry(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple t, int &tuple_index, int &field_index) = 0;
      

    string getBaseColumnName()
    {
      return name;
    }

    virtual vector<string> getColumnNames()
    {
      vector<string> decl;
      decl.push_back(getBaseColumnName());
      return decl;
    }

    virtual vector<string> getColumnTypes() = 0;
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: DIMENSION_Q8TYGR7Q */
