#ifndef DIMENSION_INT_Q8TYGR7Q
#define DIMENSION_INT_Q8TYGR7Q

#include "dimension.h"

namespace jetstream {
namespace cube {
  
class MysqlDimensionInt: public MysqlDimension{
  public:
    MysqlDimensionInt(jetstream::CubeSchema_Dimension _schema) : MysqlDimension(_schema){};

    vector<string> getColumnTypes()
    {
      vector<string> decl;
      decl.push_back("INT");
      return decl;
    }
     
    void setValueForInsertEntry(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple t, int &tuple_index, int &field_index)
    {
      jetstream::Element e = t.e(tuple_index);
      if(e.has_i_val())
      {
        pstmt->setInt(field_index, e.i_val());
        tuple_index += 1;
        field_index += 1;
        return;
      }
      LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
    } 
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: DIMENSION_INT_Q8TYGR7Q */
