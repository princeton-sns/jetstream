#ifndef DIMENSION_DOUBLE_Q8TYGR7Q
#define DIMENSION_DOUBLE_Q8TYGR7Q

#include "dimension.h"

namespace jetstream {
namespace cube {
  
class MysqlDimensionDouble: public MysqlDimension{
  public:
    MysqlDimensionDouble(jetstream::CubeSchema_Dimension _schema) : MysqlDimension(_schema){};

    vector<string> getColumnTypes()
    {
      vector<string> decl;
      decl.push_back("DOUBLE");
      return decl;
    }
    
    void setValueForInsertEntry(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple t, int &tuple_index, int &field_index)
    {
      jetstream::Element e = t.e(tuple_index);
      if(e.has_d_val())
      {
        pstmt->setDouble(field_index, e.d_val());
        tuple_index += 1;
        field_index += 1;
        return;
      }
      LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
    }
      
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: DIMENSION_DOUBLE_Q8TYGR7Q */
