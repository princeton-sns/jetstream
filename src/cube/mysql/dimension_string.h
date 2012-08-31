#ifndef DIMENSION_STRING_Q8TYGR7Q
#define DIMENSION_STRING_Q8TYGR7Q

#include "dimension.h"

namespace jetstream {
namespace cube {
  
class MysqlDimensionString: public MysqlDimension{
  public:
    MysqlDimensionString(jetstream::CubeSchema_Dimension _schema) : MysqlDimension(_schema){};

    vector<string> getColumnTypes()
    {
      vector<string> decl;
      //TODO: 255?
      decl.push_back("VARCHAR(255)");
      return decl;
    }
     
    void setValueForInsertEntry(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple t, int &tuple_index, int &field_index)
    {
      jetstream::Element e = t.e(tuple_index);
      if(e.has_s_val())
      {
        pstmt->setString(field_index, e.s_val());
        tuple_index += 1;
        field_index += 1;
        return;
      }
      LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
    }
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: DIMENSION_STRING_Q8TYGR7Q */
