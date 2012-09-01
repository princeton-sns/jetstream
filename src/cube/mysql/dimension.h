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


    virtual void set_value_for_insert_entry(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple t, int &tuple_index, int &field_index) = 0;
      

    string get_base_column_name()
    {
      return name;
    }

    virtual vector<string> get_column_names()
    {
      vector<string> decl;
      decl.push_back(get_base_column_name());
      return decl;
    }

    virtual vector<string> get_column_types() = 0;
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: DIMENSION_Q8TYGR7Q */
