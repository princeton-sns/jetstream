#ifndef DIMENSION_DOUBLE_Q8TYGR7Q
#define DIMENSION_DOUBLE_Q8TYGR7Q

#include "dimension.h"
#include <boost/lexical_cast.hpp>

namespace jetstream {
namespace cube {
  
class MysqlDimensionDouble: public MysqlDimension{
  public:
    MysqlDimensionDouble(jetstream::CubeSchema_Dimension _schema) : MysqlDimension(_schema){};

    vector<string> get_column_types()
    {
      vector<string> decl;
      decl.push_back("DOUBLE");
      return decl;
    }
    
    void set_value_for_insert(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple t, int &tuple_index, int &field_index)
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

    
    string get_where_clause(jetstream::Tuple t, int &tuple_index, string op, bool is_optional=true) {
      jetstream::Element e = t.e(tuple_index);
      if(e.has_d_val())
      {
        tuple_index += 1;
        return "`"+get_base_column_name() + "` "+ op +" "+boost::lexical_cast<std::string>(e.d_val());
      }
      if(!is_optional)
        LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
      return "";
    }
    
    virtual void populate_tuple(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index)
    {
      jetstream::Element *elem = t->add_e();
      elem->set_d_val(resultset->getDouble(column_index));
      ++column_index;
    }
      
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: DIMENSION_DOUBLE_Q8TYGR7Q */
