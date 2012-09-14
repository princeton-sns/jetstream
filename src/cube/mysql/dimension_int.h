#ifndef DIMENSION_INT_Q8TYGR7Q
#define DIMENSION_INT_Q8TYGR7Q

#include "dimension.h"
#include <boost/lexical_cast.hpp>

namespace jetstream {
namespace cube {
  
class MysqlDimensionInt: public MysqlDimension{
  public:
    MysqlDimensionInt(jetstream::CubeSchema_Dimension _schema) : MysqlDimension(_schema){};

    vector<string> get_column_types() const
    {
      vector<string> decl;
      decl.push_back("INT");
      return decl;
    }
     
    void set_value_for_insert(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &tuple_index, int &field_index) const
    {
      jetstream::Element * const e = const_cast<jetstream::Tuple &>(t).mutable_e(tuple_index);
      if(e->has_i_val())
      {
        pstmt->setInt(field_index, e->i_val());
        tuple_index += 1;
        field_index += 1;
        return;
      }
      LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
    }

    string get_where_clause(jetstream::Tuple const &t, int &tuple_index, string op, bool is_optional=true) const {
      jetstream::Element e = t.e(tuple_index);
      if(e.has_i_val())
      {
        tuple_index += 1;
        return "`"+get_base_column_name() + "` "+ op +" "+boost::lexical_cast<std::string>(e.i_val());
      }
      if(!is_optional)
        LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
      
      tuple_index += 1;
      return "";
    }

    virtual void populate_tuple(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const
    {
      jetstream::Element *elem = t->add_e();
      elem->set_i_val(resultset->getInt(column_index));
      ++column_index;
    }
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: DIMENSION_INT_Q8TYGR7Q */
