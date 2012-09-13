#ifndef DIMENSION_STRING_Q8TYGR7Q
#define DIMENSION_STRING_Q8TYGR7Q

#include "dimension.h"
#include "mysql_connection.h"

namespace jetstream {
namespace cube {
  
class MysqlDimensionString: public MysqlDimension{
  public:
    MysqlDimensionString(jetstream::CubeSchema_Dimension _schema) : MysqlDimension(_schema){};

    vector<string> get_column_types() const
    {
      vector<string> decl;
      //TODO: 255?
      decl.push_back("VARCHAR(255)");
      return decl;
    }
     
    void set_value_for_insert(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &tuple_index, int &field_index) const
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

    string get_where_clause(jetstream::Tuple const &t, int &tuple_index, string op, bool is_optional=true) const {
      jetstream::Element e = t.e(tuple_index);
      if(e.has_s_val())
      {
        tuple_index += 1;
        sql::mysql::MySQL_Connection* mysql_conn = dynamic_cast<sql::mysql::MySQL_Connection*>(get_connection().get());
        string str = mysql_conn->escapeString(e.s_val());
        return "`"+get_base_column_name() + "` "+ op +" \""+str+"\"";
      }
      if(!is_optional)
        LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
      
      tuple_index += 1;
      return "";
    }

    virtual void populate_tuple(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const
    {
      jetstream::Element *elem = t->add_e();
      elem->set_s_val(resultset->getString(column_index));
      ++column_index;
    }
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: DIMENSION_STRING_Q8TYGR7Q */
