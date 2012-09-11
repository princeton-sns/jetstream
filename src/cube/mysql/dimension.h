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


    virtual void set_value_for_insert(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple t, int &tuple_index, int &field_index) = 0;
    

    //TODO: right now this creates a full sql string. Maybe change to string with placehholders and preparedStatement?
    virtual string get_where_clause_exact(jetstream::Tuple t, int &tuple_index, bool is_optional = true)
    {
      return get_where_clause(t, tuple_index, " = ", is_optional);
    }

    virtual string get_where_clause_greater_than_eq(jetstream::Tuple t, int &tuple_index, bool is_optional = true)
    {
      return get_where_clause(t, tuple_index, " >= ", is_optional);
    }


    virtual string get_where_clause_less_than_eq(jetstream::Tuple t, int &tuple_index, bool is_optional = true)
    {
      return get_where_clause(t, tuple_index, " <= ", is_optional);
    }


    virtual void populate_tuple(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) = 0;

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

    void set_connection(shared_ptr<sql::Connection> con) {
      connection = con;
    }

    shared_ptr<sql::Connection> get_connection() {
      return connection;
    }

  protected:
    virtual string get_where_clause(jetstream::Tuple t, int &tuple_index, string op, bool is_optional = true) = 0;
    shared_ptr<sql::Connection> connection;
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: DIMENSION_Q8TYGR7Q */
