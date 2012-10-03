#ifndef DIMENSION_STRING_Q8TYGR7Q
#define DIMENSION_STRING_Q8TYGR7Q

#include "dimension_flat.h"
#include "mysql_connection.h"

namespace jetstream {
namespace cube {

class MysqlDimensionString: public MysqlDimensionFlat {
  public:
    MysqlDimensionString(jetstream::CubeSchema_Dimension _schema) : MysqlDimensionFlat(_schema) {};

    vector<string> get_column_types() const ;

    void set_value_for_insert(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &tuple_index, int &field_index) const ;

    string get_where_clause(jetstream::Tuple const &t, int &tuple_index, string op, bool is_optional=true) const ;

    virtual void populate_tuple(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const ;

  protected:
    virtual vector<string> get_default_value() const;
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: DIMENSION_STRING_Q8TYGR7Q */
