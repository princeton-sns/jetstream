#ifndef DIMENSION_TIME_Q8TYGR7Q
#define DIMENSION_TIME_Q8TYGR7Q

#include "dimension.h"
#include <time.h>
#include <stdlib.h>

namespace jetstream {
namespace cube {

class MysqlDimensionTime : public MysqlDimension {
  public:
    MysqlDimensionTime(jetstream::CubeSchema_Dimension _schema) : MysqlDimension(_schema) {};

    vector<string> get_column_names() const ;

    vector<string> get_column_types() const ;

    void set_value_for_insert(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const&t, int &tuple_index, int &field_index) const ;

    string get_where_clause(jetstream::Tuple const &t, int &tuple_index, string op, bool is_optional=true) const ;

    virtual void populate_tuple(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const ;

};


} /* cube */
} /* jetstream */


#endif /* end of include guard: DIMENSION_TIME_Q8TYGR7Q */
