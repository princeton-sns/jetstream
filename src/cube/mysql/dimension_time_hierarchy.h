#ifndef DIMENSION_TIME_Q8TYGR7Q
#define DIMENSION_TIME_Q8TYGR7Q

#include "dimension_flat.h"
#include <time.h>
#include <stdlib.h>

namespace jetstream {
namespace cube {

class MysqlDimensionTimeHierarchy : public MysqlDimension {
  public:
    static unsigned int const LEVEL_YEAR;
    static unsigned int const LEVEL_MONTH;
    static unsigned int const LEVEL_DAY;
    static unsigned int const LEVEL_HOUR;
    static unsigned int const LEVEL_MINUTE;
    static unsigned int const LEVEL_SECOND;


    MysqlDimensionTimeHierarchy(jetstream::CubeSchema_Dimension _schema) : MysqlDimension(_schema) {};
    
    virtual jetstream::DataCube::DimensionKey get_key(Tuple const &t) const;

    virtual string get_select_clause_for_rollup(unsigned int const level) const;
    virtual string get_groupby_clause_for_rollup(unsigned int const level) const;
    
    vector<string> get_column_names() const ;

    vector<string> get_column_types() const ;
    
    virtual void set_value_for_insert_tuple(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &field_index) const;

    void set_value_for_insert(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const&t, int &tuple_index, int &field_index) const ;

    string get_where_clause(jetstream::Tuple const &t, int &tuple_index, string op, bool is_optional=true) const ;

    virtual void populate_tuple(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const ;
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: DIMENSION_TIME_Q8TYGR7Q */
