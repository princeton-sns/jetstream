#ifndef DIMENSION_TIME_CONT_Q8TYGR7Q
#define DIMENSION_TIME_CONT_Q8TYGR7Q

#include "dimension_flat.h"
#include <time.h>
#include <stdlib.h>
#include <limits.h>

namespace jetstream {
namespace cube {

const unsigned int  DTC_SECS_PER_LEVEL[] = {INT_MAX, 3600, 1800, 600, 300, 60, 30, 10, 5, 1};
const unsigned int DTC_LEVEL_COUNT = sizeof(DTC_SECS_PER_LEVEL)/sizeof(unsigned int);


class MysqlDimensionTimeContainment : public MysqlDimension {
  public:

    MysqlDimensionTimeContainment() : MysqlDimension() {};

    static const unsigned int LEVEL_SECOND;
    static const unsigned int LEVEL_MINUTE;
    static const unsigned int LEVEL_HOUR;

    virtual unsigned leaf_level() const{
      return DTC_LEVEL_COUNT-1;
    }

    virtual jetstream::DataCube::DimensionKey get_key(Tuple const &t) const;
    virtual void get_key(Tuple const &t, std::ostringstream &ostr) const;

    virtual string get_select_clause_for_rollup(unsigned int const level) const;
    virtual string get_groupby_clause_for_rollup(unsigned int const level) const;

    vector<string> get_column_names() const ;

    vector<string> get_column_types() const ;

    virtual void set_value_for_insert_tuple(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &field_index) const;

    string get_where_clause(jetstream::Tuple const &t, int &tuple_index, string op, bool is_optional=true) const ;

    virtual void populate_tuple(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const ;

};

} /* cube */
} /* jetstream */


#endif /* end of include guard: DIMENSION_CONT_Q8TYGR7Q */
