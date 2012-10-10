#ifndef DIMENSION_FLAT_Q8TYGR7Q
#define DIMENSION_FLAT_Q8TYGR7Q

#include "dimension.h"

namespace jetstream {
namespace cube {
  
class MysqlDimensionFlat: public MysqlDimension{
  public:
    MysqlDimensionFlat() : MysqlDimension(){};

    virtual string get_select_clause_for_rollup(unsigned int const level) const;
    virtual string get_groupby_clause_for_rollup(unsigned int const level) const;

  protected:
    virtual vector<string> get_default_value() const = 0;
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: DIMENSION_FLAT_Q8TYGR7Q */
