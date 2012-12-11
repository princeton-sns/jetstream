#ifndef AGGREGATE_MIN_Q8TYGR7Q
#define AGGREGATE_MIN_Q8TYGR7Q

#include "aggregate.h"
#include "aggregate_simple.h"
#include <glog/logging.h>

namespace jetstream {
namespace cube {
template<class ValueType>
class MysqlAggregateMin: public MysqlAggregateSimple<ValueType> {
    virtual ValueType combine(ValueType a, ValueType b) const {
      if (a < b) 
        return a;
      return b;
    }

    virtual string get_update_on_insert_sql() const{
        return "`"+MysqlAggregateSimple<ValueType>::get_base_column_name()+"` = LEAST(`"+MysqlAggregateSimple<ValueType>::get_base_column_name()+"`, VALUES(`"+MysqlAggregateSimple<ValueType>::get_base_column_name()+"`))";
    }
    
    virtual string get_select_clause_for_rollup() const {
    return "MIN("+MysqlAggregateSimple<ValueType>::get_base_column_name()+")";
    }
};
} /* cube */
} /* jetstream */


#endif /* end of include guard: AGGREGATE_MIN_Q8TYGR7Q */
