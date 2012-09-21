#ifndef AGGREGATE_FACTORY_M77OYEG0
#define AGGREGATE_FACTORY_M77OYEG0
#include "mysql/aggregate.h"
#include "mysql/aggregate_count.h"
#include "mysql/aggregate_avg.h"
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <glog/logging.h>

namespace jetstream {
namespace cube {

template <class I>
struct AggregateFactory {

};

template<>
struct AggregateFactory<jetstream::cube::MysqlAggregate> {
  static boost::shared_ptr<jetstream::cube::MysqlAggregate> create(jetstream::CubeSchema_Aggregate _schema) {
    if(_schema.type() == "count") {
      return boost::make_shared<MysqlAggregateCount>(_schema);
    }
    else if(_schema.type() == "avg") {
      return boost::make_shared<MysqlAggregateAvg>(_schema);
    }
    else {
      LOG(FATAL) << "Don't have right aggregate";
    }
  };
};

} /* cube */
} /* jetstream */

#endif /* end of include guard: AGGREGATE_FACTORY_M77OYEG0 */
