#ifndef AGGREGATE_FACTORY_M77OYEG0
#define AGGREGATE_FACTORY_M77OYEG0
#include "mysql/aggregate.h"
#include "mysql/aggregate_count.h"
#include "mysql/aggregate_avg.h"
#include "mysql/aggregate_string.h"
#include "mysql/aggregate_min.h"
#include "mysql/aggregate_version.h"
#include "mysql/aggregate_quantile.h"

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
    boost::shared_ptr<jetstream::cube::MysqlAggregate> obj;
    if(_schema.type() == "count") {
      obj = boost::make_shared<MysqlAggregateCount>();
    }
    else if(_schema.type() == "avg") {
      obj = boost::make_shared<MysqlAggregateAvg>();
    }
    else if(_schema.type() == "string") {
      obj = boost::make_shared<MysqlAggregateString>();
    }
    else if(_schema.type() == "quantile") {
      obj = boost::make_shared<MysqlAggregateQuantile>();
    }
    else if(_schema.type() == "min_i") {
      obj = boost::make_shared<MysqlAggregateMin<int> >();
    }
    else if(_schema.type() == "min_d") {
      obj = boost::make_shared<MysqlAggregateMin<double> >();
    }
    else if(_schema.type() == "min_t") {
      obj = boost::make_shared<MysqlAggregateMin<time_t> >();
    }
    else {
      LOG(FATAL) << "Don't have right aggregate";
    }
    obj->init(_schema);
    return obj;
  }

  static boost::shared_ptr<jetstream::cube::MysqlAggregate> version_aggregate(uint64_t& v) {
    return boost::shared_ptr<MysqlAggregateVersion>(new MysqlAggregateVersion(v));
  }


};

} /* cube */
} /* jetstream */

#endif /* end of include guard: AGGREGATE_FACTORY_M77OYEG0 */
