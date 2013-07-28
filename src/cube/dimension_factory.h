#ifndef DIMENSION_FACTORY_M77OYEG0
#define DIMENSION_FACTORY_M77OYEG0
#include "mysql/dimension.h"
#include "mysql/dimension_time.h"

#include "mysql/dimension_time_containment.h"
#include "mysql/dimension_int.h"
#include "mysql/dimension_string.h"
//#include "mysql/dimension_url.h"
#include "mysql/dimension_double.h"
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <glog/logging.h>

namespace jetstream {
namespace cube {

template <class I>
struct DimensionFactory{
  static boost::shared_ptr<I> create(jetstream::CubeSchema_Dimension _schema)
  {
    return boost::make_shared<I>(_schema);
  }
};

template<>
struct DimensionFactory<jetstream::cube::MysqlDimension>
{
  static boost::shared_ptr<jetstream::cube::MysqlDimension> create(jetstream::CubeSchema_Dimension _schema)
  {
    if(_schema.type() == jetstream::CubeSchema_Dimension_DimensionType_TIME){
        boost::shared_ptr<jetstream::cube::MysqlDimension> obj = boost::make_shared<jetstream::cube::MysqlDimensionTime>();
        obj->init(_schema);
      return obj;
    }
    else if(_schema.type() == jetstream::CubeSchema_Dimension_DimensionType_TIME_CONTAINMENT){
        boost::shared_ptr<jetstream::cube::MysqlDimension> obj = boost::make_shared<MysqlDimensionTimeContainment>();
        obj->init(_schema);
      return obj;
    }
    else if(_schema.type() == jetstream::CubeSchema_Dimension_DimensionType_INT32) {
      boost::shared_ptr<jetstream::cube::MysqlDimension> obj = boost::make_shared<MysqlDimensionInt>();
      obj->init(_schema);
      return obj;
    }
    else if(_schema.type() == jetstream::CubeSchema_Dimension_DimensionType_DOUBLE) {
      boost::shared_ptr<jetstream::cube::MysqlDimension> obj = boost::make_shared<MysqlDimensionDouble>();
      obj->init(_schema);
      return obj;
    }
    else if(_schema.type() == jetstream::CubeSchema_Dimension_DimensionType_STRING) {
      boost::shared_ptr<jetstream::cube::MysqlDimension> obj = boost::make_shared<MysqlDimensionString>();
      obj->init(_schema);
      return obj;
    }/*
    else if(_schema.type() == jetstream::CubeSchema_Dimension_DimensionType_URL) {
      boost::shared_ptr<jetstream::cube::MysqlDimension> obj = boost::make_shared<MysqlDimensionUrl>();
      obj->init(_schema);
      return obj;
    }*/
    else
    {
      LOG(FATAL) <<"Unknown dimension type \'" << _schema.type() << "'";
      boost::shared_ptr<jetstream::cube::MysqlDimension> obj;
      return obj;    
    }
  };
};

} /* cube */
} /* jetstream */

#endif /* end of include guard: DIMENSION_FACTORY_M77OYEG0 */
