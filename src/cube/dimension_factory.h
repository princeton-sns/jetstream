#ifndef DIMENSION_FACTORY_M77OYEG0
#define DIMENSION_FACTORY_M77OYEG0
#include "mysql/dimension.h"
#include "mysql/dimension_time.h"
#include "mysql/dimension_int.h"
#include "mysql/dimension_string.h"
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
    if(_schema.type() == jetstream::Element_ElementType_TIME){
      return boost::make_shared<MysqlDimensionTime>(_schema);
    }
    else if(_schema.type() == jetstream::Element_ElementType_INT32) {
      return boost::make_shared<MysqlDimensionInt>(_schema);
    }
    else if(_schema.type() == jetstream::Element_ElementType_DOUBLE) {
      return boost::make_shared<MysqlDimensionDouble>(_schema);
    }
    else if(_schema.type() == jetstream::Element_ElementType_STRING) {
      return boost::make_shared<MysqlDimensionString>(_schema);
    }
    else
    {
      LOG(FATAL) <<"Unknown dimension type";
    }
  };
};

} /* cube */
} /* jetstream */

#endif /* end of include guard: DIMENSION_FACTORY_M77OYEG0 */
