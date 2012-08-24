#include "cube_impl.h"

using namespace ::std;
using namespace boost;

using namespace jetstream::cube; 

template <class CubeDimension, class CubeAggregate>
DataCubeImpl<CubeDimension, CubeAggregate>::DataCubeImpl(jetstream::CubeSchema _schema) : DataCube(_schema) {
  build(_schema);
}

template <class CubeDimension, class CubeAggregate>
void 
DataCubeImpl<CubeDimension, CubeAggregate>::build(jetstream::CubeSchema _schema) {
  name = _schema.name();
  shared_ptr<CubeDimension> ptr_dim;
  shared_ptr<CubeAggregate> ptr_agg;
  for (int i = 0; i < _schema.dimensions_size(); i++) {
    ptr_dim = make_shared<CubeDimension>(_schema.dimensions(i));
    dimensions.push_back(ptr_dim);
  }

  for (int i = 0; i < _schema.aggregates_size() ; i++) {
    ptr_agg = make_shared<CubeAggregate>(_schema.aggregates(i));
    aggregates.push_back(ptr_agg);
  }
}


