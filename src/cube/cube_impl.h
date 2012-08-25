#ifndef CUBE_IMPL_H
#define CUBE_IMPL_H

#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>
#include "cube.h"
#include <vector>

#include "aggregate.h"
#include "dimension.h"


namespace jetstream {
namespace cube {

template <class CubeDimension=jetstream::cube::Dimension, class CubeAggregate=jetstream::cube::Aggregate>
class DataCubeImpl : public DataCube {
public:
  DataCubeImpl(jetstream::CubeSchema _schema);
  void build(jetstream::CubeSchema _schema); 

private:
  std::vector<boost::shared_ptr<CubeDimension> > dimensions;
  std::vector<boost::shared_ptr<CubeAggregate> > aggregates;
};

}
}

#endif /* end of include guard: CUBE_IMPL_H */
