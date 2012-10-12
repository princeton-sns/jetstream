#ifndef CUBE_IMPL_H
#define CUBE_IMPL_H

#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>
#include "cube.h"
#include <vector>

#include "aggregate.h"
#include "dimension.h"
#include "dimension_factory.h"
#include "aggregate_factory.h"


namespace jetstream {
namespace cube {

/* functions placed here or lower in the type hierarchy rely on the type of CubeDimension
 * and CubeAggregate. This allows more generic code to operate on DataCubes of any Dimension
 * and Aggregate type. While, specific code need to Instantiate specific Impl types */

template <class CubeDimension=jetstream::cube::Dimension, class CubeAggregate=jetstream::cube::Aggregate>
class DataCubeImpl : public DataCube {
  public:
  
  DataCubeImpl(jetstream::CubeSchema _schema, std::string n, size_t batch=1): DataCube(_schema, n, batch) {
      build(_schema);
    }

    virtual void build(jetstream::CubeSchema _schema)  {

      //TODO: verify name is lowercase, _, no spaces.
//      name = _schema.name();
      shared_ptr<CubeDimension> ptr_dim;
      shared_ptr<CubeAggregate> ptr_agg;
      for (int i = 0; i < _schema.dimensions_size(); i++) {
        ptr_dim = DimensionFactory<CubeDimension>::create(_schema.dimensions(i));
        //ptr_dim = make_shared<CubeDimension>(_schema.dimensions(i));
        dimensions.push_back(ptr_dim);
        dimensionMap[ptr_dim->get_name()] = dimensions.size()-1;
      }

      for (int i = 0; i < _schema.aggregates_size() ; i++) {
        ptr_agg = AggregateFactory<CubeAggregate>::create(_schema.aggregates(i));
        aggregates.push_back(ptr_agg);
      }
    }

  boost::shared_ptr<CubeDimension> get_dimension(string name) const {
    size_t pos = dimensionMap.find(name)->second;
    return dimensions.at(pos);
  }


  protected:
    std::vector<boost::shared_ptr<CubeDimension> > dimensions;
    std::vector<boost::shared_ptr<CubeAggregate> > aggregates;
    std::map<string, size_t> dimensionMap;
  
    virtual DimensionKey get_dimension_key(Tuple const &t) const {
      string key="";
      for(size_t i=0; i<dimensions.size();++i) {
        key+=dimensions[i]->get_key(t)+"|";
      }
      return key; 
    }

    
    virtual void merge_tuple_into(jetstream::Tuple &into, jetstream::Tuple const &update) const {
      for(size_t i=0; i<aggregates.size();++i) {
        aggregates[i]->merge_tuple_into(into, update);
      }

    }
};

}
}

#endif /* end of include guard: CUBE_IMPL_H */
