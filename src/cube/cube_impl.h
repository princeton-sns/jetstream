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
        aggregateMap[ptr_agg->get_name()] = aggregates.size()-1;
      }
      aggregates.push_back( AggregateFactory<CubeAggregate>::version_aggregate(version) );
      aggregateMap["version"] = aggregates.size()-1;
    }

    boost::shared_ptr<CubeDimension> get_dimension(string name) const {
      std::map<string,size_t>::const_iterator found = dimensionMap.find(name);
      if(found != dimensionMap.end()) {
        size_t pos = found->second;
        return dimensions.at(pos);
      }
      LOG(FATAL) << "No dimension: "<<name;
    }

    bool has_dimension(string name) const {
      return dimensionMap.count(name) > 0;
    }

    boost::shared_ptr<CubeAggregate> get_aggregate(string name) const {
      std::map<string,size_t>::const_iterator found = aggregateMap.find(name);
      if(found != aggregateMap.end()) {
        size_t pos = found->second;
        return aggregates.at(pos);
      }
      LOG(FATAL) << "No aggregate: "<<name;
    }

    bool has_aggregate(string name) const {
      return aggregateMap.count(name) > 0;
    }


  protected:
    std::vector<boost::shared_ptr<CubeDimension> > dimensions;
    std::vector<boost::shared_ptr<CubeAggregate> > aggregates;
    std::map<string, size_t> dimensionMap;
    std::map<string, size_t> aggregateMap;

    virtual DimensionKey get_dimension_key(const Tuple &t) const {
      string key="";

      for(size_t i=0; i<dimensions.size(); ++i) {
        key+=dimensions[i]->get_key(t)+"|";
      }

      return key;
    }

    virtual void merge_tuple_into(jetstream::Tuple &into, jetstream::Tuple const &update) const {
      for(size_t i=0; i<aggregates.size(); ++i) {
        aggregates[i]->merge_tuple_into(into, update);
      }
    }


};

}
}

#endif /* end of include guard: CUBE_IMPL_H */
