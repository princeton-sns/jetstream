#ifndef CUBE_DIMENSION_H
#define CUBE_DIMENSION_H

#include <boost/shared_ptr.hpp>

#include "jetstream_types.pb.h"
#include "cube.h"

namespace jetstream {
namespace cube {
  
using namespace ::std;
using namespace boost;

class Dimension {
public:
  Dimension ();
  void init(jetstream::CubeSchema_Dimension schema_dimension);
  virtual ~Dimension (){};

  string get_name() const;
  virtual jetstream::DataCube::DimensionKey get_key(Tuple const &t) const = 0;
  virtual size_t tuple_element_count();

protected:
  string name;
  Element_ElementType type;
  vector<size_t> tuple_indexes;
};

} /* cube */
} /* jetstream */
#endif /* end of include guard: CUBE_DIMENSION_H */
