#ifndef CUBE_DIMENSION_H
#define CUBE_DIMENSION_H

#include <boost/shared_ptr.hpp>

#include "jetstream_types.pb.h"

namespace jetstream {
namespace cube {
  
using namespace ::std;
using namespace boost;

class Dimension {
public:
  Dimension (jetstream::CubeSchema_Dimension schema_dimension){
    name = schema_dimension.name();
    type = schema_dimension.type();
  };
  virtual ~Dimension ();

private:
  string name;
  Element_ElementType type;
};

} /* cube */
} /* jetstream */
#endif /* end of include guard: CUBE_DIMENSION_H */
