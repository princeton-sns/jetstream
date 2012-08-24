#ifndef CUBE_AGGREGATE_H
#define CUBE_AGGREGATE_H

#include <boost/shared_ptr.hpp>

#include "jetstream_types.pb.h"

namespace jetstream {
namespace cube {
  
using namespace ::std;
using namespace boost;

class Aggregate {
public:
  Aggregate (jetstream::CubeSchema_Aggregate schema_dimension){
    name = schema_dimension.name();
    type = schema_dimension.type();
  };
  virtual ~Aggregate ();

private:
  string name;
  string type;
};

} /* cube */
} /* jetstream */
#endif /* end of include guard: CUBE_AGGREGATE_H */
