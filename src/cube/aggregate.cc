#include "aggregate.h"

using namespace std;
using namespace jetstream::cube;

Aggregate::Aggregate (jetstream::CubeSchema_Aggregate schema_dimension) {
  name = schema_dimension.name();
  type = schema_dimension.type();
};
