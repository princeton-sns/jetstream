#include "aggregate.h"

using namespace std;
using namespace jetstream::cube;

Aggregate::Aggregate (jetstream::CubeSchema_Aggregate schema) {
  name = schema.name();
  type = schema.type();

  for(int i=0; i<schema.tuple_indexes_size(); ++i) {
    tuple_indexes.push_back(schema.tuple_indexes(i));
  }

};
