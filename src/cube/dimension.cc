#include "dimension.h"

using namespace std;
using namespace jetstream::cube;

string jetstream::cube::Dimension::get_name() const {
  return name;
}

Dimension::Dimension (jetstream::CubeSchema_Dimension schema) {
  //TODO: verify name is lowercase and _ with no spaces
  name = schema.name();
  type = schema.type();

  for(int i=0; i<schema.tuple_indexes_size(); ++i) {
    tuple_indexes.push_back(schema.tuple_indexes(i));
  };
};
