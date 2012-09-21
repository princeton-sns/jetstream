#include <dimension.h>

using namespace std;
using namespace jetstream::cube;

string jetstream::cube::Dimension::get_name() const {
  return name;
}

Dimension::Dimension (jetstream::CubeSchema_Dimension schema_dimension) {
  //TODO: verify name is lowercase and _ with no spaces
  name = schema_dimension.name();
  type = schema_dimension.type();
};
