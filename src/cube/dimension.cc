#include "dimension.h"
#include <glog/logging.h>

using namespace std;
using namespace jetstream::cube;

string jetstream::cube::Dimension::get_name() const {
  return name;
}

Dimension::Dimension () {} 

void Dimension::init(jetstream::CubeSchema_Dimension schema) {
  //TODO: verify name is lowercase and _ with no spaces
  name = schema.name();
  type = schema.type();

  assert(schema.tuple_indexes_size() >= 0);
  if((size_t)schema.tuple_indexes_size() != tuple_element_count())
  {
     LOG(FATAL) << "Wrong number of tuple indexes for "<< name;
  }

  for(int i=0; i<schema.tuple_indexes_size(); ++i) {
    tuple_indexes.push_back(schema.tuple_indexes(i));
  };
};

size_t Dimension::tuple_element_count() {
  return 1;
}
