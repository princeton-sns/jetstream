#include "aggregate.h"
#include <glog/logging.h>

using namespace std;
using namespace jetstream::cube;

Aggregate::Aggregate () {
  
}

string Aggregate::get_name() const {
  return name;
}

void Aggregate::init(jetstream::CubeSchema_Aggregate schema) {
  name = schema.name();
  type = schema.type();

  assert(schema.tuple_indexes_size() >= 0);

  if((size_t)schema.tuple_indexes_size() != number_tuple_elements())
  {
     LOG(FATAL) << "Wrong number of tuple indexes for "<< name <<" Expected: " << number_tuple_elements();
  }
  
  for(int i=0; i<schema.tuple_indexes_size(); ++i) {
    tuple_indexes.push_back(schema.tuple_indexes(i));
  }

};



size_t Aggregate::number_tuple_elements() const
{
  return 1;
}
