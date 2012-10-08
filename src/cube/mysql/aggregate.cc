#include "aggregate.h"
#include <glog/logging.h>

using namespace std;
using namespace jetstream::cube;

string  MysqlAggregate::get_base_column_name() const {
  return name;
}

vector<string>  MysqlAggregate::get_column_names() const {
  vector<string> decl;
  decl.push_back(get_base_column_name());
  return decl;
}

void MysqlAggregate::make_full_tuple(jetstream::Tuple &t) const {
  if(tuple_indexes.size() != number_tuple_elements())
  {
    LOG(FATAL) << "Wrong number of input tuple indexes for "<< name;
  }
  for(size_t i = 0; i<number_tuple_elements(); ++i)
  {
    while(t.e_size()-1 < (int)tuple_indexes[i])
    {
      t.add_e();
    }
  }
  insert_default_values_for_full_tuple(t);
}

void MysqlAggregate::merge_tuple_into(jetstream::Tuple &into, jetstream::Tuple const &update) const {
  make_full_tuple(into);
  merge_full_tuple_into(into, update);
}
