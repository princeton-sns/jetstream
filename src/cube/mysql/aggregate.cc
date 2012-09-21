#include "aggregate.h"

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

