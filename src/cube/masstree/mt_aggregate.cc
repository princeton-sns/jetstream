

#include "mt_aggregate.h"
#include <glog/logging.h>

using namespace std;
using namespace jetstream::cube;

namespace jetstream {

void
MTSumAggregate::update_from_delta(jetstream::Tuple & newV, const jetstream::Tuple& oldV) const {
  int old_val = oldV.e_size()-1 >= (int) tuple_indexes[0] ? oldV.e(tuple_indexes[0]).i_val() : 1;
  int new_val = 1;
  if (newV.e_size() > (int) tuple_indexes[0]) {
    new_val =  newV.e(tuple_indexes[0]).i_val();
  } else {
    //no new val
    LOG_IF(FATAL, old_val > 1) << "not sure how to emit a negative tuple";
    while( newV.e_size() <= (int) tuple_indexes[0])
      newV.add_e();
  }
  newV.mutable_e(tuple_indexes[0])->set_i_val(new_val - old_val);  
}


void
MTSumAggregate::merge_tuple_into(jetstream::Tuple & into, const jetstream::Tuple& update) const {
  if(update.e_size()-1 >= (int) tuple_indexes[0]) {
    const jetstream::Element& e_count_update = update.e(tuple_indexes[0]);
    jetstream::Element * e_count_into = into.mutable_e(tuple_indexes[0]);
    e_count_into->set_i_val(e_count_into->i_val()+e_count_update.i_val());
  }
  else {
    jetstream::Element * e_count_into = into.mutable_e(tuple_indexes[0]);
    e_count_into->set_i_val(e_count_into->i_val()+1);
  }
}


void
MTVersionAggregate::update_from_delta(jetstream::Tuple &into, jetstream::Tuple const &update) const {
  u_int64_t update_v = update.version();
  if (update_v > into.version())
    into.set_version(update_v);
}

void
MTVersionAggregate::merge_tuple_into(jetstream::Tuple &into, jetstream::Tuple const &update) const {
  u_int64_t update_v = update.version();
  if (update_v > into.version())
    into.set_version(update_v);
}

}
