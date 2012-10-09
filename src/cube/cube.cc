/*
 * functions related to the hypercube data structure
 *
 */
#include "cube.h"

using namespace ::std;
using namespace jetstream;

unsigned int const jetstream::DataCube::LEAF_LEVEL = std::numeric_limits<unsigned int>::max();



const std::string jetstream::DataCube::my_tyepename("data cube");


void DataCube::process(boost::shared_ptr<Tuple> t) 
{
  boost::shared_ptr<jetstream::Tuple> new_tuple;
  boost::shared_ptr<jetstream::Tuple> old_tuple;
  save_tuple(*t, false, false, new_tuple, old_tuple);
 /* DimensionKey key = get_dimension_key(*t);
  std::map<DimensionKey, TupleProcessing>::iterator res = batch.find(key);
  
  TupleProcessing tp(t);
  if(res != batch.end())
  {
    tp = res->second;
    merge_tuple_into((tp.t), t);
  }

  for(std::map<operator_id_t, boost::shared_ptr<jetstream::cube::Subscriber> >::iterator it = subscribers.begin();
      it != subscribers.end(); ++it) {
    boost::shared_ptr<jetstream::cube::Subscriber> sub = (*it).second;
    cube::Subscriber::Action act = sub->action_on_tuple(t);
    if(act == cube::Subscriber::SEND) {
      tp.insert.push_back((*it).first);
      tp.need_new_value = true;
    }
    else if(act == cube::Subscriber::SEND_NO_BATCH) {
      tp.insert.push_back((*it).first);
      tp.need_new_value = true;
      tp.batch = false;
    }
    else if(act == cube::Subscriber::SEND_NO_BATCH) {
      tp.update.push_back((*it).first);
      tp.need_new_value = true;
      tp.need_old_value = true;
    }
  }
  
  insert_partial_aggregate(*t); */
}  
