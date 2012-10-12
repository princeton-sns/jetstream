/*
 * functions related to the hypercube data structure
 *
 */
#include "cube.h"
#include <glog/logging.h>

using namespace ::std;
using namespace jetstream;

unsigned int const jetstream::DataCube::LEAF_LEVEL = std::numeric_limits<unsigned int>::max();


DataCube::DataCube(jetstream::CubeSchema _schema, std::string _name, size_t batch) :
      schema(_schema), name(_name), is_frozen(false), tupleBatcher(new cube::TupleBatch(this, batch)) {};

const std::string jetstream::DataCube::my_tyepename("data cube");


void DataCube::process(boost::shared_ptr<Tuple> t) 
{
  //boost::shared_ptr<jetstream::Tuple> new_tuple;
  //boost::shared_ptr<jetstream::Tuple> old_tuple;
  //save_tuple(*t, false, false, new_tuple, old_tuple);
  DimensionKey key = get_dimension_key(*t);

  std::map<DimensionKey, TupleProcessing>::iterator res = batch.find(key);
  
  TupleProcessing tp(t);
  bool in_batch = false;

  if(res != batch.end())
  {
    in_batch = true;
    tp = res->second;
    merge_tuple_into(*(tp.t), *t);
  }
  LOG(INFO) <<"Process: "<< key << "in batch: "<<in_batch<<" count: "<<batch.count(key);

  bool batch = true;
  bool need_new_value = false;
  bool need_old_value = false;
  for(std::map<operator_id_t, boost::shared_ptr<jetstream::cube::Subscriber> >::iterator it = subscribers.begin();
      it != subscribers.end(); ++it) {
    boost::shared_ptr<jetstream::cube::Subscriber> sub = (*it).second;
    cube::Subscriber::Action act = sub->action_on_tuple(t);
    if(act == cube::Subscriber::SEND) {
      if(!in_batch)
      {
        tp.insert.push_back((*it).first);
        need_new_value = true;
      }
    }
    else if(act == cube::Subscriber::SEND_NO_BATCH) {
      if(!in_batch)
      {
        tp.insert.push_back((*it).first);
      
        need_new_value = true;
      }
      batch = false;
    }
    else if(act == cube::Subscriber::SEND_UPDATE) {
      if(!in_batch)
      {
        tp.update.push_back((*it).first);
      
        need_new_value = true;
        need_old_value = true;
      }
    }
  }
 
  this->batch.insert(pair<DimensionKey,TupleProcessing>(key, tp));
  if(!in_batch)
  {
    tp.pos = tupleBatcher->insert_tuple(t, batch, need_new_value, need_old_value);
    if(this->batch.count(key))
      this->batch.insert(pair<DimensionKey,TupleProcessing>(key, tp));
  }
  else
  {
    tupleBatcher->update_batched_tuple(tp.pos, t, batch);
  }
}  
    
void DataCube::save_callback(DimensionKey key, boost::shared_ptr<jetstream::Tuple> update, boost::shared_ptr<jetstream::Tuple> new_tuple, boost::shared_ptr<jetstream::Tuple> old_tuple)
{
  std::map<DimensionKey, TupleProcessing>::iterator res = batch.find(key);
  
  if(res != batch.end())
  {
    TupleProcessing tp = res->second;
    for( std::list<operator_id_t>::iterator it=tp.insert.begin(); it != tp.insert.end(); ++it)
    {
      subscribers[*it]->insert_callback(update, new_tuple); 
    }
    for( std::list<operator_id_t>::iterator it=tp.update.begin(); it != tp.update.end(); ++it)
    {
      subscribers[*it]->update_callback(update, new_tuple, old_tuple); 
    }
  }

  batch.erase(key);
  LOG(INFO) << "In Save Callback:" <<key << "cnt: "<< batch.count(key);
}
