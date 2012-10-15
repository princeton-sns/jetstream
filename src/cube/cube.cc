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


void DataCube::process(boost::shared_ptr<Tuple> t) {
  //boost::shared_ptr<jetstream::Tuple> new_tuple;
  //boost::shared_ptr<jetstream::Tuple> old_tuple;
  //save_tuple(*t, false, false, new_tuple, old_tuple);
  DimensionKey key = get_dimension_key(*t);

  bool in_batch = false;

  if (batch.count(key)) {
    in_batch = true;
  }

  TupleProcessing & tp = batch[key];

  if(in_batch) {
    merge_tuple_into(*(tp.t), *t);
  }
  else {
    tp.t = t;
  }


  bool can_batch = true;
  bool need_new_value = false;
  bool need_old_value = false;

  for(std::map<operator_id_t, boost::shared_ptr<jetstream::cube::Subscriber> >::iterator it = subscribers.begin();
      it != subscribers.end(); ++it) {
    boost::shared_ptr<jetstream::cube::Subscriber> sub = (*it).second;
    cube::Subscriber::Action act = sub->action_on_tuple(t);

    if(act == cube::Subscriber::SEND) {
      if(!in_batch) {
        tp.insert.push_back((*it).first);
        need_new_value = true;
      }
    }
    else if(act == cube::Subscriber::SEND_NO_BATCH) {
      if(!in_batch) {
        tp.insert.push_back((*it).first);

        need_new_value = true;
      }

      can_batch = false;
    }
    else if(act == cube::Subscriber::SEND_UPDATE) {
      if(!in_batch) {
        tp.update.push_back((*it).first);

        need_new_value = true;
        need_old_value = true;
      }
    }
  }

  LOG(INFO) <<"Process: "<< key << "in batch: "<<in_batch<<" count: "<<batch.count(key) <<" pos: " << tp.pos << " can batch:" << can_batch;

  if(!in_batch) {
    tp.pos = tupleBatcher->insert_tuple(t, can_batch, need_new_value, need_old_value);
  }
  else {
    tupleBatcher->update_batched_tuple(tp.pos, t, can_batch);
  }
}

void DataCube::save_callback(DimensionKey key, boost::shared_ptr<jetstream::Tuple> update, boost::shared_ptr<jetstream::Tuple> new_tuple, boost::shared_ptr<jetstream::Tuple> old_tuple) {
  std::map<DimensionKey, TupleProcessing>::iterator res = batch.find(key);

  if(res != batch.end()) {
    TupleProcessing tp = res->second;

    for( std::list<operator_id_t>::iterator it=tp.insert.begin(); it != tp.insert.end(); ++it) {
      subscribers[*it]->insert_callback(update, new_tuple);
    }

    for( std::list<operator_id_t>::iterator it=tp.update.begin(); it != tp.update.end(); ++it) {
      subscribers[*it]->update_callback(update, new_tuple, old_tuple);
    }
  }

  batch.erase(key);
  LOG(INFO) << "In Save Callback:" <<key << "cnt: "<< batch.count(key);
}


void DataCube::add_subscriber(boost::shared_ptr<cube::Subscriber> sub) {
  assert(sub->has_cube());
  subscribers[sub->id()] = sub;
}

bool DataCube::remove_subscriber(boost::shared_ptr<cube::Subscriber> sub) {
  return remove_subscriber(sub->id());
}

bool DataCube::remove_subscriber(operator_id_t id) {
  if(subscribers.erase(id))
    return true;

  return false;
}

Tuple DataCube::empty_tuple() {
  Tuple t;

  for (int i=0; i < schema.dimensions_size(); ++i) {
    t.add_e();
  }

  return t;
}

const jetstream::CubeSchema& DataCube::get_schema() {
  return schema;
}
std::string DataCube::id_as_str() {
  return name;
}
const std::string& DataCube::typename_as_str() {
  return my_tyepename;
}

