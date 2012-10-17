/*
 * functions related to the hypercube data structure
 *
 */
#include "cube.h"
#include <glog/logging.h>
#include <boost/shared_ptr.hpp>

using namespace ::std;
using namespace jetstream;
using namespace boost;

unsigned int const jetstream::DataCube::LEAF_LEVEL = std::numeric_limits<unsigned int>::max();


DataCube::DataCube(jetstream::CubeSchema _schema, std::string _name, size_t batch) :
  schema(_schema), name(_name), is_frozen(false), tupleBatcher(new cube::TupleBatch(this, batch)) {};

const std::string jetstream::DataCube::my_tyepename("data cube");

boost::scoped_ptr<cube::TupleBatch> & DataCube::get_tuple_batcher()
{
  return tupleBatcher;
}


void DataCube::process(boost::shared_ptr<Tuple> t) {
  boost::scoped_ptr<cube::TupleBatch> & tupleBatcher = get_tuple_batcher();
  DimensionKey key = get_dimension_key(*t);

  bool in_batch = false;

  if (tupleBatcher->contains(key)) {
    in_batch = true;
  }

  shared_ptr<TupleProcessingInfo> tpi;
  if(!in_batch)
  {
    tpi = make_shared<TupleProcessingInfo>(t, get_dimension_key(*t));
  }
  else
  {
    tpi = tupleBatcher->get(key);
    merge_tuple_into(*(tpi->t), *t);
  }

  bool can_batch = true;
  for(std::map<operator_id_t, boost::shared_ptr<jetstream::cube::Subscriber> >::iterator it = subscribers.begin();
      it != subscribers.end(); ++it) {
    boost::shared_ptr<jetstream::cube::Subscriber> sub = (*it).second;
    cube::Subscriber::Action act = sub->action_on_tuple(t);

    if(act == cube::Subscriber::SEND) {
      if(!in_batch) {
        tpi->insert.push_back((*it).first);
        tpi->need_new_value = true;
      }
    }
    else if(act == cube::Subscriber::SEND_NO_BATCH) {
      if(!in_batch) {
        tpi->insert.push_back((*it).first);
        tpi->need_new_value = true;
      }
      can_batch = false;
    }
    else if(act == cube::Subscriber::SEND_UPDATE) {
      if(!in_batch) {
        tpi->update.push_back((*it).first);

        tpi->need_new_value = true;
        tpi->need_old_value = true;
      }
    }
  }

  LOG(INFO) <<"Process: "<< key << "in batch: "<<in_batch << " can batch:" << can_batch;

  if(!in_batch) {
    tupleBatcher->insert_tuple(tpi, can_batch);
  }
  else {
    tupleBatcher->update_batched_tuple(tpi, can_batch);
  }

  if(tupleBatcher->is_full())
    tupleBatcher->flush();
}

void DataCube::save_callback(jetstream::TupleProcessingInfo &tpi, boost::shared_ptr<jetstream::Tuple> new_tuple, boost::shared_ptr<jetstream::Tuple> old_tuple) {


    for( std::list<operator_id_t>::iterator it=tpi.insert.begin(); it != tpi.insert.end(); ++it) {
      subscribers[*it]->insert_callback(tpi.t, new_tuple);
    }

    for( std::list<operator_id_t>::iterator it=tpi.update.begin(); it != tpi.update.end(); ++it) {
      subscribers[*it]->update_callback(tpi.t, new_tuple, old_tuple);
    }
  LOG(INFO) << "End Save Callback:" <<tpi.key;
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

