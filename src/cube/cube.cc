/*
 * functions related to the hypercube data structure
 *
 */
#include "cube.h"
#include <glog/logging.h>
#include <boost/shared_ptr.hpp>
#include <boost/bind.hpp>

using namespace ::std;
using namespace jetstream;
using namespace boost;

unsigned int const jetstream::DataCube::LEAF_LEVEL = std::numeric_limits<unsigned int>::max();


DataCube::DataCube(jetstream::CubeSchema _schema, std::string _name, size_t elements_in_batch, size_t num_threads, boost::posix_time::time_duration batch_timeout) :
  schema(_schema), name(_name), is_frozen(false), 
  tupleBatcher(new cube::TupleBatch(this, elements_in_batch)), 
  batch_timeout(batch_timeout), elements_in_batch(elements_in_batch), 
  exec(num_threads), flushStrand(exec.make_strand()), processStrand(exec.make_strand()),
  outstanding_batches(0), batch_timeout_timer(*(exec.get_io_service())) {};

const std::string jetstream::DataCube::my_tyepename("data cube");

boost::shared_ptr<cube::TupleBatch> & DataCube::get_tuple_batcher()
{
  return tupleBatcher;
}


void DataCube::process(boost::shared_ptr<Tuple> t) {
  processStrand->dispatch(boost::bind(&DataCube::do_process, this, t));
}

void DataCube::do_process(boost::shared_ptr<Tuple> t) {
  boost::shared_ptr<cube::TupleBatch> &tupleBatcher = get_tuple_batcher();
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
    //LOG(INFO) << "Action: "<< act << "send is: " <<  cube::Subscriber::SEND;
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

  VLOG(1) <<"Process: "<< key << "in batch: "<<in_batch << " can batch:" << can_batch << " need new:" << tpi->need_new_value << " need old:"<< tpi->need_old_value;


  if(can_batch && tupleBatcher->is_empty())
  {
    start_batch_timeout();
  }
  if(!in_batch) {
    tupleBatcher->insert_tuple(tpi, can_batch);
  }
  else {
    tupleBatcher->update_batched_tuple(tpi, can_batch);
  }

  if(tupleBatcher->is_full())
  {
    queue_flush();
  }
}

size_t DataCube::batch_size() {
  return tupleBatcher->size();
}

void DataCube::set_batch_timeout(boost::posix_time::time_duration timeout)
{
  batch_timeout = timeout;  
}

void DataCube::queue_flush() //always executed in the processStrand
{
    batch_timeout_timer.cancel();
    flushStrand->post(boost::bind(&DataCube::do_flush, this, tupleBatcher));
    outstanding_batches++;
    tupleBatcher.reset(new cube::TupleBatch(this, elements_in_batch));
}

void DataCube::do_flush(boost::shared_ptr<cube::TupleBatch> tb){
  tb->flush();
  processStrand->post(boost::bind(&DataCube::post_flush, this));
}

void DataCube::post_flush() {
  outstanding_batches--;
}

void DataCube::start_batch_timeout() {
  batch_timeout_timer.expires_from_now(batch_timeout);
  batch_timeout_timer.async_wait(processStrand->wrap(boost::bind(&DataCube::batch_timer_fired, this, tupleBatcher)));
}

void DataCube::batch_timer_fired(boost::shared_ptr<cube::TupleBatch> batcher) {
  if(tupleBatcher == batcher && !tupleBatcher->is_empty())
  {
    //need to check that batcher still points to tupleBatcher in the following case:
    //flush() was called after batch_timeout_timer went off and put batch_timer_fired on strand
    //but before batch_timer_fired could be executed by the processStrand();
    queue_flush();
  }
}

void DataCube::save_callback(jetstream::TupleProcessingInfo &tpi, boost::shared_ptr<jetstream::Tuple> new_tuple, boost::shared_ptr<jetstream::Tuple> old_tuple) {


    for( std::list<operator_id_t>::iterator it=tpi.insert.begin(); it != tpi.insert.end(); ++it) {
      subscribers[*it]->insert_callback(tpi.t, new_tuple);
    }

    for( std::list<operator_id_t>::iterator it=tpi.update.begin(); it != tpi.update.end(); ++it) {
      subscribers[*it]->update_callback(tpi.t, new_tuple, old_tuple);
    }
  VLOG(1) << "End Save Callback:" <<tpi.key;
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

