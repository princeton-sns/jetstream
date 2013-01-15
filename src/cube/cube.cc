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

DataCube::DataCube(jetstream::CubeSchema _schema, std::string _name, size_t elements_in_batch,  boost::posix_time::time_duration batch_timeout) :
  schema(_schema), name(_name), is_frozen(false), 
  tupleBatcher(new cube::TupleBatch(this, elements_in_batch)), 
  batch_timeout(batch_timeout), version(0),
  elements_in_batch(elements_in_batch),
  flushExec(1), processExec(1),
  batch_timeout_timer(*(processExec.get_io_service())),
  congestMon(boost::shared_ptr<QueueCongestionMonitor>(new QueueCongestionMonitor(10, "cube " + _name)))  {
};

const std::string jetstream::DataCube::my_tyepename("data cube");

boost::shared_ptr<cube::TupleBatch> & DataCube::get_tuple_batcher()
{
  return tupleBatcher;
}


void DataCube::process(boost::shared_ptr<Tuple> t) {
  processExec.submit(boost::bind(&DataCube::do_process, this, t));
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
  shared_lock<boost::shared_mutex> lock(subscriberLock);  
  for(std::map<operator_id_t, boost::shared_ptr<jetstream::cube::Subscriber> >::iterator it = subscribers.begin();
      it != subscribers.end(); ++it) {
    boost::shared_ptr<jetstream::cube::Subscriber> sub = (*it).second;
    cube::Subscriber::Action act = sub->action_on_tuple(t);
    //LOG(INFO) << "Action: "<< act << "send is: " <<  cube::Subscriber::SEND;
    if(act == cube::Subscriber::SEND) {
      if(!in_batch) {
        VLOG(3) << "Action: "<< act << " adding to insert: " <<  tpi->key;
        tpi->insert.push_back((*it).first);
        tpi->need_new_value = true;
      }
    }
    else if(act == cube::Subscriber::SEND_NO_BATCH) {
      if(!in_batch) {
        VLOG(3) << "Action: "<< act << " adding to insert: " <<  tpi->key;
        tpi->insert.push_back((*it).first);
        tpi->need_new_value = true;
      }
      can_batch = false;
    }
    else if(act == cube::Subscriber::SEND_UPDATE) {
      if(!in_batch) {
        VLOG(3) << "Action: "<< act << " adding to update: " <<  tpi->key;
        tpi->update.push_back((*it).first);

        tpi->need_new_value = true;
        tpi->need_old_value = true;
      }
    }
  }

  VLOG(2) <<"Process: "<< key << "in batch: "<<in_batch << " can batch:" << can_batch << " need new:" << tpi->need_new_value << " need old:"<< tpi->need_old_value;


  if(can_batch && tupleBatcher->is_empty())
  {
    start_batch_timeout();
    start_time = get_msec();
  }
  if(!in_batch) {
    tupleBatcher->insert_tuple(tpi, can_batch);
  }
  else {
    tupleBatcher->update_batched_tuple(tpi, can_batch);
  }

  if(tupleBatcher->is_full() || (!tupleBatcher->is_empty() && (get_msec() - start_time > (msec_t) batch_timeout.total_milliseconds())))
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

void DataCube::set_elements_in_batch(size_t size) {
  elements_in_batch = size;
  tupleBatcher->set_max_batch_size(size);
}

void DataCube::queue_flush()
{
    batch_timeout_timer.cancel();
    flushExec.submit(boost::bind(&DataCube::do_flush, this, tupleBatcher));
  
    cube::TupleBatch * batch = new cube::TupleBatch(this, elements_in_batch);
    congestMon->report_insert(batch, 1);
    tupleBatcher.reset(batch);
}

void DataCube::do_flush(boost::shared_ptr<cube::TupleBatch> tb){
  tb->flush();
  congestMon->report_delete(tb.get(), 1);
}

/* the batch timer is only meant for the case that there are very few
 * tuples going through the system, otherwise should flush in do_process() */

void DataCube::start_batch_timeout() {
  batch_timeout_timer.expires_from_now(batch_timeout);
  batch_timeout_timer.async_wait(boost::bind(&DataCube::batch_timer_fired, this, tupleBatcher, _1));
}

void DataCube::batch_timer_fired(boost::shared_ptr<cube::TupleBatch> batcher, const boost::system::error_code& error) {
  if (error)
    return;

  if(tupleBatcher == batcher && !tupleBatcher->is_empty())
  {
    //need to check that batcher still points to tupleBatcher in the following case:
    //flush() was called after batch_timeout_timer went off and put batch_timer_fired on strand
    //but before batch_timer_fired could be executed by the processExec();
    queue_flush();
  }
}

void DataCube::save_callback(jetstream::TupleProcessingInfo &tpi, boost::shared_ptr<jetstream::Tuple> new_tuple, boost::shared_ptr<jetstream::Tuple> old_tuple) {
    shared_lock<boost::shared_mutex> lock(subscriberLock);

    for( std::list<operator_id_t>::iterator it=tpi.insert.begin(); it != tpi.insert.end(); ++it) {
      VLOG(3) << "Insert Callback:" <<tpi.key<<"; sub:"<<*it;
      if(subscribers.count(*it) > 0)
        subscribers[*it]->insert_callback(tpi.t, new_tuple);
    }

    for( std::list<operator_id_t>::iterator it=tpi.update.begin(); it != tpi.update.end(); ++it) {
      VLOG(3) << "Update Callback:" <<tpi.key<<"; sub:"<<*it;
      if(subscribers.count(*it) > 0)
        subscribers[*it]->update_callback(tpi.t, new_tuple, old_tuple);
    }
  VLOG(2) << "End Save Callback:" <<tpi.key;
}


void DataCube::add_subscriber(boost::shared_ptr<cube::Subscriber> sub) {
  lock_guard<boost::shared_mutex> lock(subscriberLock);
  
  LOG_IF(FATAL, sub->has_cube()) << "can't attach subscriber" << sub->id() << " to cube " << name<<
    "; it is already attached to " << sub->cube->name;
//  assert(!sub->has_cube()); //for now, assume subscriber-cube matching is permanent
  sub->set_cube(this);
  subscribers[sub->id()] = sub;
  LOG(INFO) << "Adding subscriber "<< sub->id() << " to " << id_as_str(); 
}



void DataCube::remove_subscriber(boost::shared_ptr<cube::Subscriber> sub) {
  return remove_subscriber(sub->id());
}

void DataCube::remove_subscriber(operator_id_t id) {
  lock_guard<boost::shared_mutex> lock(subscriberLock);
  subscribers.erase(id);
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

