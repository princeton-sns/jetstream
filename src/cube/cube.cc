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

DataCube::DataCube(jetstream::CubeSchema _schema, std::string _name) :
  schema(_schema), name(_name), is_frozen(false), 
  tupleBatcher(new cube::TupleBatch(this)), 
  version(0),
  flushExec(1), processExec(1),
  flushCongestMon(boost::shared_ptr<QueueCongestionMonitor>(new QueueCongestionMonitor(10, "cube " + _name + " flush"))),
  processCongestMon(boost::shared_ptr<ChainedQueueMonitor>(new ChainedQueueMonitor(10000, "cube " + _name + " process")))
{
  processCongestMon->set_next_monitor(flushCongestMon);
};

const std::string jetstream::DataCube::my_tyepename("data cube");

boost::shared_ptr<cube::TupleBatch> & DataCube::get_tuple_batcher()
{
  return tupleBatcher;
}


void DataCube::process(boost::shared_ptr<Tuple> t) {
  processCongestMon->report_insert(t.get(), 1);
  DimensionKey key = get_dimension_key(*t);
  processExec.submit(boost::bind(&DataCube::do_process, this, t, key));
}

void DataCube::do_process(boost::shared_ptr<Tuple> t, DimensionKey key) {
  boost::shared_ptr<cube::TupleBatch> &tupleBatcher = get_tuple_batcher();

  //tmpostr.str("");
  //tmpostr.clear();
  //get_dimension_key(*t, tmpostr);
  //DimensionKey key = tmpostr.str();
  //DimensionKey key = get_dimension_key(*t);

  bool in_batch = false;

  if (tupleBatcher->contains(key)) {
    in_batch = true;
  }

  shared_ptr<TupleProcessingInfo> tpi;
  if(!in_batch)
  {
    tpi = make_shared<TupleProcessingInfo>(t, key);
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

  //LOG(INFO) << "In do_process. " << elements_in_batch  ;


  if(!in_batch) {
    tupleBatcher->insert_tuple(tpi, can_batch);
  }
  else {
    tupleBatcher->update_batched_tuple(tpi, can_batch);
  }

  
  if(flushCongestMon->queue_length() < 1 && !tupleBatcher->is_empty()) 
  {
    //tupleBatcher->clear();
    queue_flush();
  }
  
  processCongestMon->report_delete(t.get(), 1);


/*
  if(can_batch && tupleBatcher->is_empty())
  {
    start_time = get_msec();
  }
  if(!in_batch) {
    tupleBatcher->insert_tuple(tpi, can_batch);
  }
  else {
    tupleBatcher->update_batched_tuple(tpi, can_batch);
  }
  
  processCongestMon->report_delete(t.get(), 1);

  if(tupleBatcher->size() >= elements_in_batch)
  {
    queue_flush();
  }
  else if(!tupleBatcher->is_empty())
  {
    if (processCongestMon->queue_length() == 0)
    {
      msec_t elapsed = get_msec() - start_time;
      msec_t timeout =  (msec_t) batch_timeout.total_milliseconds();
      if(elapsed >= timeout)
      {
        queue_flush();
      }
      else
      { // only need timeout if nothing is in process queue 
        msec_t left = timeout - elapsed;
        start_batch_timeout(left);
      }
    }
    else
    {
      //check every tuple_before_time_check tuple to prevent call to get_msec()
      if(time_check > tuples_before_time_check)
      {
        msec_t elapsed = get_msec() - start_time;
        msec_t timeout =  (msec_t) batch_timeout.total_milliseconds();
        if(elapsed >= timeout)
        {
          queue_flush();
          tuples_before_time_check = (batch_timeout.total_milliseconds() / 1); //pessimistic assumption of 1 ms per tuple
        }
        else
        {
          tuples_before_time_check = (timeout-elapsed) / 1; //assume here too
        }
        time_check = 0;
      }
      time_check++;
    }
  }
  */
}

void DataCube::wait_for_commits() {
  while(flushCongestMon->queue_length() > 0 || processCongestMon->queue_length() > 0)
  {
    js_usleep(processCongestMon->queue_length() + (flushCongestMon->queue_length()*10));
  }
}

size_t DataCube::batch_size() {
  return tupleBatcher->size();
}

void DataCube::queue_flush()
{
    //LOG_EVERY_N(INFO, 10) << "flushing size:" << tupleBatcher->size();
    //batch_timeout_timer.cancel();
    flushCongestMon->report_insert(tupleBatcher.get(), 1);
    flushExec.submit(boost::bind(&DataCube::do_flush, this, tupleBatcher));
  
    cube::TupleBatch * batch = new cube::TupleBatch(this);
    tupleBatcher.reset(batch);
}

void DataCube::do_flush(boost::shared_ptr<cube::TupleBatch> tb){
  tb->flush();

  if(flushCongestMon->queue_length() == 1 && processCongestMon->queue_length() < 1)
  {
    //we just flushed and there is nothing in the process q
    //but there may be stuff in tupleBatcher we need to flush (slow incoming rate)
    processCongestMon->report_insert(NULL, 1);
    processExec.submit(boost::bind(&DataCube::check_tuple_batcher_flush, this));
  }
  
  flushCongestMon->report_delete(tb.get(), 1);
}

void DataCube::check_tuple_batcher_flush() {
  if(!tupleBatcher->is_empty())
  {
    queue_flush();
  }
  processCongestMon->report_delete(NULL, 1);
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

