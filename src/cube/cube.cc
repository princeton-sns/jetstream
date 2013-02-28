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

unsigned int const jetstream::DataCube::LEAF_LEVEL =  100000;
  //std::numeric_limits<unsigned int>::max();

ProcessCallable::ProcessCallable(DataCube * cube, std::string name): name(name), service(new io_service(1)), work(*service), cube(cube), tupleBatcher(new cube::TupleBatch(cube)) {

  // this should always be the last line in the constructor
  internal_thread = boost::thread(&ProcessCallable::run, this);
}


ProcessCallable::~ProcessCallable() {
  service->stop();
  internal_thread.join();
}

void ProcessCallable::run() {
  jetstream::set_thread_name("js-cube-proc-"+name);
  service->run();
}

void ProcessCallable::assign(boost::shared_ptr<Tuple> t, DimensionKey key, boost::shared_ptr<std::vector<unsigned int> > levels) {
  service->post(boost::bind(&ProcessCallable::process, this, t, key, levels));
}

void ProcessCallable::process(boost::shared_ptr<Tuple> t, DimensionKey key, boost::shared_ptr<std::vector<unsigned int> > levels ) {
  VLOG(10) << "got a tuple in ProcessCallable with key " << key;
  boost::lock_guard<boost::mutex> lock(batcherLock);
  cube->do_process(t, key, levels, tupleBatcher);
  VLOG(10) << "finished a tuple in ProcessCallable with key " << key;
}

boost::shared_ptr<cube::TupleBatch> ProcessCallable::batch_flush() {
  boost::lock_guard<boost::mutex> lock(batcherLock);
  boost::shared_ptr<cube::TupleBatch> ptr = tupleBatcher;
  cube::TupleBatch * batch = new cube::TupleBatch(cube);
  tupleBatcher.reset(batch);
  return ptr;
}

bool ProcessCallable::batcher_ready() {
  return !tupleBatcher->is_empty();
}




DataCube::DataCube(jetstream::CubeSchema _schema, std::string _name, const NodeConfig &conf) :
  schema(_schema), name(_name), is_frozen(false),
  version(0),
  flushExec(1, "js-cube-flush"), current_processor(0),
  flushCongestMon(boost::shared_ptr<QueueCongestionMonitor>(new QueueCongestionMonitor(10, "cube " + _name + " flush"))),
  processCongestMon(boost::shared_ptr<ChainedQueueMonitor>(new ChainedQueueMonitor(10000, "cube " + _name + " process")))
{
  processCongestMon->set_next_monitor(flushCongestMon);

  LOG(INFO) << "Starting cube with "<<conf.cube_processor_threads <<" threads ";
  for(size_t i=0; i<conf.cube_processor_threads;i++) {
    boost::shared_ptr<ProcessCallable> proc(new ProcessCallable(this, boost::lexical_cast<string>(i)));
    processors.push_back(proc);
  }

};

const std::string jetstream::DataCube::my_tyepename("data cube");

void DataCube::process(boost::shared_ptr<Tuple> t) {
//  LOG(INFO) << "processing" << fmt(*t);
   static boost::thread_specific_ptr<std::ostringstream> tmpostr;
   static boost::thread_specific_ptr<boost::hash<std::string> > hash_fn;

   if (!tmpostr.get())
    tmpostr.reset(new std::ostringstream());
   if(!hash_fn.get())
    hash_fn.reset(new boost::hash<std::string>());

  processCongestMon->report_insert(t.get(), 1);
  tmpostr->str("");
  tmpostr->clear();
  get_dimension_key(*t, current_levels, *tmpostr);
  DimensionKey key = tmpostr->str();
  size_t kh = (*hash_fn)(key);
  processors[kh % processors.size()]->assign(t, key, current_levels);
}

void DataCube::do_process(boost::shared_ptr<Tuple> t, DimensionKey key, boost::shared_ptr<std::vector<unsigned int> > levels, boost::shared_ptr<cube::TupleBatch> &tupleBatcher) {
  bool in_batch = false;

  VLOG(2) << "Processing " << key  << " thread id " << boost::this_thread::get_id();

  if (tupleBatcher->contains(key)) {
    in_batch = true;
  }

  shared_ptr<TupleProcessingInfo> tpi;
  if(!in_batch)
  {
    tpi = make_shared<TupleProcessingInfo>(t, key, levels);
  }
  else
  {
    tpi = tupleBatcher->get(key);
    merge_tuple_into(*(tpi->t), *t);
  }

  bool can_batch = true;
  { //lock
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
  } //lock

  VLOG(2) <<"Process: "<< key << "in batch: "<<in_batch << " can batch:" << can_batch << " need new:" << tpi->need_new_value << " need old:"<< tpi->need_old_value;

  //LOG(INFO) << "In do_process. " << elements_in_batch  ;

  bool was_empty=tupleBatcher->is_empty();

  if(!in_batch) {
    tupleBatcher->insert_tuple(tpi, can_batch);
  }
  else {
    tupleBatcher->update_batched_tuple(tpi, can_batch);
  }

  if(!tupleBatcher->is_empty() && was_empty)
  {
    bool start_flush = (flushCongestMon->queue_length() <= 0);

    //INVARIANT: flushCongestMon contains a count of non-empty, non-flushed TupleBatchers in the system
    //add one for every TB only once when it becomes non-empty.
    flushCongestMon->report_insert(tupleBatcher.get() , 1);

    if(start_flush)
    {
      //queue up flush, because check_flush may not be running
      //INVARIANT: check_flush run after some tuples in tupleBatcher (needs to be posted after insert)
      //also flushCongestMon should be non-0 (so after increment)
      flushExec.submit(boost::bind(&DataCube::check_flush, this));
    }

  }


  processCongestMon->report_delete(t.get(), 1);


}


//runs in queue exec;
void DataCube::check_flush() {
  while(flushCongestMon->queue_length() > 0) {
    if(processors[current_processor]->batcher_ready())
    {
       boost::shared_ptr<cube::TupleBatch> tb = processors[current_processor]->batch_flush();
       VLOG_EVERY_N(1, 1000) << "Flushing processor "<< current_processor << " with size "<< tb->size() << " thread id " << boost::this_thread::get_id()
         << " Current flushCongestMon = " << flushCongestMon->queue_length()
         << " Current processhCongestMon = " << processCongestMon->queue_length();
       tb->flush();
       flushCongestMon->report_delete(tb.get(), 1);
    }
    current_processor = (current_processor+1) % processors.size();
  }

}

void DataCube::wait_for_commits() {
  while(flushCongestMon->queue_length() > 0 || processCongestMon->queue_length() > 0)
  {
    js_usleep(processCongestMon->queue_length() + (flushCongestMon->queue_length()*10));
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

void DataCube::meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred) {
  if( msg.type() == DataplaneMessage::ROLLUP_LEVELS) {
    if(msg.rollup_levels_size() == 0)
    {
      set_current_levels(get_leaf_levels());
    }
    else
    {
      LOG_IF(FATAL, (unsigned int) msg.rollup_levels_size() != num_dimensions()) << "got a rollup levels msg with the wrong number of dimensions: "
        << msg.rollup_levels_size()<< " should be " <<num_dimensions();
      std::vector<unsigned int> levels;
      for(int i = 0; i < msg.rollup_levels_size(); ++i ) {
        levels.push_back(msg.rollup_levels(i));
      }
      set_current_levels(levels);
    }
  }
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

void DataCube::set_current_levels(const std::vector<unsigned int> &levels) {
   current_levels = make_shared<std::vector<unsigned int> >(levels);
}

void DataCube::set_current_levels(boost::shared_ptr<std::vector<unsigned int> > levels) {
   current_levels = levels;
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

