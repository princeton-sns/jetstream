#include <glog/logging.h>
#include <time.h>

#include <sstream>
#include <string>


#include "base_subscribers.h"
#include "js_utils.h"
#include "node.h"

using namespace std;
using namespace boost;
using namespace jetstream::cube;

jetstream::cube::Subscriber::Action QueueSubscriber::action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) {
  return returnAction;
}

void QueueSubscriber::insert_callback(boost::shared_ptr<jetstream::Tuple> const &update, boost::shared_ptr<jetstream::Tuple> const &new_value) {
  insert_q.push_back(new_value);
}

void QueueSubscriber::update_callback(boost::shared_ptr<jetstream::Tuple> const &update,boost::shared_ptr<jetstream::Tuple> const &new_value, boost::shared_ptr<jetstream::Tuple> const &old_value) {
  update_q.push_back(new_value);
}

jetstream::cube::Subscriber::Action UnionSubscriber::action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) {
  return SEND;
}

void UnionSubscriber::insert_callback(boost::shared_ptr<jetstream::Tuple> const &update, boost::shared_ptr<jetstream::Tuple> const &new_value) {
  emit(update);
}

void UnionSubscriber::update_callback(boost::shared_ptr<jetstream::Tuple> const &update,boost::shared_ptr<jetstream::Tuple> const &new_value, boost::shared_ptr<jetstream::Tuple> const &old_value) {
  LOG(FATAL)<<"Should never be used";  
}


namespace jetstream {

cube::Subscriber::Action
TimeBasedSubscriber::action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) {

  time_t tuple_time = update->e(ts_field).t_val();
  if (tuple_time < next_window_start_time) {
    backfill_tuples ++;
    return SEND_UPDATE;
  } else
    return NO_SEND;
}

void
TimeBasedSubscriber::insert_callback(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value) {
	; 
}

//called on backfill
void
TimeBasedSubscriber::update_callback(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value, 
                                 boost::shared_ptr<jetstream::Tuple> const &old_value) {
  ;
}

operator_err_t
TimeBasedSubscriber::configure(std::map<std::string,std::string> &config) {

  windowOffsetMs = DEFAULT_WINDOW_OFFSET;
  if ((config["window_offset"].length() > 0) &&
    !(stringstream(config["window_offset"]) >> windowOffsetMs)) {
    
   return operator_err_t("window_offset must be a number");
  }


  if (config.find("window_size") != config.end())
    windowSizeMs = boost::lexical_cast<time_t>(config["window_size"]);
  else
    windowSizeMs = 1000;

  string serialized_slice = config["slice_tuple"];
  min.ParseFromString(serialized_slice);
  max.CopyFrom(min);

  time_t start_ts = time(NULL); //now
  if (config.find("start_ts") != config.end())
    start_ts = boost::lexical_cast<time_t>(config["start_ts"]);
  
  if (config.find("ts_field") != config.end()) {
    ts_field = boost::lexical_cast<int32_t>(config["ts_field"]);
    min.mutable_e(ts_field)->set_t_val(start_ts);

  } else
    ts_field = -1;
//    return operator_err_t("Must specify start_ts field");

  num_results = 0;
  if (config.find("num_results") != config.end()) 
    num_results = boost::lexical_cast<int32_t>(config["num_results"]);
  
  if (config.find("sort_order") != config.end()) {
    std::stringstream ss(config["sort_order"]);
    std::string item;
    while(std::getline(ss, item, ',')) {
        sort_order.push_back(item);
    }
  }
  return NO_ERR;
}


void
TimeBasedSubscriber::start() {
  running = true;
  loopThread = shared_ptr<boost::thread>(new boost::thread(boost::ref(*this)));
}

void 
TimeBasedSubscriber::operator()() {
  time_t newMax = time(NULL) - (windowOffsetMs + 999) / 1000; //can be cautious here since it's just first window
  if (ts_field >= 0)
    max.mutable_e(ts_field)->set_t_val(newMax);
  
  int slice_fields = min.e_size();
  int cube_dims = cube->get_schema().dimensions_size();
  
  if (slice_fields != cube_dims) {
    LOG(FATAL) << id() << " trying to query " << cube_dims << "dimensions with a tuple of length " << slice_fields;
  }
  LOG(INFO) << id() << " is attached to " << cube->id_as_str();
  boost::shared_ptr<CongestionMonitor> congested = congestion_monitor();
  
  while (running)  {
    LOG(INFO) << id() << " doing query; range is " << fmt(min) << " to " << fmt(max);
    cube::CubeIterator it = cube->slice_query(min, max, true, sort_order, num_results);
    while ( it != cube->end()) {
      emit(*it);
      it++;      
    }

    boost::this_thread::sleep(boost::posix_time::milliseconds(windowSizeMs));
    while (congested->is_congested()) {
      boost::this_thread::yield();
      boost::this_thread::sleep(boost::posix_time::milliseconds(100));
    }

    
    if (ts_field >= 0) {
      next_window_start_time = max.e(ts_field).t_val();
      min.mutable_e(ts_field)->set_t_val(next_window_start_time + 1);
      newMax = time(NULL) - (windowOffsetMs + 999) / 1000; //TODO could instead offset from highest-ts-seen
      max.mutable_e(ts_field)->set_t_val(newMax);
    }
      //else leave next_window_start_time as 0; data is never backfill because we always send everything
  }
}


std::string
TimeBasedSubscriber::long_description() {
//  boost::lock_guard<boost::mutex> lock (mutex);

  ostringstream out;
  out << backfill_tuples << " late tuples";
  return out.str();

}


const string TimeBasedSubscriber::my_type_name("Timer-based subscriber");


} //end namespace
