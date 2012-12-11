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

void QueueSubscriber::post_insert(boost::shared_ptr<jetstream::Tuple> const &update, boost::shared_ptr<jetstream::Tuple> const &new_value) {
  insert_q.push_back(new_value);
}

void QueueSubscriber::post_update(boost::shared_ptr<jetstream::Tuple> const &update,boost::shared_ptr<jetstream::Tuple> const &new_value, boost::shared_ptr<jetstream::Tuple> const &old_value) {
  update_q.push_back(new_value);
}

jetstream::cube::Subscriber::Action UnionSubscriber::action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) {
  return SEND;
}

void UnionSubscriber::post_insert(boost::shared_ptr<jetstream::Tuple> const &update, boost::shared_ptr<jetstream::Tuple> const &new_value) {
  emit(update);
}

void UnionSubscriber::post_update(boost::shared_ptr<jetstream::Tuple> const &update,boost::shared_ptr<jetstream::Tuple> const &new_value, boost::shared_ptr<jetstream::Tuple> const &old_value) {
  LOG(FATAL)<<"Should never be used";  
}


namespace jetstream {

cube::Subscriber::Action
TimeBasedSubscriber::action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) {

  if (ts_field >= 0) {
    time_t tuple_time = update->e(ts_field).t_val();
    if (tuple_time < next_window_start_time) {
      backfill_tuples ++;
      return SEND_UPDATE;
    }
  }
  return NO_SEND;
}

void
TimeBasedSubscriber::post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value) {
	; 
}

//called on backfill
void
TimeBasedSubscriber::post_update(boost::shared_ptr<jetstream::Tuple> const &update,
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
  
    if(congested->is_congested()) {
      js_usleep(1000 * 10);
      continue;
    }
  
    VLOG(1) << id() << " doing query; range is " << fmt(min) << " to " << fmt(max);
    cube::CubeIterator it = cube->slice_query(min, max, true, sort_order, num_results);
    while ( it != cube->end()) {
      emit(*it);
      it++;      
    }

    js_usleep(1000 * windowSizeMs);
  
    if (ts_field >= 0) {
      next_window_start_time = max.e(ts_field).t_val();
      min.mutable_e(ts_field)->set_t_val(next_window_start_time + 1);
      newMax = time(NULL) - (windowOffsetMs + 999) / 1000; //TODO could instead offset from highest-ts-seen
      max.mutable_e(ts_field)->set_t_val(newMax);
    }
      //else leave next_window_start_time as 0; data is never backfill because we always send everything
    if (!get_dest()) {
      LOG(WARNING) << "Subscriber " << id() << " exiting because no successor.";
      running = false;
    }
  }
  
  LOG(INFO) << "Subscriber " << id() << " exiting. Emitted " << emitted_count()
      << ". Total backfill tuple count " << backfill_tuples;
}


std::string
TimeBasedSubscriber::long_description() {
//  boost::lock_guard<boost::mutex> lock (mutex);

  ostringstream out;
  out << backfill_tuples << " late tuples; sample interval " << windowSizeMs << " ms";
  return out.str();

}


const string TimeBasedSubscriber::my_type_name("Timer-based subscriber");



operator_err_t
LatencyMeasureSubscriber::configure(std::map<std::string,std::string> &config) {

  if ((config["time_tuple_index"].length() < 1) ||
      !(stringstream(config["time_tuple_index"]) >> time_tuple_index)) {

    return operator_err_t("must have a numeric time tuple index");
  }

  if ((config["hostname_tuple_index"].length() < 1) ||
      !(stringstream(config["hostname_tuple_index"]) >> hostname_tuple_index)) {

    return operator_err_t("must have a numeric hostname tuple index");
  }

  bucket_size_ms=100;

  if (config.find("bucket_size_ms") != config.end())
    bucket_size_ms = boost::lexical_cast<unsigned int>(config["bucket_size_ms"]);

  return NO_ERR;
}

jetstream::cube::Subscriber::Action LatencyMeasureSubscriber::action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update)
{
  string hostname = update->e(hostname_tuple_index).s_val();
  map<unsigned int, unsigned int> &bucket_map_rt = stats_before_rt[hostname];
  map<unsigned int, unsigned int> &bucket_map_skew = stats_before_skew[hostname];
  double tuple_time_ms = update->e(time_tuple_index).d_val();
  
  make_stats(tuple_time_ms, bucket_map_rt, bucket_map_skew, max_seen_tuple_before_ms);

  return SEND;
}

void LatencyMeasureSubscriber::post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value) {

  string hostname = update->e(hostname_tuple_index).s_val();
  map<unsigned int, unsigned int> &bucket_map_rt = stats_after_rt[hostname];
  map<unsigned int, unsigned int> &bucket_map_skew = stats_after_skew[hostname];
  double tuple_time_ms = update->e(time_tuple_index).d_val();
  
  make_stats(tuple_time_ms, bucket_map_rt, bucket_map_skew, max_seen_tuple_after_ms);
}

void  LatencyMeasureSubscriber::post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value)
{
  assert(0);
}

unsigned int LatencyMeasureSubscriber::get_bucket(double latency) {
  assert(latency >= 0);

  if(latency < 100)
  {
    return (unsigned int)  (latency/10)*10;
  }
  if(latency <1000)
  {
    return (unsigned int)  (latency/100)*100;
  }
  return (unsigned int)  (latency/1000)*1000;
}

void LatencyMeasureSubscriber::make_stats(double tuple_time_ms,  map<unsigned int, unsigned int> &bucket_map_rt,
     map<unsigned int, unsigned int> &bucket_map_skew, double& max_seen_tuple_ms)
{
  double current_time_ms = (double)(get_usec()/1000); 
  double latency_rt_ms = current_time_ms-tuple_time_ms;
 
  //LOG(INFO) << "Latency: " << current_time_ms << " - " << tuple_time_ms << " = " << latency_rt_ms;
  unsigned int bucket_rt = get_bucket(latency_rt_ms);
  bucket_map_rt[bucket_rt] += 1;

  if(tuple_time_ms > max_seen_tuple_ms) {
    max_seen_tuple_ms = tuple_time_ms; 
  }
  else { 
    double latency_skew_ms = max_seen_tuple_ms-tuple_time_ms;
    unsigned int bucket_skew = get_bucket(latency_skew_ms);
    bucket_map_skew[bucket_skew] += 1;
  }
}

void
LatencyMeasureSubscriber::start() {
  running = true;
  loopThread = shared_ptr<boost::thread>(new boost::thread(boost::ref(*this)));
}

void 
LatencyMeasureSubscriber::operator()() {
  while (running)  {
    std::stringstream line;
    line<<"Stats before entry into cube. Wrt real-time" << endl;
    print_stats(stats_before_rt, line);
    line<<"Stats after entry into cube. Wrt real-time" << endl;
    print_stats(stats_after_rt, line);

    boost::shared_ptr<jetstream::Tuple> tuple(new jetstream::Tuple());
    tuple->add_e()->set_s_val(line.str());

    emit(tuple);
    js_usleep(1000000);
  }
}

void LatencyMeasureSubscriber::print_stats(std::map<std::string, std::map<unsigned int, unsigned int> > & stats,  std::stringstream& line)
{
  std::map<std::string, std::map<unsigned int, unsigned int> >::iterator stats_it;
  for(stats_it = stats.begin(); stats_it != stats.end(); ++stats_it)
  {
    line << "Hostname: "<< (*stats_it).first <<endl;
    std::map<unsigned int, unsigned int>::iterator latency_it;
    for(latency_it = (*stats_it).second.begin(); latency_it != (*stats_it).second.end(); ++latency_it) {
      line<< "Latency: " << (*latency_it).first<< " count: " << (*latency_it).second<<endl;
    }
  }
}


} //end namespace
