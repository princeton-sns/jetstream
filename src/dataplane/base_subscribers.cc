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

  interval_ms = 1000;
  if (config.find("interval_ms") != config.end())
    interval_ms = boost::lexical_cast<unsigned int>(config["interval_ms"]);

  cumulative = false;
  if (config.find("cumulative") != config.end())
    cumulative = boost::lexical_cast<bool>(config["cumulative"]);

  return NO_ERR;
}

jetstream::cube::Subscriber::Action LatencyMeasureSubscriber::action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update)
{
  lock_guard<boost::mutex> critical_section (lock);
  string hostname = update->e(hostname_tuple_index).s_val();
  map<int, unsigned int> &bucket_map_rt = stats_before_rt[hostname];
  map<int, unsigned int> &bucket_map_skew = stats_before_skew[hostname];
  double tuple_time_ms = update->e(time_tuple_index).d_val();
  
  if(bucket_map_rt.empty()) {
    start_time_ms = get_usec()/1000; 
  }

  make_stats(tuple_time_ms, bucket_map_rt, bucket_map_skew, max_seen_tuple_before_ms);

  return SEND;
}

void LatencyMeasureSubscriber::post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value) {

  lock_guard<boost::mutex> critical_section (lock);
  string hostname = update->e(hostname_tuple_index).s_val();
  map<int, unsigned int> &bucket_map_rt = stats_after_rt[hostname];
  map<int, unsigned int> &bucket_map_skew = stats_after_skew[hostname];
  double tuple_time_ms = update->e(time_tuple_index).d_val();
  
  make_stats(tuple_time_ms, bucket_map_rt, bucket_map_skew, max_seen_tuple_after_ms);
}

void  LatencyMeasureSubscriber::post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value)
{
  LOG(FATAL) << "This subscriber should never get backfill data";
}

int LatencyMeasureSubscriber::get_bucket(int latency) {

  if(abs(latency) < 100) {
    return (latency/10)*10;
  }
  if(abs(latency) <1000) {
    return (latency/100)*100;
  }
  return (latency/1000)*1000;
}

void LatencyMeasureSubscriber::make_stats (msec_t tuple_time_ms,
                                           map<int, unsigned int> &bucket_map_rt,
                                           map<int, unsigned int> &bucket_map_skew,
                                          msec_t& max_seen_tuple_ms) {
  msec_t current_time_ms = get_usec()/1000;
  int latency_rt_ms = current_time_ms-tuple_time_ms; //note that this is SIGNED. Positive means source lags subscriber.
 
  LOG(INFO) << "Latency: " << current_time_ms << " - " << tuple_time_ms << " = " << latency_rt_ms;
  int bucket_rt = get_bucket(latency_rt_ms);
  bucket_map_rt[bucket_rt] += 1;

  if(tuple_time_ms > max_seen_tuple_ms) {
    max_seen_tuple_ms = tuple_time_ms; 
  }
  else { 
    msec_t latency_skew_ms = max_seen_tuple_ms-tuple_time_ms;
    int bucket_skew = get_bucket(latency_skew_ms);
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
    msec_t now = get_usec() / 1000;
    string now_str = lexical_cast<string>(now);

    {
      lock_guard<boost::mutex> critical_section (lock);

  //    std::stringstream line;
  //    line<<"Stats before entry into cube. Wrt real-time" << endl;
      string s = string("before cube insert");
      s += now_str;
      print_stats(stats_before_rt, s.c_str());
      s = string("after cube insert");
      s += now_str;
      print_stats(stats_after_rt, s.c_str());

      if(!cumulative)
      {
        stats_before_rt.clear();
        stats_after_rt.clear();
        stats_before_skew.clear();
        stats_after_skew.clear();
      }
    } //release lock
    js_usleep(interval_ms*1000);
  }
}

void
LatencyMeasureSubscriber::print_stats (std::map<std::string, std::map<int, unsigned int> > & stats,
                                       const char * label) {
  std::map<std::string, std::map<int, unsigned int> >::iterator stats_it;
  for(stats_it = stats.begin(); stats_it != stats.end(); ++stats_it) {
    //unsigned int total=0;
    
    std::map<int, unsigned int>::iterator latency_it;
    for(latency_it = (*stats_it).second.begin(); latency_it != (*stats_it).second.end(); ++latency_it) {
      boost::shared_ptr<jetstream::Tuple> t(new Tuple);
      extend_tuple(*t, stats_it->first);  //the hostname
      extend_tuple(*t, label);
      extend_tuple(*t, (int) latency_it->first);
      extend_tuple(*t, (int) latency_it->second);
//      total += (*latency_it).second;
      emit(t);
    }
    /*
    line << "Total count: " << total <<endl;
    unsigned int med_total = 0;
    for(latency_it = (*stats_it).second.begin(); latency_it != (*stats_it).second.end(); ++latency_it) {
      if(med_total+(*latency_it).second < total/2) {
        med_total += (*latency_it).second;
      }
      else{
        line << "Median Bucket: " << (*latency_it).first<<endl;
        break;
      }
    }
    double current_time_ms = (double)(get_usec()/1000); 
    line << "Analysis time "<< (current_time_ms-start_time_ms)<<endl;*/
  }
}


} //end namespace
