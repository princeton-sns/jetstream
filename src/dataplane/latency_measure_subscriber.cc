#include <glog/logging.h>
#include <time.h>

#include <sstream>
#include <string>

#include "latency_measure_subscriber.h"

using namespace ::std;
using namespace jetstream;
using namespace boost;


void
LatencyMeasureSubscriber::start() {
  //overwrite to handle n-cube case
  if (!has_cube()) {
    running = true;
    return;
  }

  running = true;
  querier.set_cube(cube);
}



operator_err_t
LatencyMeasureSubscriber::configure(std::map<std::string,std::string> &config) {

  if ((config["time_tuple_index"].length() < 1) ||
      !(stringstream(config["time_tuple_index"]) >> time_tuple_index)) {

    return operator_err_t("must have a numeric index for time field in tuples");
  }

  if ((config["hostname_tuple_index"].length() < 1) ||
      !(stringstream(config["hostname_tuple_index"]) >> hostname_tuple_index)) {

    return operator_err_t("must have a numeric index for source-hostname in tuples");
  }

  interval_ms = 1000;

  if (config.find("interval_ms") != config.end())
    interval_ms = boost::lexical_cast<unsigned int>(config["interval_ms"]);

  cumulative = false;

  if (config.find("cumulative") != config.end())
    cumulative = boost::lexical_cast<bool>(config["cumulative"]);

  return NO_ERR;
}

jetstream::cube::Subscriber::Action
LatencyMeasureSubscriber::action_on_tuple( OperatorChain * c,
                                          boost::shared_ptr<const jetstream::Tuple> const update) {
  process_c(update);
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


void
LatencyMeasureSubscriber::process_c(boost::shared_ptr<const Tuple> update) {
  lock_guard<boost::mutex> critical_section (lock);
  string hostname = update->e(hostname_tuple_index).s_val();
  map<int, unsigned int> &bucket_map_rt = stats_before_rt[hostname];
  map<int, unsigned int> &bucket_map_skew = stats_before_skew[hostname];
  double tuple_time_ms = update->e(time_tuple_index).d_val();

  if(bucket_map_rt.empty()) {
    start_time_ms = get_usec()/1000;
  }
  make_stats(tuple_time_ms, bucket_map_rt, bucket_map_skew, max_seen_tuple_before_ms);
  
}

void  LatencyMeasureSubscriber::post_update(boost::shared_ptr<jetstream::Tuple> const &update,
    boost::shared_ptr<jetstream::Tuple> const &new_value,
    boost::shared_ptr<jetstream::Tuple> const &old_value) {
  LOG(FATAL) << "This subscriber should never get backfill data";
}

int LatencyMeasureSubscriber::get_bucket(int latency) {

  if (abs(latency) < 10)
    return (latency / 2) * 2;

  if(abs(latency) < 100) {
    return (latency/10)*10;
  }

  if(abs(latency) <1000) {
    return (latency/20)*20;
  }

  return (latency/100)*100;
}

void LatencyMeasureSubscriber::make_stats (msec_t tuple_time_ms,
    map<int, unsigned int> &bucket_map_rt,
    map<int, unsigned int> &bucket_map_skew,
    msec_t& max_seen_tuple_ms) {
  msec_t current_time_ms = get_usec()/1000;
  int latency_rt_ms = current_time_ms-tuple_time_ms; //note that this is SIGNED. Positive means source lags subscriber.

//  LOG(INFO) << "Latency: " << current_time_ms << " - " << tuple_time_ms << " = " << latency_rt_ms;
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


int
LatencyMeasureSubscriber::emit_batch() {
  msec_t now = get_usec() / 1000;
  string now_str = lexical_cast<string>(now);

  {
    lock_guard<boost::mutex> critical_section (lock);

    //    std::stringstream line;
    //    line<<"Stats before entry into cube. Wrt real-time" << endl;
    string s = string("before cube insert ");
    s += now_str;
    print_stats(stats_before_rt, s.c_str());
    s = string("after cube insert ");
    s += now_str;
    print_stats(stats_after_rt, s.c_str());

    if(!cumulative) {
      stats_before_rt.clear();
      stats_after_rt.clear();
      stats_before_skew.clear();
      stats_after_skew.clear();
    }
  } //release lock
  return interval_ms;
}

void
LatencyMeasureSubscriber::print_stats (std::map<std::string, std::map<int, unsigned int> > & stats,
                                       const char * label) {
  std::map<std::string, std::map<int, unsigned int> >::iterator stats_it;

  for(stats_it = stats.begin(); stats_it != stats.end(); ++stats_it) {
    //unsigned int total=0;

    std::map<int, unsigned int>::iterator latency_it;

    vector< boost::shared_ptr<jetstream::Tuple> > this_window_stats;

    for(latency_it = (*stats_it).second.begin(); latency_it != (*stats_it).second.end(); ++latency_it) {
      boost::shared_ptr<jetstream::Tuple> t(new Tuple);
      extend_tuple(*t, stats_it->first);  //the hostname
      extend_tuple(*t, label);
      extend_tuple(*t, (int) latency_it->first);
      extend_tuple(*t, (int) latency_it->second);
//      total += (*latency_it).second;
      this_window_stats.push_back(t);
    }
    chain->process(this_window_stats);
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
