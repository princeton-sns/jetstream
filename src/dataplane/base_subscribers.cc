#include <glog/logging.h>
#include <time.h>

#include <sstream>
#include <string>

#include "timeteller.h"
#include "base_subscribers.h"
#include "js_utils.h"
#include "node.h"
#include "time_containment_levels.h"
#include "cube.h"

using namespace std;
using namespace boost;
using namespace jetstream::cube;

#undef BACKFILL

jetstream::cube::Subscriber::Action QueueSubscriber::action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) {
  return returnAction;
}

void QueueSubscriber::post_insert(boost::shared_ptr<jetstream::Tuple> const &update, boost::shared_ptr<jetstream::Tuple> const &new_value) {
  insert_q.push_back(new_value);
}

void QueueSubscriber::post_update(boost::shared_ptr<jetstream::Tuple> const &update,boost::shared_ptr<jetstream::Tuple> const &new_value, boost::shared_ptr<jetstream::Tuple> const &old_value) {
  update_q.push_back(new_value);
}

namespace jetstream {


void
ThreadedSubscriber::start() {

  if (!has_cube()) {
    LOG(ERROR) << "No cube for subscriber " << id() << " aborting";
    return;
  }

  running = true;
  querier.set_cube(cube);
  loopThread = shared_ptr<boost::thread>(new boost::thread(boost::ref(*this)));
}

operator_err_t
OneShotSubscriber::configure(std::map<std::string,std::string> &config) {
  return querier.configure(config, id());
}


void
OneShotSubscriber::operator()() {

  cube::CubeIterator it = querier.do_query();

  while ( it != cube->end()) {
    emit(*it);
    it++;
  }

  no_more_tuples();
  node->stop_operator(id());
}

cube::Subscriber::Action
TimeBasedSubscriber::action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) {
  //update->add_e()->set_i_val(get_msec());
  if (ts_input_tuple_index >= 0) {
    time_t tuple_time = update->e(ts_input_tuple_index).t_val();
    LOG_IF_EVERY_N(INFO, tuple_time < next_window_start_time, 10001) 
      << "(every 10001) TimeBasedSubscriber before db next_window_start_time: "<< next_window_start_time 
      <<" tuple time being processed: " << tuple_time <<" diff (>0 is good): "<< (tuple_time-next_window_start_time)
      <<" Process q: "<< cube->process_congestion_monitor()->queue_length();


    if(latency_ts_field >= 0) {
      msec_t orig_time=  update->e(latency_ts_field).d_val();
      msec_t now = get_msec();

      LOG_IF_EVERY_N(INFO, (now > orig_time) && ((now-orig_time) > 1000) , 10001)<< "(every 10001) HIGH LATENCY in action_on_tuple: "<<(now - orig_time) << " index "<<latency_ts_field << " orig_time "<< orig_time << " now "<< now;
    }

    if (tuple_time < next_window_start_time) {
      backfill_tuples ++;
      last_backfill_time = tuple_time;
      return SEND_UPDATE;
    }
    else {
      regular_tuples++;
    }
  }

  return SEND;
}

void
TimeBasedSubscriber::post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value) {
  if (ts_input_tuple_index >= 0) {
    time_t tuple_time = update->e(ts_input_tuple_index).t_val();
      VLOG_EVERY_N(1, 10001) << "(every 10001) TimeBasedSubscriber after db insert next_window_start_time: "
      << next_window_start_time <<" tuple time being processed: " << tuple_time 
      <<" diff (>0 is good): "<< (tuple_time-next_window_start_time) 
      <<" Process q: "<< cube->process_congestion_monitor()->queue_length();

    if (tuple_time < next_window_start_time) {
      LOG_EVERY_N(INFO, 1001) << id() << "DANGEROUS CASE (every 1001): tuple was supposed to be insert but is actually a backfill. Tuple time: "<< tuple_time 
        << ". Next window: " << next_window_start_time << ". Diff: "<< (next_window_start_time-tuple_time)
        <<" Window Offset(s): "<< get_window_offset_sec() << " Process q: "<< cube->process_congestion_monitor()->queue_length();

      if(latency_ts_field >= 0) {
        msec_t orig_time=  update->e(latency_ts_field).d_val();
        LOG(INFO)<< "Latency of Dangerous Case Tuple "<<(get_msec() - orig_time) <<" Orig time "<<orig_time;
      }
    }

  }

}

//called on backfill
void
TimeBasedSubscriber::post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value) {
  if (ts_input_tuple_index >= 0) {
    time_t tuple_time = update->e(ts_input_tuple_index).t_val();
    LOG_EVERY_N(INFO, 10001) << "(every 10001) TimeBasedSubscriber after db update next_window_start_time: "<< next_window_start_time <<" tuple time being processed: " << tuple_time <<" diff (>0 is good): "<< (tuple_time-next_window_start_time);
  }

  #ifdef BACKFILL
  boost::shared_ptr<jetstream::Tuple> new_to_propagate(new Tuple);
  new_to_propagate->CopyFrom(*new_value);
  
  Tuple old_to_propagate;
  //this sometimes is wrong because old_value may not be set
  old_to_propagate.CopyFrom(*old_value);
  emit(old_to_propagate, new_to_propagate);
  #endif
}

operator_err_t
TimeBasedSubscriber::configure(std::map<std::string,std::string> &config) {
  operator_err_t r = querier.configure(config, id());

  if (r != NO_ERR)
    return r;

  if (config.find("window_size") != config.end())
    windowSizeMs = boost::lexical_cast<time_t>(config["window_size"]);
  else
    windowSizeMs = 1000;

  start_ts = time(NULL); //now

  if (config.find("start_ts") != config.end())
    start_ts = boost::lexical_cast<time_t>(config["start_ts"]);

  if (config.find("ts_field") != config.end()) {
    ts_field = boost::lexical_cast<int32_t>(config["ts_field"]);

    if (querier.min.e_size() <= ts_field) {
      ostringstream of;
      of << "can't use field " << ts_field << " as time; input only has "
         << querier.min.e_size() << " fields";
      return of.str();
    }

    querier.min.mutable_e(ts_field)->set_t_val(start_ts);

  }
  else
    ts_field = -1;

  if (config.find("latency_ts_field") != config.end()) {
    latency_ts_field = boost::lexical_cast<int32_t>(config["latency_ts_field"]);
    /* if (querier.min.e_size() <= latency_ts_field) {
       ostringstream of;
       of << "can't use field " << latency_ts_field << " as time; input only has "
          << querier.min.e_size() << " fields";
       return of.str();
     }*/
  }
  else
    latency_ts_field = -1;

//    return operator_err_t("Must specify start_ts field");

  windowOffsetMs = DEFAULT_WINDOW_OFFSET;

  if ((config["window_offset"].length() > 0) &&
      !(stringstream(config["window_offset"]) >> windowOffsetMs)) {
    return operator_err_t("window_offset must be a number");
  }


  simulation = false;
  simulation_rate = -1;

  if (config.find("simulation_rate") != config.end()) {
    simulation_rate = boost::lexical_cast<time_t>(config["simulation_rate"]);

    LOG(INFO) << "configuring a TimeSubscriber simulation" << endl;
    windowOffsetMs *= simulation_rate;
    simulation = true;

    VLOG(1) << "TSubscriber simulation start: " << start_ts << endl;
    VLOG(1) << "TSubscriber simulation rate: " << simulation_rate << endl;
    VLOG(1) << "TSubscriber window size ms: " << windowSizeMs << endl;
  }

  return NO_ERR;
}

double shouldRunArr[] = {0, 1};

void
TimeBasedSubscriber::respond_to_congestion() {
  int should_run = 1;

  while(running && should_run == 0) {
    should_run += congest_policy->get_step(id(), shouldRunArr, 2, should_run);
    js_usleep(1000 * 50);  //10 ms
  }
}

string ts_to_str(time_t t) {
  struct tm parsed_time;
  gmtime_r(&t, &parsed_time);

  char tmbuf[80];
  strftime(tmbuf, sizeof(tmbuf), "%H:%M:%S", &parsed_time);
  return string(tmbuf);
}

unsigned int TimeBasedSubscriber::get_window_offset_sec() {
  unsigned int level_offset_sec = 0;
  if(querier.rollup_levels.size() > 0) {
    size_t time_level = querier.rollup_levels[ts_field];
//    LOG_IF(FATAL, time_level >= DTC_LEVEL_COUNT) << "unknown rollup level " << time_level;

    if(time_level < DTC_LEVEL_COUNT-1) { //not a leaf
      level_offset_sec = DTC_SECS_PER_LEVEL[time_level];
      level_offset_sec = min(3600U, level_offset_sec);
    }
  }
    //either not rolled up, or else rolled up completely
  return (windowOffsetMs + 999) / 1000+level_offset_sec; //TODO could instead offset from highest-ts-seen

}


void
TimeBasedSubscriber::update_backfill_stats(int elems) {
  int backfill_window = backfill_tuples - backfill_old_window;
  int regular_window = regular_tuples - regular_old_window;

  if(backfill_window > 0) {
    LOG(INFO)<< id() << ": Backfill in window (at action_on_tuple): " << backfill_window <<". Non-Backfill: "<<regular_window
             <<". Next window start time = "<< next_window_start_time<< ". Last backfill was at: " << last_backfill_time 
             <<" Process queue length: "<< cube->process_congestion_monitor()->queue_length();

  }

  backfill_old_window = backfill_tuples;
  regular_old_window = regular_tuples;

  VLOG(1) << id() << " read " << elems << " tuples from cube. Total backfill: " << backfill_tuples << " Total regular: "<<regular_tuples;
}


void
TimeBasedSubscriber::operator()() {

  TimeTeller * ts;

  if (simulation) {
    ts = new TimeSimulator(start_ts, simulation_rate);
  }
  else
    ts = new TimeTeller();

  boost::shared_ptr<TimeTeller> tt(ts);

  time_t newMax = tt->now() - get_window_offset_sec(); 

  if (ts_field >= 0)
    querier.max.mutable_e(ts_field)->set_t_val(newMax);

  int slice_fields = querier.min.e_size();
  int cube_dims = cube->get_schema().dimensions_size();

  if(ts_field >= 0)
    ts_input_tuple_index = cube->get_schema().dimensions(ts_field).tuple_indexes(0);
  else
    ts_input_tuple_index = ts_field;

  if (slice_fields != cube_dims) {
    LOG(FATAL) << id() << " trying to query " << cube_dims << " dimensions with tuple "
               << fmt(querier.min) << " of length " << slice_fields;
  }

  LOG(INFO) << id() << " is attached to " << cube->id_as_str();

  DataplaneMessage end_msg;
  end_msg.set_type(DataplaneMessage::END_OF_WINDOW);
  send_rollup_levels();


  while (running)  {

    cube::CubeIterator it = querier.do_query();

    if(it == cube->end()) {
      VLOG(1) << id() << ": Nothing found in time subscriber query. Next window start time = "<< next_window_start_time
                <<" Cube monitor capacity ratio: " << cube->congestion_monitor()->capacity_ratio();
    }

    size_t elems = 0;

    while ( it != cube->end()) {
      emit(*it);
      it++;
      elems ++;
    }

    end_msg.set_window_length_ms(windowSizeMs);
    send_meta_downstream(end_msg);

    update_backfill_stats(elems);
    js_usleep(1000 * windowSizeMs);
    respond_to_congestion(); //do this BEFORE updating window. It may sleep, changing time.

    if (ts_field >= 0) {
      next_window_start_time = querier.max.e(ts_field).t_val() + 1;
      querier.min.mutable_e(ts_field)->set_t_val(next_window_start_time);
      newMax = tt->now() - get_window_offset_sec(); //TODO could instead offset from highest-ts-seen
      querier.max.mutable_e(ts_field)->set_t_val(newMax);
      VLOG(1) << id() << "Updated query times to "<< ts_to_str(next_window_start_time)
        << "-" << ts_to_str(newMax);
    }
    // else leave next_window_start_time as 0; data is never backfill because we always send everything

    if (!get_dest()) {
      LOG(WARNING) << "Subscriber " << id() << " exiting because no successor.";
      running = false;
    }
  }

  no_more_tuples();
  LOG(INFO) << "Subscriber " << id() << " exiting. Emitted " << emitted_count()
            << ". Total backfill tuple count " << backfill_tuples <<". Total non-backfill tuples " << regular_tuples;
}


std::string
TimeBasedSubscriber::long_description() {
//  boost::lock_guard<boost::mutex> lock (mutex);

  ostringstream out;
  out << backfill_tuples << " late tuples; sample interval " << windowSizeMs << " ms";
  return out.str();

}



void
TimeBasedSubscriber::send_rollup_levels() {
  DataplaneMessage msg;
  msg.set_type(DataplaneMessage::ROLLUP_LEVELS);
  querier.set_rollup_levels(msg);
  send_meta_downstream(msg);
}

void
VariableCoarseningSubscriber::respond_to_congestion() {
//  int prev_level = cur_level;
  int delta = congest_policy->get_step(id(), rollup_data_ratios.data(), rollup_data_ratios.size(), cur_level);
  
  
  if (delta != 0) {
    cur_level += delta;
    int change_in_window = rollup_time_periods[cur_level] * 1000 - windowSizeMs;
    int prev_window = windowSizeMs;
    windowSizeMs = rollup_time_periods[cur_level] * 1000;
//    congest_policy->set_effect_delay(id(), 2 * windowSizeMs);
    LOG(INFO) << "Subscriber " << id() << " switching to period " << windowSizeMs
              << " from " << prev_window;
    if (ts_field >= 0) {
      querier.set_rollup_level(ts_field, cur_level + (DTC_LEVEL_COUNT - rollup_data_ratios.size() ));
      send_rollup_levels();
    }
    if (change_in_window > 0)
      js_usleep( 1000 * change_in_window);
  } else {
    VLOG(1) << "Subscriber " << id() << " staying at period " << windowSizeMs;
  }
}

operator_err_t
VariableCoarseningSubscriber::configure(std::map<std::string,std::string> &config) {
  operator_err_t base_err = TimeBasedSubscriber::configure(config);

  if (base_err != NO_ERR)
    return base_err;

//  if (ts_field < 0)
//    return operator_err_t("time field is mandatory for variable coarsening for now");

  unsigned max_window_size = 60 * 5; // default to no more than once every five minutes
  if ((config["max_window_size"].length() > 0) &&
      !(stringstream(config["max_window_size"]) >> max_window_size)) {
    return operator_err_t("max_window_size must be a number");
  }  
  
  for (unsigned int i = 0; i < DTC_LEVEL_COUNT; ++i) {
    if ( DTC_SECS_PER_LEVEL[i] <= max_window_size) {
      rollup_data_ratios.push_back( 1.0 / DTC_SECS_PER_LEVEL[i]);
      rollup_time_periods.push_back(DTC_SECS_PER_LEVEL[i]);
    }
  }
  
  cur_level = rollup_data_ratios.size() -1;

  return NO_ERR;
}


const string TimeBasedSubscriber::my_type_name("Timer-based subscriber");
const string VariableCoarseningSubscriber::my_type_name("Variable time-based subscriber");


} //end namespace
