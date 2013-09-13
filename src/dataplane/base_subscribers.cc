#include <glog/logging.h>
#include <time.h>

#include <sstream>
#include <string>
#include <limits>

#include "timeteller.h"
#include "base_subscribers.h"
#include "js_utils.h"
#include "node.h"
#include "time_containment_levels.h"
#include "cube.h"

using std::string;
using std::vector;
using std::endl;
using std::map;
using std::ostringstream;
using std::pair;
using namespace boost;
using namespace jetstream::cube;

#undef BACKFILL

jetstream::cube::Subscriber::Action
QueueSubscriber::action_on_tuple(OperatorChain * c,
                                 boost::shared_ptr<const jetstream::Tuple> const update) {
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
StrandedSubscriber::start() {

  if (!has_cube()) {
    LOG(ERROR) << "No cube for subscriber " << id() << " aborting";
    return;
  }
  
  int slice_fields = querier.min.e_size();
  int cube_dims = cube->get_schema().dimensions_size();

  if (slice_fields != cube_dims) {
    LOG(FATAL) << id() << " trying to query " << cube_dims << " dimensions with tuple "
               << fmt(querier.min) << " of length " << slice_fields;
    return;
  }


  running = true;
  querier.set_cube(cube);
  timer = node->get_timer();
  st = node->get_new_strand();
  chain->strand = st.get();
  timer->expires_from_now(boost::posix_time::seconds(0));
  timer->async_wait(st->wrap(boost::bind(&StrandedSubscriber::emit_wrapper, this)));  
}

StrandedSubscriber::~StrandedSubscriber() {
  LOG(INFO) << "Deleting stranded subscriber " << id();
  //FIXME do we cancel the timer here?
}



void
StrandedSubscriber::emit_wrapper() {
  if (running) {
    int delay_to_next = emit_batch();
    if (delay_to_next >= 0) {
      timer->expires_from_now(boost::posix_time::millisec(delay_to_next));
      timer->async_wait(st->wrap(boost::bind(&StrandedSubscriber::emit_wrapper, this)));
      return;
    }
  }
    //either !running and we were invoked by the timer-cancel or else a delay_to_next == -1
  LOG(INFO)<< typename_as_str() << " " << id() << " exiting; should tear down";
  if (chain) {
    shared_ptr<OperatorChain> c(chain);
    chain->stop_from_within();
    node->unregister_chain(c);
    c.reset();
  }
  cube->remove_subscriber(id());// will trigger destructor for this!
}

void
StrandedSubscriber::stop_from_subscriber() {
  running = false;
  timer->cancel();
}
  
operator_err_t
OneShotSubscriber::configure(std::map<std::string,std::string> &config) {
  return querier.configure(config, id());
}

const unsigned TUPLES_PER_BUFFER = 1000;
int
OneShotSubscriber::emit_batch() {

  cube::CubeIterator it = querier.do_query();

  LOG(INFO) << "one-shot querier found " << it.numCells() <<  " cells";
  vector< boost::shared_ptr<Tuple> > buffer;
  buffer.reserve(TUPLES_PER_BUFFER);
//  DataplaneMessage no_msg;
  while ( it != cube->end()) {
    buffer.push_back( *it);
    it++;
    if (buffer.size() >= TUPLES_PER_BUFFER) {
      chain->process(buffer);
      buffer.clear();
    }
  }

  if (buffer.size() > 0)
    chain->process(buffer);

  return -1;
}

const int BACKFILL_LOG_FREQ = 10;
cube::Subscriber::Action
TimeBasedSubscriber::action_on_tuple(OperatorChain * c, boost::shared_ptr<const jetstream::Tuple> const update) {
  //update->add_e()->set_i_val(get_msec());
  if (ts_input_tuple_index >= 0) {
    time_t tuple_time = update->e(ts_input_tuple_index).t_val();
    LOG_IF_EVERY_N(INFO, tuple_time < next_window_start_time, BACKFILL_LOG_FREQ) 
      << "(every " << BACKFILL_LOG_FREQ << ") "<< id_as_str() << " before db next_window_start_time: "<< next_window_start_time
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
      LOG_EVERY_N(INFO, 5) << id() << "DANGEROUS CASE (every 5): tuple was supposed to be insert but is actually a backfill. Tuple time: "<< tuple_time
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

  if (config.find("window_size") != config.end()) {
    if( !(std::stringstream(config["window_size"]) >> windowSizeMs))
      return operator_err_t("windowSizeMs must be an int");
  }
  else
    windowSizeMs = 1000;

  start_ts = time(NULL); //now

  if (config.find("start_ts") != config.end()) {
    if( !(std::stringstream(config["start_ts"]) >> start_ts))
      return operator_err_t("start_ts must be an int");
    start_ts = boost::lexical_cast<time_t>(config["start_ts"]);
  }

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
      !(std::stringstream(config["window_offset"]) >> windowOffsetMs)) {
    return operator_err_t("window_offset must be a number");
  }


  int simulation_rate = -1;

  if (config.find("simulation_rate") != config.end()) {
    simulation_rate = boost::lexical_cast<time_t>(config["simulation_rate"]);

    windowOffsetMs *= simulation_rate;
    tt = boost::shared_ptr<TimeTeller>(new TimeSimulator(start_ts, simulation_rate));

    LOG(INFO) << "configuring a TimeSubscriber simulation for " << id() <<". Starting at "<< start_ts<<
     " and rate is " << simulation_rate;
    VLOG(1) << "TSubscriber window size ms: " << windowSizeMs << endl;
  } 
  else {
    tt = boost::shared_ptr<TimeTeller>(new TimeTeller);
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
      level_offset_sec = std::min(3600U, level_offset_sec);
    }
  }
    //either not rolled up, or else rolled up completely
  return (windowOffsetMs + 999) / 1000+level_offset_sec; //TODO could instead offset from highest-ts-seen

}

void
TimeBasedSubscriber::start() {
  if(ts_field >= 0)
    ts_input_tuple_index = cube->get_schema().dimensions(ts_field).tuple_indexes(0);
  else
    ts_input_tuple_index = -1;
  StrandedSubscriber::start();
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

  

shared_ptr<FlushInfo>
TimeBasedSubscriber::incoming_meta(const OperatorChain& c,
                                   const DataplaneMessage& msg) {
  if (msg.type() == DataplaneMessage::END_OF_WINDOW && msg.has_window_end()) {
      unique_lock<boost::mutex> lock(stateLock);
      time_t t =  times.count(&c) > 0 ? times[&c] : 0;
      time_t msg_ts = time_t(msg.window_end());
//      LOG(INFO) << "End-of-window timestampped " << msg_ts << " ( "  << (tt->now() - msg_ts) << " secs ago)";
      times[&c] = std::max( msg_ts, t);
  } else if (msg.type() == DataplaneMessage::NO_MORE_DATA ) {
      unique_lock<boost::mutex> lock(stateLock);
      times.erase(&c);
  }
  shared_ptr<FlushInfo> p;
  return p;
}
  
  

int
TimeBasedSubscriber::emit_batch() {

  respond_to_congestion(); //do this BEFORE updating window. It may sleep, changing time.

  time_t newMax = 0;
  if (ts_field >= 0) {
    newMax = tt->now() - get_window_offset_sec();
//    LOG(INFO) << "For " << id() << ", tt now is " << tt->now();
    unique_lock<boost::mutex> lock(stateLock);
    
    if (times.size() > 0) {
      time_t min_window_seen = std::numeric_limits<time_t>::max();
      map<const OperatorChain*, time_t>::iterator it = times.begin();
      while (it != times.end()){
        min_window_seen = std::min(min_window_seen, it->second);
        it++;
      }
      VLOG_IF(1,newMax < min_window_seen) << "Subscriber " << id_as_str() <<" on "
         << cube->id_as_str() << " has some end markers; fast-forwarding from " <<
        newMax <<" to "<< min_window_seen;
      
      newMax = std::max(newMax, min_window_seen);
    }
  }
  
  shared_ptr<FlushInfo> nextWindow(new FlushInfo);
  nextWindow->id = newMax;
  nextWindow->subsc = dynamic_pointer_cast<Subscriber>(node->get_operator( id() ));
  cube->flush(nextWindow);
//  post_flush(newMax);
  
  return windowSizeMs;
//  LOG(INFO) << "Subscriber " << id() << " exiting. "   /* Emitted " << emitted_count() */
//            << "Total backfill tuple count " << backfill_tuples <<". ;
}

void
TimeBasedSubscriber::post_flush(unsigned newMax) {

  DataplaneMessage end_msg;
  end_msg.set_type(DataplaneMessage::END_OF_WINDOW);

  if (newMax > 0) {
    end_msg.set_window_end( newMax ); //timestamp is last time inside window, in usec
    querier.max.mutable_e(ts_field)->set_t_val(newMax);
    VLOG(1) << id() << "Updated query times to "<< ts_to_str(next_window_start_time)
      << "-" << ts_to_str(newMax);
  }

  cube::CubeIterator it = querier.do_query();

  if(it == cube->end()) {
    VLOG(1) << id() << ": Nothing found in time subscriber query. Next window start time = "<< next_window_start_time
              <<" Cube monitor capacity ratio: " << cube->congestion_monitor()->capacity_ratio();
  }

  size_t elems = 0;
  vector< shared_ptr<Tuple> > data_buf;
  
  while ( it != cube->end()) {
    data_buf.push_back(*it);
    it++;
    elems ++;
    if (data_buf.size() > TUPLES_PER_BUFFER) {
      chain->process(data_buf);
      data_buf.clear();
    }
    
  }

  end_msg.set_window_length_ms(windowSizeMs);
    
  chain->process(data_buf, end_msg);

  update_backfill_stats(elems);
  
  if (ts_field >= 0) {
    next_window_start_time = querier.max.e(ts_field).t_val() + 1;
    querier.min.mutable_e(ts_field)->set_t_val(next_window_start_time);
  }
  
}

std::string
TimeBasedSubscriber::long_description() const {
//  boost::lock_guard<boost::mutex> lock (mutex);

  ostringstream out;
  out << "Total non-backfill tuples " << regular_tuples << " and ";
  out << backfill_tuples << " late tuples. Sample interval " << windowSizeMs << " ms";
  return out.str();
}

void
TimeBasedSubscriber::send_rollup_levels() {
  DataplaneMessage msg;
  msg.set_type(DataplaneMessage::ROLLUP_LEVELS);
  querier.set_rollup_levels(msg);
  LOG(WARNING) << "subscriber should indicate rollup levels"; // FIXME CHAINS
//  send_meta_downstream(msg);
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
      !(std::stringstream(config["max_window_size"]) >> max_window_size)) {
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

operator_err_t
DelayedOneShotSubscriber::configure(std::map<std::string,std::string> &config) {
  subsc_start = get_msec();
  return OneShotSubscriber::configure(config);
}

int
DelayedOneShotSubscriber::emit_batch() {
  
  int sz;
  {
    unique_lock<boost::mutex> lock(stateLock);
    sz = times.size();
  }
  
  LOG(INFO) << "Delayed one-shot " << id() <<  " is timing out; could have done something here."
    << " Pending on " << sz << " chains";
  //though we also get invoked immediately
  return 10 * 1000; //wait ten seconds.
}

void
DelayedOneShotSubscriber::post_flush(unsigned id) {
  query_running = get_msec();
  OneShotSubscriber::emit_batch();
  unsigned num_chains = former_chains.size();
  LOG(INFO) << "Delayed one-shot statistics (with " << num_chains <<
  " chains):\n***************" << endl
   << "* From subscriber start to first data arriving: " << (first_data - subsc_start) << " ms" << endl
   << "* From subscriber start to first source completing: " << (first_close - subsc_start) << " ms" << endl
   << "* From subscriber start to last source completing: " << (last_close - subsc_start) << " ms" << endl
   << "* First-to-last: " << (last_close - first_close) << " ms" << endl
   << "* From subscriber start to db query start " << (query_running - subsc_start) << " ms " << endl
   << "***************";
  stop_from_subscriber();
//  chain_stopping(NULL); //will trigger a stop.
}


cube::Subscriber::Action
DelayedOneShotSubscriber::action_on_tuple(OperatorChain * chain, boost::shared_ptr<const jetstream::Tuple> const update) {
  unique_lock<boost::mutex> lock(stateLock);
  if (times.find(chain) == times.end() &&
    (former_chains.find(chain) == former_chains.end())) {
    LOG(INFO) << "new chain: " << chain;
    msec_t t = get_msec();
    if (first_data == 0)
      first_data = t;
    times[chain] = t;
  }
  return SEND;
}

shared_ptr<FlushInfo>
DelayedOneShotSubscriber::incoming_meta(const OperatorChain& chain,
                                            const DataplaneMessage& msg) {
  unique_lock<boost::mutex> lock(stateLock);
  LOG(INFO) << "Incoming meta " << msg.type() << " to delayed-one-shot " << id()
    << " from " << (&chain);
  if (msg.type() == DataplaneMessage::END_OF_WINDOW) {
    times[&chain] = get_msec();
  } else if (msg.type() == DataplaneMessage::NO_MORE_DATA ) {
    times.erase(&chain);
    former_chains[&chain] = true;
    if(first_close == 0)
      first_close = get_msec();
  }
  unsigned chains_in = cube->in_chains();
  LOG(INFO) << "Total of " << chains_in << " chains left for " << id();
  shared_ptr<FlushInfo> p;
  if (chains_in == 0 ) {
    
    last_close = get_msec();
    p = shared_ptr<FlushInfo>(new FlushInfo);
    p->id = 0; //ignored
  }
  return p;
}

const string TimeBasedSubscriber::my_type_name("Timer-based subscriber");
const string VariableCoarseningSubscriber::my_type_name("Variable time-based subscriber");
const string OneShotSubscriber::my_type_name("One-shot subscriber");
const string DelayedOneShotSubscriber::my_type_name("Delayed-effect one-shot subscriber");



} //end namespace
