
#include "js_utils.h"
#include <glog/logging.h>

#include "topk_tput.h"
#include "querier.h"

using namespace ::std;
//using namespace boost;
//using namespace boost::interprocess::ipcdetail;

namespace jetstream {


double get_rank_val(boost::shared_ptr<const jetstream::Tuple> t, size_t col) {
  double v = 0;
  const Element& e = t->e(col);
  if (e.has_d_val())
    v = e.d_val();
  else if (e.has_i_val())
    v = e.i_val();
  else
    LOG(FATAL) << "expected a numeric value for column "<< col << "; got " << fmt(*t);
  return v;
}

cube::Subscriber::Action
MultiRoundSender::action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) {

  return NO_SEND;
}




void
MultiRoundSender::post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value) {
	;
}

//called on backfill
void
MultiRoundSender::post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value) {
  ;
}


void
MultiRoundSender::get_bounds(Tuple & my_min, Tuple & my_max, const Tuple & q, int time_col) {
    my_min.CopyFrom(q);
    my_max.CopyFrom(q);
    for (unsigned i = 0; i < rollup_levels.size(); ++i) {
      if (rollup_levels[i] == 0) {
        my_min.mutable_e(i)->CopyFrom(min.e(i));
        my_max.mutable_e(i)->CopyFrom(max.e(i));
      }
    }
}

void
MultiRoundSender::meta_from_downstream(DataplaneMessage & msg) {

  DataplaneMessage round_end_marker;
  vector<  boost::shared_ptr<Tuple> > data_buf;

  round_end_marker.set_type(DataplaneMessage::END_OF_WINDOW);

  take_greatest = msg.tput_sort_key()[0] == '-';
//  VLOG(1) << "TPUT got meta from downstream";
  if ( msg.type() == DataplaneMessage::TPUT_START) {
    //sources send their top k.  TODO what about ties?
    sort_order.push_back(msg.tput_sort_key());
    min.CopyFrom(msg.has_tput_r1_start() ?  msg.tput_r1_start() : cube->empty_tuple());
    max.CopyFrom( msg.has_tput_r1_end() ?  msg.tput_r1_end(): min);


    LOG(INFO) << id() << " doing query; range is " << fmt(min) << " to " << fmt(max);
    if (rollup_levels.size() == 0) {
      for (int i =0; i < msg.rollup_levels_size(); ++i) {
        rollup_levels.push_back( msg.rollup_levels(i));
      }
    }
    LOG_IF(FATAL, rollup_levels.size() != cube->num_dimensions()) << "Got "
        << rollup_levels.size() << " rollup dimensions for cube with "
        << cube->num_dimensions() << "dims";
    cube::CubeIterator it = cube->slice_and_rollup(rollup_levels, min, max, sort_order, msg.tput_k());
//    LOG(INFO) << "round-1 slice query returned " << it.numCells() << " cells";

    while ( it != cube->end()) {
      data_buf.push_back(*it);
      it++;
    }
    round_end_marker.set_tput_round(1);

  } else  if ( msg.type() == DataplaneMessage::TPUT_ROUND_2) {

    size_t total_col = msg.tput_r2_col();
  // Sources send all items >= T  and not in top k.
    cube::CubeIterator it = cube->slice_and_rollup(rollup_levels, min, max, sort_order);
    int count = 0;
    while ( it != cube->end() && count++ < msg.tput_k() ) {
      it++;
    }
    boost::shared_ptr<Tuple> t = *it;
    if (take_greatest) {
      while ( it != cube->end() && get_rank_val(t, total_col) >= msg.tput_r2_threshold()) {
        data_buf.push_back(t);
        it++;
        t = *it;
      }
    } else {
      while ( it != cube->end() && get_rank_val(t, total_col) <= msg.tput_r2_threshold()) {
        data_buf.push_back(t);
        it++;
        t = *it;
      }
    }
    round_end_marker.set_tput_round(2);

  } else  if ( msg.type() == DataplaneMessage::TPUT_ROUND_3) {

    int emitted = 0;
    for (int i =0; i < msg.tput_r3_query_size(); ++i) {
      int time_col = msg.has_tput_r3_timecol() ? msg.tput_r3_timecol() :  -1;
          //The below is a yucky hack to make sure we do a rollup of the time dimension
      const Tuple& q = msg.tput_r3_query(i);
      Tuple my_min;
      Tuple my_max;
      get_bounds(my_min, my_max, q, time_col);
//      LOG(INFO) << "R3 querying for " << fmt( my_min ) << " to " << fmt(my_max);

      cube::CubeIterator v = cube->slice_and_rollup(rollup_levels, my_min, my_max);
      
      if(v.numCells() == 1) {
        boost::shared_ptr<Tuple> val = *v;
        if (time_col != -1)
          val->mutable_e(time_col)->set_t_val(my_min.e(time_col).t_val());
        VLOG(1) << "R3 of " << id() << " emitting " << fmt( *(val));
        data_buf.push_back(val);
        emitted ++;
      } else if (v.numCells() == 0) {
        //this is not an error. Something might be a candidate but have no hits on this node.
        LOG(WARNING) << "no matches when querying for " << fmt( my_min ) << " to " <<
              fmt(my_max);
      } else {
        LOG(WARNING) << "too many matches querying for " << fmt( my_min ) <<
              " to " << fmt(my_max) << " ( got " << v.numCells() << ")";
      }
    }
    
//    LOG(INFO) << "end of tput for " << id() << "; emitted " << emitted << " tuples";
//     << " based on " << msg.tput_r3_query_size() << " query terms";

    round_end_marker.set_tput_round(3);
  } else {
//    DataPlaneOperator::meta_from_downstream(msg);
    return; //no emit
  }
  
  chain->process(data_buf, round_end_marker);

}

operator_err_t
MultiRoundCoordinator::configure(std::map<std::string,std::string> &config) {

  if (config.find("num_results") != config.end()) {
    num_results = boost::lexical_cast<int32_t>(config["num_results"]);
  } else
    return operator_err_t("must specify num_results for multi-round top-k");


  if (config.find("sort_column") != config.end()) {
    sort_column = config["sort_column"];
  } else {
    return operator_err_t("must specify sort_column for multi-round top-k");
  }

  if (config.find("min_window_size") != config.end())
    min_window_size = boost::lexical_cast<unsigned>(config["min_window_size"]);
  else
    min_window_size = 0;

  if (config.find("rollup_levels") != config.end()) {
    get_rollup_level_array(config["rollup_levels"], rollup_levels);
  }

  if (config.find("ts_field") != config.end()) {
    ts_field = boost::lexical_cast<int32_t>(config["ts_field"]);
//    total_fields = boost::lexical_cast<int32_t>(config["total_fields"]);

    if (config.find("start_ts") != config.end())
      start_ts = boost::lexical_cast<time_t>(config["start_ts"]);
    else
      start_ts = 0;
    
    window_offset = 0;
    if (config.find("window_offset") != config.end()) {
      window_offset = boost::lexical_cast<time_t>(config["window_offset"]);
      window_offset /= 1000; //convert to seconds from ms
    }
  }
  else {
    ts_field = -1;
    start_ts = 0;
    window_offset = 0;
  }

  phase = NOT_STARTED;
  return NO_ERR;
}


void
MultiRoundCoordinator::start() {
  boost::lock_guard<tput_mutex> lock (mutex);
  
  chain = future_preds.begin()->second;
  LOG_IF(FATAL, !chain) << "Can't have a coordinator with no chain";

  if (!destcube) {
    boost::shared_ptr<ChainMember> dest = chain->member(chain->members() -1);
    destcube = boost::dynamic_pointer_cast<DataCube>(dest);
    if (!destcube) {
      LOG(FATAL) << "must attach MultiRoundCoordinator to cube";
    }
  
    string trimmed_name = sort_column[0] == '-' ? sort_column.substr(1) : sort_column;
    total_col = destcube->aggregate_offset(trimmed_name)[0]; //assume only one column for dimension
  }
  TimerSource::start();
}

void
MultiRoundCoordinator::add_chain(boost::shared_ptr<OperatorChain> chain) {
  LOG(INFO) << "Putting MultiRoundCoordinator into chain " << chain->chain_name();
  boost::lock_guard<tput_mutex> lock (mutex);
  future_preds[chain.get()] = chain;
}

void
MultiRoundCoordinator::chain_stopping(OperatorChain * c) {
  boost::lock_guard<tput_mutex> lock (mutex);
  future_preds.erase(c);
}


  //invoked only via timer
void
MultiRoundCoordinator::start_phase_1(time_t window_end) {
  boost::lock_guard<tput_mutex> lock (mutex);

  if (! running)
    return;

  phase = ROUND_1;
  responses_this_phase = 0;
  DataplaneMessage start_proto;
  start_proto.set_type(DataplaneMessage::TPUT_START);
  start_proto.set_tput_k(num_results);
  start_proto.set_tput_sort_key(sort_column);
  if (rollup_levels.size() > 0)
    for (unsigned int i = 0; i < rollup_levels.size(); ++i)
      start_proto.add_rollup_levels(rollup_levels[i]);
  else
    for (unsigned int i = 0; i <  destcube->num_dimensions(); ++i)
      start_proto.add_rollup_levels(DataCube::LEAF_LEVEL);
  
  candidates.clear();
  
  predecessors.clear();
  map<OperatorChain *, boost::shared_ptr<OperatorChain> >::iterator preds;
  for (preds =future_preds.begin(); preds != future_preds.end(); ++preds) {
    predecessors.push_back(preds->second);
    LOG(INFO) << "Adding pred " << preds->second->chain_name();
  }
  
  if ( predecessors.size() == 0) {
    LOG(INFO) << "TPUT stalling waiting for a predecessor";
    timer->expires_from_now(boost::posix_time::seconds(2));
    timer->async_wait(boost::bind(&MultiRoundCoordinator::start_phase_1, this, window_end+2));
    return;
  }
  
  
  LOG(INFO) << "starting TPUT, k = " << num_results << " and col is " << sort_column << " (id " << total_col << "). "
      << predecessors.size() << " predecessors";

  //todo should set tput_r1_start and tput_r2_start
  if (ts_field > -1) {
    
    dim_filter_start = destcube->empty_tuple();
    dim_filter_end = destcube->empty_tuple();
    
    dim_filter_start.mutable_e(ts_field)->set_t_val( start_ts );
    dim_filter_end.mutable_e(ts_field)->set_t_val( window_end );
    start_ts = window_end + 1;
    
    start_proto.mutable_tput_r1_start()->CopyFrom(dim_filter_start);
    start_proto.mutable_tput_r1_end()->CopyFrom(dim_filter_end);
    
    start_proto.set_rollup_levels(ts_field, 0); //roll up the time period
  }
  
  for (unsigned int i = 0; i < predecessors.size(); ++i) {
    shared_ptr<OperatorChain> pred = predecessors[i];
    pred->upwards_metadata(start_proto, this);
  }

}



void
MultiRoundCoordinator::process (
          OperatorChain * c,
          vector<boost::shared_ptr<Tuple> > & tuples,
          DataplaneMessage & msg) {
  boost::lock_guard<tput_mutex> lock (mutex);

  if ( (phase == ROUND_1)|| (phase == ROUND_2)) {

    for (int i = 0; i < tuples.size(); ++i) {
      boost::shared_ptr<Tuple> t = tuples[i];
      if(!t)
        continue;
      DimensionKey k = destcube->get_dimension_key(*t, destcube->get_leaf_levels());
      double v = get_rank_val(t, total_col);

      std::map<DimensionKey,CandidateItem>::const_iterator found = candidates.find(k);
      if(found != candidates.end()) {
        CandidateItem& c = candidates[k];
        c.val += v;
        c.responses ++;
      } else {
        candidates[k] = CandidateItem(v, 1,  *t);
      }
    }
  } else if (phase == ROUND_3) {//just let responses through
//    LOG(INFO) << "passing through " << fmt(*t) << " in TPUT phase 3";
    return;
  }
//  else
//    LOG(WARNING) << "ignoring input"

  if (msg.type() == DataplaneMessage::END_OF_WINDOW) {
   // check what phase source was in; increment label and counter.
   // If we're done with phase, proceed!
    LOG_IF(FATAL, !msg.has_tput_round()) << " a valid end-of-window to a tput controller must have a phase number";
    int sender_round = msg.tput_round();
    if (sender_round == phase) {
      responses_this_phase ++;
      if (responses_this_phase == predecessors.size()) {
        LOG(INFO) << id() << " completed TPUT round " << phase << " with " << candidates.size()<< " candidates";
        if (candidates.size() == 0) {          
          phase = ROUND_3;
        }
        
        responses_this_phase = 0;
        if ( phase == 1) {
          start_phase_2();
        } else if (phase == 2) {
          start_phase_3();
        } else {
          LOG_IF(FATAL, phase != 3) << " TPUT should only be in phases 1-3, was " << phase;
          //done!
          phase = NOT_STARTED;
        }

      }
    }
  }


}




double
MultiRoundCoordinator::calculate_tau() {
  double tau = 0;
  
  vector<double> vals;
  vals.reserve(candidates.size());
  std::map<DimensionKey, CandidateItem >::iterator iter;
  for (iter = candidates.begin(); iter != candidates.end(); iter++) {
    vals.push_back(iter->second.val);
  }
  sort (vals.begin(), vals.end());

  if (vals.size() < num_results)
    tau = vals[ vals.size() -1];
  else
    if ( sort_column[0] == '-' ) //reverse sort; greatest to least
      tau = vals[vals.size() - num_results];
    else
      tau = vals[num_results-1];
  return tau;
}


void
MultiRoundCoordinator::start_phase_2() {

//	Controller takes partial sums; declare kth one as phase-1 bottom Tau-1.
//	[Needs no per-source state at controller]

  tau_1 = calculate_tau();

  double t1 = tau_1 / predecessors.size();
  VLOG(1) << "tau at start of phase two is " << tau_1 << ". Threshold is " << t1
    << ". " << candidates.size()<< " candidates";

  phase = ROUND_2;
  DataplaneMessage start_phase;
  start_phase.set_type(DataplaneMessage::TPUT_ROUND_2);
  start_phase.set_tput_r2_threshold(t1);
  start_phase.set_tput_r2_col(total_col);
  for (unsigned int i = 0; i < predecessors.size(); ++i) {
    shared_ptr<OperatorChain> pred = predecessors[i];
    pred->upwards_metadata(start_phase, this);
  }

}

void
MultiRoundCoordinator::start_phase_3() {

  phase = ROUND_3;

  double tau = calculate_tau();
  DataplaneMessage r3_start;
  r3_start.set_type(DataplaneMessage::TPUT_ROUND_3);
  r3_start.set_tput_r3_timecol(ts_field);

  std::map<DimensionKey, CandidateItem >::iterator iter;
  unsigned int pred_size = predecessors.size();
  for (iter = candidates.begin(); iter != candidates.end(); iter++) {
    double upper_bound = (pred_size - iter->second.responses) * tau_1 + iter->second.val;
    if (upper_bound >= tau) {
      Tuple * t = r3_start.add_tput_r3_query();
      t->CopyFrom(iter->second.example);
    }
  }
  VLOG(1) << "tau at start of phase three is " << tau<< "; total of " << r3_start.tput_r3_query_size() << " candidates";

  for (unsigned int i = 0; i < pred_size; ++i) {
    shared_ptr<OperatorChain> pred = predecessors[i];
    pred->upwards_metadata(r3_start, this);
  }
}


int
MultiRoundCoordinator::emit_data() {
  boost::lock_guard<tput_mutex> lock (mutex);

  time_t now = time(NULL);
  time_t window_end = max( now - window_offset, start_ts + min_window_size);

  if (window_end < now)
    return 1000 * (now - window_end);
  else {
    if (phase == NOT_STARTED) {
      start_phase_1(window_end);
    }
  }
  return min_window_size;
  
//  LOG(INFO) << "Timer will expire in " << timer->expires_from_now();
}


const string MultiRoundSender::my_type_name("TPUT Multi-Round sender");
const string MultiRoundCoordinator::my_type_name("TPUT Multi-Round coordinator");


}
