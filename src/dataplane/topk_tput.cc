
#include "js_utils.h"
#include <glog/logging.h>

#include "topk_tput.h"

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
MultiRoundSender::end_of_round(int round_no) {
  VLOG(1) << "Completed TPUT round " << round_no << " on source side";

  DataplaneMessage end_msg;
  end_msg.set_tput_round(round_no);
  end_msg.set_type(DataplaneMessage::END_OF_WINDOW);
  send_meta_downstream(end_msg);
}

void
MultiRoundSender::meta_from_downstream(const DataplaneMessage & msg) {

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
      emit(*it);
      it++;
    }
    end_of_round(1);

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
        emit(t);
        it++;
        t = *it;
      }
    } else {
      while ( it != cube->end() && get_rank_val(t, total_col) <= msg.tput_r2_threshold()) {
        emit(t);
        it++;
        t = *it;
      }
    }

    end_of_round(2);

  } else  if ( msg.type() == DataplaneMessage::TPUT_ROUND_3) {

    int emitted = 0;
    for (int i =0; i < msg.tput_r3_query_size(); ++i) {
      const Tuple& q = msg.tput_r3_query(i);
      Tuple my_min;
      my_min.CopyFrom(q);
      Tuple my_max;
      my_max.CopyFrom(q);
      for ( int i = 0; i < rollup_levels.size(); ++i)
        if ( rollup_levels[i] == 0) {
          my_min.mutable_e(i)->CopyFrom(min.e(i));
          my_max.mutable_e(i)->CopyFrom(max.e(i));
        }
      
      cube::CubeIterator v = cube->slice_and_rollup(rollup_levels,
                    cube->get_sourceformat_tuple(my_min),
                    cube->get_sourceformat_tuple(my_max));
      if(v.numCells() == 1) {
        boost::shared_ptr<Tuple> val = *v;
        VLOG(1) << "R3 of " << id() << " emitting " << fmt( *(val));
        emit(val);
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
    LOG(INFO) << "end of tput for " << id() << "; emitted " << emitted << " tuples";
//     << " based on " << msg.tput_r3_query_size() << " query terms";
    end_of_round(3);
    
  } else {
    DataPlaneOperator::meta_from_downstream(msg);
  }

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

  boost::shared_ptr<TupleReceiver> dest = get_dest();
  boost::shared_ptr<DataPlaneOperator> dest_op;
  do {
    destcube = boost::dynamic_pointer_cast<DataCube>(dest);
    dest_op = boost::dynamic_pointer_cast<DataPlaneOperator>(dest);
    if (dest_op)
      dest = dest_op->get_dest();
  } while (dest_op && !destcube);
  
  if (!destcube) {
    LOG(FATAL) << "must attach MultiRoundCoordinator to cube";
  }
  
  string trimmed_name = sort_column[0] == '-' ? sort_column.substr(1) : sort_column;
  total_col = destcube->aggregate_offset(trimmed_name)[0]; //assume only one column for dimension
  running = true;
  timer = get_timer();
  
  wait_for_restart();
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
  for (int i = 0; i < destcube->num_dimensions(); ++i)
    start_proto.add_rollup_levels(DataCube::LEAF_LEVEL);
  candidates.clear();
  
  while (! new_preds.empty()) {
    predecessors.push_back(new_preds.back());
    LOG(INFO) << "Adding pred " << new_preds.back()->id_as_str();
    new_preds.pop_back();
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
    shared_ptr<TupleSender> pred = predecessors[i];
    pred->meta_from_downstream(start_proto);
  }

}



void
MultiRoundCoordinator::process(boost::shared_ptr<Tuple> t, const operator_id_t pred) {
  boost::lock_guard<tput_mutex> lock (mutex);

  if ( (phase == ROUND_1)|| (phase == ROUND_2)) {

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


  } else if (phase == ROUND_3) {//just let responses through
//    LOG(INFO) << "passing through " << fmt(*t) << " in TPUT phase 3";
    emit(t);
  }
//  else
//    LOG(WARNING) << "ignoring input"

}


void
MultiRoundCoordinator::meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred) {
  boost::lock_guard<tput_mutex> lock (mutex);

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
          if (min_window_size > 0)
            wait_for_restart();
        }

      }
    }
  }

}

double
MultiRoundCoordinator::calculate_tao() {
  double tao = 0;
  
  vector<double> vals;
  vals.reserve(candidates.size());
  std::map<DimensionKey, CandidateItem >::iterator iter;
  for (iter = candidates.begin(); iter != candidates.end(); iter++) {
    vals.push_back(iter->second.val);
  }
  sort (vals.begin(), vals.end());

  if (vals.size() < num_results)
    tao = vals[ vals.size() -1];
  else
    if ( sort_column[0] == '-' ) //reverse sort; greatest to least
      tao = vals[vals.size() - num_results];
    else
      tao = vals[num_results-1];
  return tao;
}


void
MultiRoundCoordinator::start_phase_2() {

//	Controller takes partial sums; declare kth one as phase-1 bottom Tao-1.
//	[Needs no per-source state at controller]

  tao_1 = calculate_tao();

  double t1 = tao_1 / predecessors.size();
  VLOG(1) << "tao at start of phase two is " << tao_1 << ". Threshold is " << t1
    << ". " << candidates.size()<< " candidates";

  phase = ROUND_2;
  DataplaneMessage start_phase;
  start_phase.set_type(DataplaneMessage::TPUT_ROUND_2);
  start_phase.set_tput_r2_threshold(t1);
  start_phase.set_tput_r2_col(total_col);
  for (unsigned int i = 0; i < predecessors.size(); ++i) {
    shared_ptr<TupleSender> pred = predecessors[i];
    pred->meta_from_downstream(start_phase);
  }

}

void
MultiRoundCoordinator::start_phase_3() {

  phase = ROUND_3;

  double tao = calculate_tao();
  DataplaneMessage r3_start;
  r3_start.set_type(DataplaneMessage::TPUT_ROUND_3);

  std::map<DimensionKey, CandidateItem >::iterator iter;
  unsigned int pred_size = predecessors.size();
  for (iter = candidates.begin(); iter != candidates.end(); iter++) {
    double upper_bound = (pred_size - iter->second.responses) * tao_1 + iter->second.val;
    if (upper_bound >= tao) {
      Tuple * t = r3_start.add_tput_r3_query();
      t->CopyFrom(iter->second.example);
    }
  }
  LOG(INFO) << "tao at start of phase three is " << tao<< "; total of " << r3_start.tput_r3_query_size() << " candidates";

  for (unsigned int i = 0; i < pred_size; ++i) {
    shared_ptr<TupleSender> pred = predecessors[i];
    pred->meta_from_downstream(r3_start);
  }
}


void
MultiRoundCoordinator::wait_for_restart() {
  time_t now = time(NULL);
  time_t window_end = max( now - window_offset, start_ts + min_window_size);
  LOG(INFO) << "Going to run TPUT at " << window_end << " (now is  " << now<< ")";
  timer->expires_at(boost::posix_time::from_time_t(window_end));
  timer->async_wait(boost::bind(&MultiRoundCoordinator::start_phase_1, this, window_end));
//  LOG(INFO) << "Timer will expire in " << timer->expires_from_now();
}

void
MultiRoundCoordinator::stop() {
  boost::lock_guard<tput_mutex> lock (mutex);
  running = false;
  timer->cancel();
}

const string MultiRoundSender::my_type_name("TPUT Multi-Round sender");
const string MultiRoundCoordinator::my_type_name("TPUT Multi-Round coordinator");


}
