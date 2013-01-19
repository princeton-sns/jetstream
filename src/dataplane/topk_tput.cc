
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

  const jetstream::Tuple& min = msg.has_tput_r1_start() ?  msg.tput_r1_start() : cube->empty_tuple();
  const jetstream::Tuple& max = msg.has_tput_r1_end() ?  msg.tput_r1_end(): min;
  take_greatest = msg.tput_sort_key()[0] == '-';
  VLOG(2) << "got meta from downstream";
  if ( msg.type() == DataplaneMessage::TPUT_START) {
    //sources send their top k.  TODO what about ties?
    sort_order.push_back(msg.tput_sort_key());
    
    VLOG(1) << id() << " doing query; range is " << fmt(min) << " to " << fmt(max);
    cube::CubeIterator it = cube->slice_query(min, max, true, sort_order, msg.tput_k());
    while ( it != cube->end()) {
      emit(*it);
      it++;
    }
    end_of_round(1);

  } else  if ( msg.type() == DataplaneMessage::TPUT_ROUND_2) {

    size_t total_col = msg.tput_r2_col();
  // Sources send all items >= T  and not in top k.
    cube::CubeIterator it = cube->slice_query(min, max, true, sort_order);
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
      boost::shared_ptr<Tuple> v = cube->get_cell_value(q);
      if(v) {
        VLOG(1) << "R3 of " << id() << " emitting " << fmt( *v);
        emit(v);
        emitted ++;
      } else {
        //this is not an error. Something might be a candidate but have no hits on this node.
//        LOG(WARNING) << "no matches when querying for " << fmt( q );
      }
    }
    LOG(INFO) << "end of tput for " << id() << "; emitting " << emitted << " tuples";
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
  } else  
    return operator_err_t("must specify sort_column for multi-round top-k");

  phase = NOT_STARTED;
  return NO_ERR;
}

void
MultiRoundCoordinator::start() {

  destcube = boost::dynamic_pointer_cast<DataCube>(get_dest());
  if (!destcube) {
    LOG(FATAL) << "must attach MultiRoundCoordinator to cube";
  }
  string trimmed_name = sort_column[0] == '-' ? sort_column.substr(1) : sort_column;
  total_col = destcube->aggregate_offset(trimmed_name)[0]; //assume only one column for dimension
  
  phase = ROUND_1;
  responses_this_phase = 0;
  DataplaneMessage start_proto;
  start_proto.set_type(DataplaneMessage::TPUT_START);
  start_proto.set_tput_k(num_results);
  start_proto.set_tput_sort_key(sort_column);
  LOG(INFO) << "starting TPUT, k = " << num_results << " and col is " << sort_column << " (id " << total_col << "). "
      << predecessors.size() << " predecessors";
  
  //todo should set tput_r1_start and tput_r2_start
  for (unsigned int i = 0; i < predecessors.size(); ++i) {
    shared_ptr<TupleSender> pred = predecessors[i];
    pred->meta_from_downstream(start_proto);
  }
  
}



void
MultiRoundCoordinator::process(boost::shared_ptr<Tuple> t, const operator_id_t pred) {

  if ( (phase == ROUND_1)|| (phase == ROUND_2)) {
    
    DimensionKey k = destcube->get_dimension_key(*t);
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
  if (msg.type() == DataplaneMessage::END_OF_WINDOW) {
   // check what phase source was in; increment label and counter.
   // If we're done with phase, proceed!
    LOG_IF(FATAL, !msg.has_tput_round()) << " a valid end-of-window to a tput controller must have a phase number";
    int sender_round = msg.tput_round();
    if (sender_round == phase) {
      responses_this_phase ++;
      if (responses_this_phase == predecessors.size()) {
        LOG(INFO) << id() << " completed TPUT round " << phase << " with " << candidates.size()<< " candidates";
        responses_this_phase = 0;
        if ( phase == 1) {
          start_phase_2();
        } else if (phase == 2) {
          start_phase_3();
        } else {
          //done!
          phase = NOT_STARTED;
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
  VLOG(1) << "tao at start of phase three is " << tao<< "; total of " << r3_start.tput_r3_query_size() << " candidates";

  for (unsigned int i = 0; i < pred_size; ++i) {
    shared_ptr<TupleSender> pred = predecessors[i];
    pred->meta_from_downstream(r3_start);
  }

}



const string MultiRoundSender::my_type_name("TPUT Multi-Round sender");
const string MultiRoundCoordinator::my_type_name("TPUT Multi-Round coordinator");


}
