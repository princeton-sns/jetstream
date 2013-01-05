
#include "js_utils.h"
#include <glog/logging.h>

#include "topk_tput.h"

using namespace ::std;
//using namespace boost;
//using namespace boost::interprocess::ipcdetail;

namespace jetstream {


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
MultiRoundSender::meta_from_downstream(const DataplaneMessage & msg) {

  if ( msg.type() == DataplaneMessage::TPUT_START) {
    const jetstream::Tuple& min = msg.tput_r1_start();
    const jetstream::Tuple& max = msg.tput_r1_end();
    
    std::list<string> sort_order;
    std::stringstream ss(msg.tput_sort_key());
    std::string item;
    while(std::getline(ss, item, ',')) {
        sort_order.push_back(item);
    }

    VLOG(1) << id() << " doing query; range is " << fmt(min) << " to " << fmt(max);
    cube::CubeIterator it = cube->slice_query(min, max, true, sort_order, msg.tput_k());
    while ( it != cube->end()) {
      emit(*it);
      it++;
    }
    DataplaneMessage end_msg;
    end_msg.set_tput_round(1);
    end_msg.set_type(DataplaneMessage::END_OF_WINDOW);
    send_meta_downstream(end_msg);

  } else  if ( msg.type() == DataplaneMessage::TPUT_ROUND_2) {

    
  /*
    Sources send all items >= T  [and not in top k?]
  */


  } else  if ( msg.type() == DataplaneMessage::TPUT_ROUND_3) {
  
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
    return operator_err_t("must specify sort_order for multi-round top-k");

  phase = NOT_STARTED;
  return NO_ERR;
}

void
MultiRoundCoordinator::start() {

  destcube = boost::dynamic_pointer_cast<DataCube>(get_dest());
  if (!destcube) {
    LOG(FATAL) << "must attach MultiRoundCoordinator to cube";
  }
  total_col = destcube->aggregate_offset(sort_column);
  
  phase = ROUND_1;
  responses_this_phase = 0;
  DataplaneMessage start_proto;
  start_proto.set_tput_k(num_results);
  start_proto.set_tput_sort_key(sort_column);
  
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
    double v = 0;
    const Element& e = t->e(total_col);
    if (e.has_d_val())
      v = e.d_val();
    else if (e.has_i_val())
      v += e.i_val();
    else
      LOG(FATAL) << "expected a numeric value for column "<< total_col << "; got " << fmt(*t);
    
    std::map<DimensionKey,size_t>::const_iterator found = response_counts.find(k);
    if(found != response_counts.end()) {
      response_counts[k] ++;
      partial_totals[k] += v;
    } else {
      response_counts[k] = 1;
      partial_totals[k] = v;
    }

    
  } else if (phase == ROUND_3)//just let responses through
    emit(t);
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
        LOG(INFO) << "have completed round " << phase;
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



void
MultiRoundCoordinator::start_phase_2() {

//	Controller takes partial sums; declare kth one as phase-1 bottom Tao-1.
//	[Needs no per-source state at controller]

  vector<double> vals;
  vals.reserve(partial_totals.size());
  std::map<DimensionKey, double >::iterator iter;
  for (iter = partial_totals.begin(); iter != partial_totals.end(); iter++) {
    vals.push_back(iter->second);
  }
  sort (vals.begin(), vals.end());
  
  double tao_1;
  if (vals.size() <= num_results)
    tao_1 = vals[ vals.size() -1];
  else
    tao_1 = vals[num_results];
  double t1 = tao_1 / predecessors.size();

  phase = ROUND_2;
  DataplaneMessage start_phase;
  start_phase.set_tput_r2_threshold(t1);
  for (unsigned int i = 0; i < predecessors.size(); ++i) {
    shared_ptr<TupleSender> pred = predecessors[i];
    pred->meta_from_downstream(start_phase);
  }
  
}

void
MultiRoundCoordinator::start_phase_3() {
/*	Take partial sums; kth one is Tao-2
	[No per-source state]
*/
// 	Controller sends back set of candidate objects

}



const string MultiRoundSender::my_type_name("TPUT Multi-Round sender");
const string MultiRoundCoordinator::my_type_name("TPUT Multi-Round coordinator");


}