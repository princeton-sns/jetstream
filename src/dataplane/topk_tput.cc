
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

  } else  if ( msg.type() == DataplaneMessage::TPUT_ROUND_2) {

  } else  if ( msg.type() == DataplaneMessage::TPUT_ROUND_3) {
  
  } else {
    DataPlaneOperator::meta_from_downstream(msg);
  }  

}




void
MultiRoundCoordinator::process(boost::shared_ptr<Tuple> t) {
  emit(t);

}


const string MultiRoundSender::my_type_name("TPUT Multi-Round sender");
const string MultiRoundCoordinator::my_type_name("TPUT Multi-Round coordinator");


}