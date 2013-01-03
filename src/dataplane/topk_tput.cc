
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



}