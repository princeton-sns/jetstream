
#include "filter_subscriber.h"

using namespace jetstream;
using namespace ::std;


jetstream::cube::Subscriber::Action
FilterSubscriber::action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) {
  return SEND;
}

void
FilterSubscriber::post_insert(boost::shared_ptr<jetstream::Tuple> const &update, boost::shared_ptr<jetstream::Tuple> const &new_value) {
  emit(update);
}

void
FilterSubscriber::post_update(boost::shared_ptr<jetstream::Tuple> const &update,boost::shared_ptr<jetstream::Tuple> const &new_value, boost::shared_ptr<jetstream::Tuple> const &old_value) {
  LOG(FATAL)<<"Should never be used";
}



operator_err_t
FilterSubscriber::configure(std::map<std::string,std::string> &config) {

  return NO_ERR;
}



void
FilterSubscriber::process(boost::shared_ptr<Tuple> t) {
  //should do something here

}