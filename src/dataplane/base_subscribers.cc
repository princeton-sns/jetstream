#include "base_subscribers.h"
#include <glog/logging.h>

using namespace std;
using namespace jetstream::cube;

jetstream::cube::Subscriber::Action QueueSubscriber::action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) {
  return returnAction;
}

void QueueSubscriber::insert_callback(boost::shared_ptr<jetstream::Tuple> const &update, boost::shared_ptr<jetstream::Tuple> const &new_value) {
  insert_q.push_back(new_value);
}

void QueueSubscriber::update_callback(boost::shared_ptr<jetstream::Tuple> const &update,boost::shared_ptr<jetstream::Tuple> const &new_value, boost::shared_ptr<jetstream::Tuple> const &old_value) {
  update_q.push_back(new_value);
}

jetstream::cube::Subscriber::Action UnionSubscriber::action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) {
  return SEND;
}

void UnionSubscriber::insert_callback(boost::shared_ptr<jetstream::Tuple> const &update, boost::shared_ptr<jetstream::Tuple> const &new_value) {
  emit(update);
}

void UnionSubscriber::update_callback(boost::shared_ptr<jetstream::Tuple> const &update,boost::shared_ptr<jetstream::Tuple> const &new_value, boost::shared_ptr<jetstream::Tuple> const &old_value) {
  LOG(FATAL)<<"Should never be used";  
}
