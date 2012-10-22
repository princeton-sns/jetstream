#include "base_subscribers.h"
#include <glog/logging.h>

using namespace std;
using namespace boost;
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


namespace jetstream {

cube::Subscriber::Action
TimeBasedSubscriber::action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) {
  return NO_SEND;
}

void
TimeBasedSubscriber::insert_callback(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value) {
	; 
}

void
TimeBasedSubscriber::update_callback(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value, 
                                 boost::shared_ptr<jetstream::Tuple> const &old_value) {
  ;
}

operator_err_t
TimeBasedSubscriber::configure(std::map<std::string,std::string> &config) {
 windowSizeMs = 1000;
 return NO_ERR;
}


void
TimeBasedSubscriber::start() {
  running = true;
  loopThread = shared_ptr<boost::thread>(new boost::thread(boost::ref(*this)));
}

void TimeBasedSubscriber::operator()() {

	 while (running)  {
  //sleep
    boost::this_thread::sleep(boost::posix_time::milliseconds(windowSizeMs));
    //should do a query and push the results
    
	}

}


const string TimeBasedSubscriber::my_type_name("Timer-based subscriber");



} //end namespace