#include <string>

#include "subscriber.h"

using namespace ::std;

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


void TimeBasedSubscriber::operator()() {

	 while (running)  {
  //sleep
    boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
	}

}


const string TimeBasedSubscriber::my_type_name("Timer-based subscriber");



}