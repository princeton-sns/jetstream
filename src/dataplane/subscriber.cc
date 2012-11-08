#include "subscriber.h"
#include <glog/logging.h>
#include <boost/bind.hpp>

using namespace std;
using namespace jetstream::cube;

void Subscriber::process (boost::shared_ptr<jetstream::Tuple> t) {
  LOG(FATAL)<<"Cube Subscriber should never process";
}

const string Subscriber::my_type_name("Subscriber");

void Subscriber::insert_callback(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value) {
  exec.submit(boost::bind(&Subscriber::post_insert, this, update, new_value));  
}

void Subscriber::update_callback(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value, 
                                 boost::shared_ptr<jetstream::Tuple> const &old_value) {
  exec.submit(boost::bind(&Subscriber::post_update, this, update, new_value, old_value));  
}

size_t Subscriber::queue_length() {
  return exec.outstanding_tasks();
}
