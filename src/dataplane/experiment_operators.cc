#include "experiment_operators.h"

#include <glog/logging.h>

using namespace std;
using namespace boost;

namespace jetstream {


DummyReceiver::~DummyReceiver() {
  VLOG(1) << "destructing dummy receiver";
}



operator_err_t
SendK::configure (std::map<std::string,std::string> &config) {
  if (config["k"].length() > 0) {
    // stringstream overloads the '!' operator to check the fail or bad bit
    if (!(stringstream(config["k"]) >> k)) {
      LOG(WARNING) << "invalid number of tuples: " << config["k"] << endl;
      return operator_err_t("Invalid number of tuples: '" + config["k"] + "' is not a number.") ;
    }
  } else {
    // Send one tuple by default
    k = 1;
  }
  send_now = config["send_now"].length() > 0;
  return NO_ERR;
}

void
SendK::start() {
  if (send_now) {
    (*this)();
  }
  else {
    running = true;
    loopThread = shared_ptr<boost::thread>(new boost::thread(boost::ref(*this)));
  }
}


void
SendK::process(boost::shared_ptr<Tuple> t) {
  LOG(ERROR) << "Should not send data to a SendK";
} 


void
SendK::stop() {
  running = false;
  LOG(INFO) << "Stopping SendK operator " << id();
  if (running) {
    assert (loopThread->get_id()!=boost::this_thread::get_id());
    loopThread->join();
  }
}


void
SendK::operator()() {
  boost::shared_ptr<Tuple> t(new Tuple);
  t->add_e()->set_s_val("foo");
  for (u_int i = 0; i < k; i++) {
    emit(t);
  }
  no_more_tuples();
}



const string DummyReceiver::my_type_name("DummyReceiver operator");
const string SendK::my_type_name("SendK operator");


}