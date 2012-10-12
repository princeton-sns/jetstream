#include "experiment_operators.h"

#include <glog/logging.h>

using namespace std;
using namespace boost;

using namespace boost::posix_time;


namespace jetstream {


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


void
RateRecordReceiver::process(boost::shared_ptr<Tuple> t) {
  {
    boost::lock_guard<boost::mutex> lock (mutex);
    tuples_in_window ++;
    bytes_in_window += t->ByteSize();
  }
  if (get_dest() != NULL)
    emit(t);//pass forward
}
  
std::string
RateRecordReceiver::long_description() {
  boost::lock_guard<boost::mutex> lock (mutex);

  ostringstream out;
  out << bytes_per_sec << " bytes per second, " << tuples_per_sec << " tuples";
  return out.str();
}
  
void
RateRecordReceiver::start() {
  running = true;
  loopThread = shared_ptr<boost::thread>(new boost::thread(boost::ref(*this)));
}

void
RateRecordReceiver::stop() {
  running = false;
  LOG(INFO) << "Stopping RateRecordReceiver operator " << id();
  if (running) {
    assert (loopThread->get_id()!=boost::this_thread::get_id());
    loopThread->join();
  }
}


void
RateRecordReceiver::operator()() {
  window_start = microsec_clock::local_time();
  while (running)  {
  //sleep
    boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
    ptime wake_time = microsec_clock::local_time();
    {
      boost::lock_guard<boost::mutex> lock (mutex);
      
      tuples_per_sec = (tuples_in_window * 1000.0) / (wake_time - window_start).total_milliseconds();
      bytes_per_sec = (bytes_in_window * 1000.0) / (wake_time - window_start).total_milliseconds();
      tuples_in_window = bytes_in_window = 0;
      
      window_start = wake_time;
    }
  }
}



const string DummyReceiver::my_type_name("DummyReceiver operator");
const string SendK::my_type_name("SendK operator");
const string RateRecordReceiver::my_type_name("Rate recorder");


}