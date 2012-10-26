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
  
  boost::shared_ptr<CongestionMonitor> congested = congestion_monitor();
  
  
  for (u_int i = 0; i < k; i++) {
    while (congested->is_congested()) {
      boost::this_thread::yield();
      boost::this_thread::sleep(boost::posix_time::milliseconds(100));
//      LOG(INFO) << "BLOCKING waiting for congestion";
    }
    emit(t);
  }
  LOG(INFO) << "SendK " << id() << " done with " << k << " tuples";
  no_more_tuples();
}


operator_err_t
ContinuousSendK::configure (std::map<std::string,std::string> &config) {
  if (config["k"].length() > 0) {
    // stringstream overloads the '!' operator to check the fail or bad bit
    if (!(stringstream(config["k"]) >> k)) {
      LOG(WARNING) << "invalid number of tuples: " << config["k"] << endl;
      return operator_err_t("Invalid number of tuples: '" + config["k"] + "' is not a number.");
    }
  } else {
    // Send one tuple by default
    k = 1;
  }

  if (config["period"].length() > 0) {
    if (!(stringstream(config["period"]) >> period)) {
      LOG(WARNING) << "invalid send period (msecs): " << config["period"] << endl;
      return operator_err_t("Invalid send period (msecs) '" + config["period"] + "' is not a number.");
    }
  } else {
    // Wait one second by default
    period = 1000;
  }
  
  send_now = config["send_now"].length() > 0;
  return NO_ERR;
}

void
ContinuousSendK::start() {
  if (send_now) {
    (*this)();
  }
  else {
    running = true;
    loopThread = shared_ptr<boost::thread>(new boost::thread(boost::ref(*this)));
  }
}


void
ContinuousSendK::process(boost::shared_ptr<Tuple> t) {
  LOG(ERROR) << "Should not send data to a ContinuousSendK";
} 


void
ContinuousSendK::stop() {
  running = false;
  LOG(INFO) << "Stopping ContinuousSendK operator " << id();
  if (running) {
    assert (loopThread->get_id()!=boost::this_thread::get_id());
    loopThread->join();
  }
}


void
ContinuousSendK::operator()() {
  while (running) {
    boost::shared_ptr<Tuple> t(new Tuple);
    t->add_e()->set_s_val("foo");
    for (u_int i = 0; i < k; i++) {
      emit(t);
    }
    boost::this_thread::sleep(boost::posix_time::milliseconds(period));
  }
  no_more_tuples();
}


void
RateRecordReceiver::process(boost::shared_ptr<Tuple> t) {
  
  {
//    boost::lock_guard<boost::mutex> lock (mutex);
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
  if (running) {
    LOG(FATAL) << "Should only start() once";
  }
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
    long tuples_in_win = 0;
    ptime wake_time = microsec_clock::local_time();
    {
      boost::lock_guard<boost::mutex> lock (mutex);
      
      long sleeptime = (wake_time - window_start).total_milliseconds();
      tuples_per_sec = (tuples_in_window * 1000.0) / sleeptime;
      bytes_per_sec = (bytes_in_window * 1000.0) / sleeptime;
      
      tuples_in_win = tuples_in_window;
      
      tuples_in_window = 0;
      bytes_in_window = 0;
      
      window_start = wake_time;
    }
    LOG(INFO) << tuples_in_win << " tuples this period. "<< bytes_per_sec <<
       " bytes per second, " << tuples_per_sec << " tuples per sec";
    

  }
}

int BUFSZ = 10000;
char* buf = new char [BUFSZ];
void
SerDeOverhead::process(boost::shared_ptr<Tuple> t) {
  int len = t->ByteSize();
  assert (len < BUFSZ);
//  char* buf = new char[len];
  t->SerializeToArray(buf, len);
  
  boost::shared_ptr<Tuple> t2 = boost::shared_ptr<Tuple>(new Tuple);
  t2->ParseFromArray(buf, len);
  emit(t2);
}

void
EchoOperator::process(boost::shared_ptr<Tuple> t) {
  cout << id() <<": " <<fmt(*t) << endl;

  if (get_dest() != NULL)
    emit(t);
}


const string DummyReceiver::my_type_name("DummyReceiver operator");
const string SendK::my_type_name("SendK operator");
const string ContinuousSendK::my_type_name("ContinuousSendK operator");
const string RateRecordReceiver::my_type_name("Rate recorder");
const string SerDeOverhead::my_type_name("Dummy serializer");
const string EchoOperator::my_type_name("Echo");



}
