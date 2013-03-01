#include "experiment_operators.h"

#include <fstream>
#include <glog/logging.h>
#include "window_congest_mon.h"

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
  exit_at_end = config["exit_at_end"].length() == 0 || config["exit_at_end"] != "false";
  
  n = 1; // number sent
  
  return NO_ERR;
}


bool
SendK::emit_1() {
  t = boost::shared_ptr<Tuple>(new Tuple);
  t->add_e()->set_s_val("foo");
  t->set_version(n);

  emit(t);
//  cout << "sendk. N=" << n<< " and k = " << k<<endl;
  return (++n > k);
  
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
  
//  send_now = config["send_now"].length() > 0;
  t = boost::shared_ptr<Tuple>(new Tuple);
  t->set_version(num_sent);
  t->add_e()->set_s_val("foo");  
  
  return NO_ERR;
}


bool
ContinuousSendK::emit_1() {
//  cout << " continuous-send is sending" << endl;

  t->set_version(num_sent++ );
  emit(t);
  boost::this_thread::sleep(boost::posix_time::milliseconds(period));
  
  return false; //never break out of loop
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
  (*o) << id() <<": " <<fmt(*t) << endl;

  if (get_dest() != NULL)
    emit(t);
}


operator_err_t
EchoOperator::configure(std::map<std::string,std::string> &config) {

  string out_file_name = config["file_out"];
  if ( out_file_name.length() > 0) {
    bool clear_file = (config["append"].length() > 0) && (config["append"] != "false");
    LOG(INFO) << "clear_file is " << clear_file;
    o = new ofstream(out_file_name.c_str(), (clear_file ? ios_base::out : ios_base::ate | ios_base::app));
  }
  return NO_ERR;
}

EchoOperator::~EchoOperator() {
  if (o != &std::cout) {
    ((ofstream*)o)->close();
    delete o;
  }
}


class MyMon: public CongestionMonitor {
  public:
    MockCongestion & m;
    MyMon(MockCongestion & m2): CongestionMonitor("mock"),m(m2) {}
  
    virtual double capacity_ratio() {
      return m.congestion;
    }
};


MockCongestion::MockCongestion(): congestion(INFINITY) {
  mon = boost::shared_ptr<CongestionMonitor>(new MyMon(*this));
}


operator_err_t
FixedRateQueue::configure(std::map<std::string,std::string> &config) {
  if (config["ms_wait"].length() > 0) {
    // stringstream overloads the '!' operator to check the fail or bad bit
    if (!(stringstream(config["ms_wait"]) >> ms_per_dequeue)) {
      LOG(WARNING) << "invalid time_wait per dequeue: " << config["ms_wait"]<< " for " << id() << endl;
      return operator_err_t("Invalid time_wait per dequeue: '" + config["ms_wait"] + "' is not a number.");
    }
  } else
    return operator_err_t(id().to_string() + ": Must set parameter ms_wait");
//    ms_per_dequeue = 500;


  if (config["mon_type"] == "queue") {
    int max_q_len = 0;
    if (config["queue_length"].length() > 0) {
      // stringstream overloads the '!' operator to check the fail or bad bit
      if (!(stringstream(config["queue_length"]) >> max_q_len)) {
        LOG(WARNING) << "invalid queue length: " << config["queue_length"]<< " for " << id() << endl;
        return operator_err_t("Invalid  queue length: '" + config["queue_length"] + "' is not a number.");
      }
    } else
      return operator_err_t(id().to_string() + ": Must set parameter queue_length");
  
    mon = boost::shared_ptr<NetCongestionMonitor>(new QueueCongestionMonitor(max_q_len, id().to_string()));
  } else
    mon = boost::shared_ptr<NetCongestionMonitor>(new WindowCongestionMonitor(id().to_string()));
  
  return NO_ERR;
}

void
FixedRateQueue::start() {
  running = true;
  timer = get_timer();
  timer->expires_from_now(boost::posix_time::millisec(ms_per_dequeue));
  timer->async_wait(boost::bind(&FixedRateQueue::process1, this));
}

void
FixedRateQueue::stop() {
//  cout << "in FixedRateQueue stop" << endl;
  boost::lock_guard<boost::mutex> lock (mutex);
  running = false;
  timer->cancel();
  while (!q.empty())
    q.pop();
//  cout << "FixedRateQueue stopped" << endl;
}



void
FixedRateQueue::process(boost::shared_ptr<Tuple> t) {
  boost::lock_guard<boost::mutex> lock (mutex);
  LOG_IF(FATAL, !t) << "didn't expect a null tuple here";
  DataplaneMessage msg;
  Tuple * t2 = msg.add_data();
  t2->CopyFrom(*t);
  q.push(msg);
}

void
FixedRateQueue::meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred) {
//  boost::shared_ptr<DataplaneMessage> msg2(new DataplaneMessage);
//  msg2->CopyFrom(msg);
  boost::lock_guard<boost::mutex> lock (mutex);
  q.push(msg);
/*  if ( msg.type() == DataplaneMessage::END_OF_WINDOW) {
    window_start = mon->get_window_start();
    mon->new_window_start();
  }*/
}


void
FixedRateQueue::process1() {
  if (!running) {
    return; //spurious wakeup, e.g. on close
  }
  
  {
    boost::lock_guard<boost::mutex> lock (mutex);
    //deqeue
    DataplaneMessage msg;
    
    if (! q.empty()) {
      msg = q.front();
      q.pop();
    }
  //  cout << "dequeue, length = " << q.size() << endl;
    if( msg.data_size() > 0) {
      boost::shared_ptr<Tuple> t(new Tuple);
      t->CopyFrom(msg.data(0));
      mon->report_insert(t.get(), 1);
      emit(t);
      mon->report_delete(t.get(), 1);      
    } else {
      if ( msg.type() == DataplaneMessage::END_OF_WINDOW) {
        mon->end_of_window(msg.window_length_ms(), mon->get_window_start());
        mon->new_window_start();
      }
      DataPlaneOperator::meta_from_upstream(msg, id()); //delegate to base class
    }

    timer->expires_from_now(boost::posix_time::millisec(ms_per_dequeue));
    timer->async_wait(boost::bind(&FixedRateQueue::process1, this));
  }
}



void
ExperimentTimeRewrite::process(boost::shared_ptr<Tuple> t) {

  Element * e = t->mutable_e(field);
  double old_ts = 0;
  if (e->has_d_val()) {
    old_ts = e->d_val();
    e->clear_d_val();
  } else if (e->has_t_val()) {
    old_ts = e->t_val();
    e->clear_t_val();
  } else {
    DLOG(FATAL) << "can't timestamp tuple " << fmt(*t);
    return;
  }

  time_t new_t = old_ts * warp + delta;
  int emitted = emitted_count();
  
  if(emitted == 0)
    delta = time(NULL);
  else
    if ( (emitted & 0xFF) == 0 ) { // once every 256 tuples
    if (new_t < time(0) -1)
      LOG(INFO) << "ExperimentTimeRewrite has fallen behind";
  }
  e->set_t_val( new_t);
  
  emit(t);

}
  
operator_err_t
ExperimentTimeRewrite::configure(std::map<std::string,std::string> &config) {
  if ( !(istringstream(config["field"]) >> field)) {
    return operator_err_t("must specify an int as field; got " + config["field"] +  " instead");
  }

  if ( !(istringstream(config["warp"]) >> warp)) {
    return operator_err_t("must specify a timewarp as 'warp'.  " + config["field"] +  " instead");
  }
  return NO_ERR;

}


const string DummyReceiver::my_type_name("DummyReceiver operator");
const string SendK::my_type_name("SendK operator");
const string ContinuousSendK::my_type_name("ContinuousSendK operator");
const string RateRecordReceiver::my_type_name("Rate recorder");
const string SerDeOverhead::my_type_name("Dummy serializer");
const string EchoOperator::my_type_name("Echo");
const string MockCongestion::my_type_name("Mock Congestion");
const string FixedRateQueue::my_type_name("Fixed rate queue");
const string ExperimentTimeRewrite::my_type_name("Time rewrite");

}
