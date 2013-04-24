#include "experiment_operators.h"

#include <fstream>
#include <glog/logging.h>
#include "window_congest_mon.h"
#include "node.h"

using namespace std;
using namespace boost;

using namespace boost::posix_time;


namespace jetstream {

/*
void
DummyReceiver::process_delta (Tuple& oldV, boost::shared_ptr<Tuple> newV, const operator_id_t pred) {
  for (unsigned i = 0; i < tuples.size(); ++i) {
    boost::shared_ptr<Tuple> t = tuples[i];
      //TODO this may be sensitive to field order. This is probably okay.
    if (t->SerializeAsString() == oldV.SerializeAsString()) {
      tuples[i] = newV;
    }
  }
}*/


void
DummyReceiver::process( OperatorChain * chain,
                         std::vector< boost::shared_ptr<Tuple> > & in_t,
                         DataplaneMessage&) {
  if(store) {
    size_t cur_sz = tuples.size();
    tuples.reserve(cur_sz + in_t.size());
    for (int i = 0; i < in_t.size(); ++i)
      tuples.push_back(in_t[i]);
  }
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
ExperimentTimeRewrite::process_one(boost::shared_ptr<Tuple>& t) {

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

  if(first_tuple_t == 0) {
    first_tuple_t = old_ts;
    if (delta == 0)
      delta = time(NULL); //amount to shift by
  }
  time_t new_t = (old_ts - first_tuple_t) / warp + delta;
  
  if ( (emitted++ & 0xFFF) == 0 ) { // once every 256 tuples
    time_t now = time(0);
    if (new_t < now -1) {
      LOG(INFO) << "ExperimentTimeRewrite has fallen behind. Emitting " <<
        new_t << " at " << now;
    } else if (wait_for_catch_up && new_t > now + 1) {
      time_t diff = new_t - now - 1;
      js_usleep(diff * 1000 * 1000);
      LOG_EVERY_N(INFO, 5) << "ExperimentTimeRewrite stalling for catch-up";
    }
  }
  e->set_t_val( new_t);
}
  
operator_err_t
ExperimentTimeRewrite::configure(std::map<std::string,std::string> &config) {
  if ( !(istringstream(config["field"]) >> field)) {
    return operator_err_t("must specify an int as field; got " + config["field"] +  " instead");
  }

  if ( !(istringstream(config["warp"]) >> warp)) {
    return operator_err_t("must specify a timewarp as 'warp'.  " + config["field"] +  " instead");
  }

  if (config.find("delta") != config.end()) {
    if ( !(istringstream(config["delta"]) >> delta)) {
      return operator_err_t("Delta must be a timestamp, if set.  " + config["delta"] +  " instead");
    }    
  }
  
  wait_for_catch_up = false;
  if ( config.find("wait_for_catch_up") != config.end()) {
    wait_for_catch_up = (config["wait_for_catch_up"] != "false");
  }

  return NO_ERR;

}



void
CountLogger::process(boost::shared_ptr<Tuple> t) {

  tally_in_window += t->e(field).i_val();
  emit(t);
}

void
CountLogger::meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred) {

  if ( msg.type() == DataplaneMessage::END_OF_WINDOW) {
      time_t now = time(NULL);
      LOG(INFO) << " Tally in window ending at " << now << " "
        << tally_in_window << " or " << (1000 * tally_in_window) / msg.window_length_ms() << " per sec";
      tally_in_window = 0;
  }
  DataPlaneOperator::meta_from_upstream(msg, pred);
}


operator_err_t
CountLogger::configure(std::map<std::string,std::string> &config) {
  if ( !(istringstream(config["field"]) >> field)) {
    return operator_err_t("must specify an int as field; got " + config["field"] +  " instead");
  }
  return NO_ERR;  
}

void
AvgCongestLogger::meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred) {
  if ( msg.type() == DataplaneMessage::END_OF_WINDOW) {
    boost::lock_guard<boost::mutex> lock (mutex);
    window_for[pred] = msg.window_length_ms();
    
    if (msg.has_filter_level())
      sample_lev_for[pred] = msg.filter_level();
    
    if (msg.tput_r2_threshold())
      err_bound += msg.tput_r2_threshold();
  }
  DataPlaneOperator::meta_from_upstream(msg, pred); //delegate to base class
}

void
AvgCongestLogger::start() {
  running = true;
  timer = get_timer();
  timer->expires_from_now(boost::posix_time::millisec(report_interval));
  timer->async_wait(boost::bind(&AvgCongestLogger::report, this));

}

void
AvgCongestLogger::process(boost::shared_ptr<Tuple> t) {
  
  {
    boost::lock_guard<boost::mutex> lock (mutex);
    tuples_in_interval ++;
    if (field >= 0) {
      LOG_IF(FATAL, t->e_size() <= field) << "no such field " << field<< ". Got "
        << fmt(*t) << ".";
      count_tally += t->e(field).i_val();
    }

    if (hist_field >= 0) {
      LOG_IF(FATAL, t->e_size() <= hist_field) << "no such field " << hist_field<< ". Got "
        << fmt(*t) << ".";
      hist_size_total += t->e(hist_field).summary().histo().bucket_vals_size();
    }
    //release lock here
  }
  
  emit(t);
}


void
AvgCongestLogger::stop() {
  boost::lock_guard<boost::mutex> lock (mutex);
  running = false;
  timer->cancel();
}

void
AvgCongestLogger::report() {
  boost::lock_guard<boost::mutex> lock (mutex);

  if ( running ) {
    if (window_for.size() > 0) {
      unsigned total_windows_secs = 0;
      map<operator_id_t, unsigned>::iterator it = window_for.begin();
      for ( ; it != window_for.end(); ++it) {
        total_windows_secs += it->second;
      }
      double avg_window_secs = total_windows_secs / double(window_for.size());
      uint64_t bytes_total = node->bytes_in.read();
      double bytes_per_sec =  double(bytes_total - last_bytes) * 1000 / report_interval;
      double tuples_per_sec = tuples_in_interval * 1000.0 / report_interval;
      last_bytes = bytes_total;
      
      string maybe_h_stats = "";
      if (hist_field >= 0 && tuples_in_interval > 0)
        maybe_h_stats = " avg hist size " + boost::lexical_cast<string>( hist_size_total / tuples_in_interval );
      
      string maybe_sample_stats = "";
      double total_sample_ratios = 0;
      map<operator_id_t, double>::iterator sample_it = sample_lev_for.begin();
      for ( ; sample_it != sample_lev_for.end(); ++sample_it) {
        total_sample_ratios += sample_it->second;
      }
      
      if (total_sample_ratios > 0)
        maybe_sample_stats = " avg sample freq " +
          boost::lexical_cast<string>(   total_sample_ratios / sample_lev_for.size() );
      
      LOG(INFO) << "RootReport@ "<< time(NULL)<< " Avg window: " << avg_window_secs << " - " << bytes_per_sec
       << " bytes/sec " << tuples_per_sec << " tuples/sec"; // << " (bytes_total " << bytes_total << ")";
      LOG(INFO) << "Statistics: bytes_in=" << node->bytes_in.read() << "  bytes_out="<<node->bytes_out.read()
        << " Lifetime total count=" << count_tally << maybe_h_stats << maybe_sample_stats;
      
      
      LOG(INFO) << "Filter-error bound: " << err_bound;
      err_bound = 0;
      
      tuples_in_interval = 0;
      hist_size_total = 0;
      
    }
    timer->expires_from_now(boost::posix_time::millisec(report_interval));
    timer->async_wait(boost::bind(&AvgCongestLogger::report, this));
  }
}


operator_err_t
AvgCongestLogger::configure(std::map<std::string,std::string> &config) {
  if ( config.find("field") != config.end())
    if ( !(istringstream(config["field"]) >> field)) {
      return operator_err_t("must specify an int as field; got " + config["field"] +  " instead");
    }

  if ( config.find("hist_field") != config.end())
    if ( !(istringstream(config["hist_field"]) >> hist_field)) {
      return operator_err_t("must specify an int as hist_field; got " + config["hist_field"] +  " instead");
    }  
  
  return NO_ERR;
}


const string DummyReceiver::my_type_name("DummyReceiver operator");
const string ContinuousSendK::my_type_name("ContinuousSendK operator");
const string RateRecordReceiver::my_type_name("Rate recorder");
const string SerDeOverhead::my_type_name("Dummy serializer");
const string EchoOperator::my_type_name("Echo");
const string MockCongestion::my_type_name("Mock Congestion");
const string FixedRateQueue::my_type_name("Fixed rate queue");
const string ExperimentTimeRewrite::my_type_name("Time rewrite");
const string CountLogger::my_type_name("Count logger");
const string AvgCongestLogger::my_type_name("Avg. Congest Reporter");


}
