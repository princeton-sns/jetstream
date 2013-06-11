#include <iostream>
#include "dataplaneoperator.h"
#include "node.h"

#include <glog/logging.h>
#include <boost/date_time/posix_time/posix_time.hpp>

using namespace std;
using namespace jetstream;
using namespace boost::posix_time;


void
DataPlaneOperator::send_meta_downstream(const DataplaneMessage & msg) {
  if ( dest ) {
    dest->meta_from_upstream(msg, operID);
  }
}



DataPlaneOperator::~DataPlaneOperator() 
{
  VLOG(1) << "destroying " << id();
}


void
DataPlaneOperator::process (boost::shared_ptr<Tuple> t, const operator_id_t src) {
  process(t);
}

void
DataPlaneOperator::process (boost::shared_ptr<Tuple> t) {
  assert(t);
  LOG(INFO) << "Operator: base operator process" << endl;
}

void
DataPlaneOperator::process_delta (Tuple& oldV, boost::shared_ptr<Tuple> newV, const operator_id_t pred) {

  if (dest)
    dest->process_delta(oldV, newV, operID);
  else
    LOG(WARNING) << "Operator: no destination for operator " << operID << endl;
}

void 
DataPlaneOperator::emit (boost::shared_ptr<Tuple> t)
{
  tuplesEmitted ++;
  if (dest)
    dest->process(t, operID);
  else
    LOG(WARNING) << "Operator: no destination for operator " << operID << endl;
}

void
DataPlaneOperator::emit (Tuple& old, boost::shared_ptr<Tuple> newV) {
  if (dest)
    dest->process_delta(old, newV, operID);
}


boost::shared_ptr<CongestionMonitor>
DataPlaneOperator::congestion_monitor() {
  if(dest)
    return dest->congestion_monitor();
  else {
    //LOG(FATAL) << "Every chain-ending operator should have a congestion monitor";
    return boost::shared_ptr<CongestionMonitor>(new UncongestedMonitor);
  }
}


void
DataPlaneOperator::no_more_tuples () {
  VLOG(1) << "no more tuples for " << id();
  if (dest != NULL) {
    DataplaneMessage msg;
    msg.set_type(DataplaneMessage::NO_MORE_DATA);
    dest->meta_from_upstream(msg, id());
    dest->remove_pred( id());
    dest.reset(); //trigger destruction if no more pointers.
  }
  if (node != NULL) {
    //can't stop here -- causes a cycle. Also probably unnecessary.
//    node->stop_operator(operID);
  }
}


void
DataPlaneOperator::chain_is_broken () {

  if (pred != NULL) {
    pred->chain_is_broken();
  }
    //note that we recurse before stopping. So the stops will happen front-to-back.
  if (node != NULL) {
//    node->stop_operator(operID);
  }
}



void
DataPlaneOperator::meta_from_downstream(const DataplaneMessage &msg) {
  if (pred != NULL)
    pred->meta_from_downstream(msg);
}

void
DataPlaneOperator::meta_from_upstream(const DataplaneMessage & msg, operator_id_t pred) {
  if (msg.type() == DataplaneMessage::NO_MORE_DATA)
    no_more_tuples();
  else if (dest != NULL)
    dest->meta_from_upstream(msg, operID);
}


boost::shared_ptr<boost::asio::deadline_timer>
DataPlaneOperator::get_timer() {
  return node->get_timer();
}

void
OperatorCleanup::cleanup(boost::shared_ptr<DataPlaneOperator> op) {
  cleanup_strand.post( boost::bind(&OperatorCleanup::cleanup_cb, this, op) );
}

void
OperatorCleanup::cleanup_cb(boost::shared_ptr<DataPlaneOperator> op) {
  boost::shared_ptr<DataPlaneOperator> no_ptr;
  op->set_dest( no_ptr );
  op->clear_preds();
  LOG(INFO) << "destroying operator " << op->id();
  op.reset();
  //do nothing, quietly invokes destructor for op
  //this runs in the ioserv thread pool.
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
RateRecordReceiver::long_description() const {
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
  window_start = boost::posix_time::microsec_clock::local_time();
  while (running)  {
  //sleep
    boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
    long tuples_in_win = 0;
    ptime wake_time = boost::posix_time::microsec_clock::local_time();
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


const string DataPlaneOperator::my_type_name("base operator");
const string xDummyReceiver::my_type_name("Legacy DummyReceiver");
const string RateRecordReceiver::my_type_name("Rate recorder");
const string MockCongestion::my_type_name("Mock Congestion");
const string CountLogger::my_type_name("Count logger");

