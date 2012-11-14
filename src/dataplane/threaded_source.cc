
#include "threaded_source.h"

#include <glog/logging.h>


using namespace std;

namespace jetstream {

void
ThreadedSource::start() {
  if (send_now) {
    boost::shared_ptr<CongestionMonitor> congested = congestion_monitor();
    congested->wait_for_space();
    while (! emit_1())
      ;
  }
  else {
    running = true;
    loopThread = boost::shared_ptr<boost::thread>(new boost::thread(boost::ref(*this)));
  }
}


void
ThreadedSource::process(boost::shared_ptr<Tuple> t) {
  LOG(FATAL) << "Should not send data to a fixed rate source";
} 


void
ThreadedSource::stop() {
  LOG(INFO) << "Stopping " << typename_as_str() << " operator " << id();
  if (running) {
    running = false;
    assert (loopThread->get_id()!=boost::this_thread::get_id());
    loopThread->join();
  }
}


void
ThreadedSource::operator()() {
  boost::shared_ptr<CongestionMonitor> congested = congestion_monitor();
  
  
  do {
    congested->wait_for_space();

    if (emit_1())
      break;
  } while (running); //running will be false if we're running synchronously
  
  LOG(INFO) << typename_as_str() << " " << id() << " done with " << emitted_count() << " tuples";
  if (exit_at_end)
    no_more_tuples();
}

}