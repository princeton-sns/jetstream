
#include "threaded_source.h"

#include <glog/logging.h>


using namespace std;

namespace jetstream {

double congestSteps[] = {0, 1};

void
ThreadedSource::start() {
  if (send_now) {

    int is_running = 1;
    
    if( !congest_policy ) {
      congest_policy = boost::shared_ptr<CongestionPolicy>(new CongestionPolicy); //null policy
    }
    do {
      is_running += congest_policy->get_step(id(), congestSteps, 2, is_running);
      boost::this_thread::yield();
      js_usleep(100 * 1000);
      //don't loop here; need to re-check running
    } while (is_running < 1);

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
  LOG(INFO) << "Stopping " << typename_as_str() << " operator " << id() <<
    ". Running is " << running;
  if (running) {
    running = false;
    assert (loopThread->get_id() != boost::this_thread::get_id());
    loopThread->join();
  }
}


void
ThreadedSource::operator()() {
//  const int MAX_WAIT_TICKS = 10;
  int is_running = 1;
  
  if( !congest_policy ) {
    congest_policy = boost::shared_ptr<CongestionPolicy>(new CongestionPolicy); //null policy
  }
  
  do {
      is_running += congest_policy->get_step(id(), congestSteps, 2, is_running);
      if (is_running == 0) {
        boost::this_thread::yield();
        js_usleep(100 * 1000);
        //don't loop here; need to re-check running
      }
      else if (emit_1())
        break;
  } while (running); //running will be false if we're running synchronously
  
  LOG(INFO) << typename_as_str() << " " << id() << " done with " << emitted_count() << " tuples";
  if (exit_at_end && running)
    no_more_tuples();
  running = false; //no need to join with thread; it's exiting
}

void
ThreadedSource::end_of_window(msec_t duration) {
  DataplaneMessage end_msg;
  end_msg.set_type(DataplaneMessage::END_OF_WINDOW);
  end_msg.set_window_length_ms(duration);
  send_meta_downstream(end_msg);
  
}

}