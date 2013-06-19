#include "chain_ops.h"
#include <algorithm>
#include "node.h"
#include "base_operators.h"

using namespace ::std;
using namespace boost;

namespace jetstream {


COperator::~COperator() {
//  unregister();  //TODO: WHY DOES THIS CAUSE A CRASH IF UNCOMMENTED? Unregister should be idempotent.
}

/*
void
COperator::unregister() {
  if (node)
    node->unregister_operator(id());
}*/

void
TimerSource::process(OperatorChain * chain, std::vector<boost::shared_ptr<Tuple> > & d, DataplaneMessage&) {
  LOG(WARNING) << "Shouldn't be sending data to a timer source base process() method";
  return;
}


void
TimerSource::start() {
  if (!running) {
    LOG(INFO) << "timer-based operator " << id() << " of type " << typename_as_str() << " starting";
    running = true;
    timer = node->get_timer();
    st = node->get_new_strand();
  //  LOG_IF(FATAL, !st) << "Can't get strand from node. Is the node started?";
    chain->strand = st.get();
    timer->expires_from_now(boost::posix_time::seconds(0));
    timer->async_wait(st->wrap(boost::bind(&TimerSource::emit_wrapper, this)));
  }
}

void
TimerSource::emit_wrapper() {
  if (running) {
    int delay_to_next = emit_data();
    if (delay_to_next >= 0) {
      timer->expires_from_now(boost::posix_time::millisec(delay_to_next));
      timer->async_wait(st->wrap(boost::bind(&TimerSource::emit_wrapper, this)));
      return;
    } else {
      LOG(INFO)<< "Source " << id() << " has no more tuples; will tear down";
      running = false;
      shared_ptr<OperatorChain> c  = chain;
      chain->stop_from_within(); // on thread, can call this; note it will call chain-stopping
      node->unregister_chain(c);
      c.reset();// will trigger destructor for this!
    }
  }
}


void
TimerSource::chain_stopping(OperatorChain *) {
  bool was_running = running;
  running = false;
  if (was_running) {
    timer->cancel();
  }
  chain.reset(); //so that we stop if invoked externally.
}

TimerSource::~TimerSource() {
  chain.reset();
  if (timer && running)
    timer->cancel();
}

void
CEachOperator::process ( OperatorChain * c,
                          std::vector<boost::shared_ptr<Tuple> > & tuples,
                          DataplaneMessage& msg) {
  
  for (unsigned i =0 ; i < tuples.size(); ++i ) {
//    boost::shared_ptr<Tuple> t = ;
    if(tuples[i])
      process_one(tuples[i]);
  }
  meta_from_upstream(c, msg);
}


void
CFilterOperator::process(OperatorChain * chain,
                       std::vector<boost::shared_ptr<Tuple> > & tuples,
                       DataplaneMessage& msg) {
  unsigned out_idx = 0;
  for(unsigned i = 0; i < tuples.size(); ++i) {
    if (should_emit(*tuples[i])) {
      tuples[out_idx++] = tuples[i];
    }
  }
  
  if (msg.type() == DataplaneMessage::END_OF_WINDOW) {
    end_of_window(msg);
  }
  
  tuples.resize(out_idx);
}
  

}

