#include "chain_ops.h"
#include <algorithm>
#include "node.h"
#include "base_operators.h"

using namespace ::std;
using namespace boost;

namespace jetstream {


void
COperator::unregister() {
  if (node)
    node->unregister_operator(id());
}


void
TimerSource::process(OperatorChain * chain, std::vector<boost::shared_ptr<Tuple> > & d, DataplaneMessage&) {
  LOG(WARNING) << "Shouldn't be sending data to a timer source base process() method";
  return;
}


void
TimerSource::start() {
  LOG(INFO) << "timer-based operator " << id() << " of type " << typename_as_str() << " starting";
  running = true;
  timer = node->get_timer();
  st = node->get_new_strand();
  chain->strand = st.get();
  timer->expires_from_now(boost::posix_time::seconds(0));
  timer->async_wait(st->wrap(boost::bind(&TimerSource::emit_wrapper, this)));
}

void
TimerSource::emit_wrapper() {
  if (running) {
    int delay_to_next = emit_data();
    if (delay_to_next >= 0) {
      timer->expires_from_now(boost::posix_time::millisec(delay_to_next));
      timer->async_wait(st->wrap(boost::bind(&TimerSource::emit_wrapper, this)));
    } else {
      LOG(INFO)<< "EOF; should tear down";
      running = false;
      chain->do_stop(no_op_v); // on thread, can call this
      chain.reset();
      node->unregister_operator(id()); // will trigger destructor for this!
    }
  }
}


void
TimerSource::stopping() {
  bool was_running = running;
  running = false;
  if (was_running) {
    timer->cancel();
  }
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
  
  for (int i =0 ; i < tuples.size(); ++i ) {
    boost::shared_ptr<Tuple> t = tuples[i];
    process_one(t);
  }
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
  
  n = 0; // number sent
  
  return NO_ERR;
}


int
SendK::emit_data() {

  vector<shared_ptr<Tuple> > tuples;
  DataplaneMessage no_meta;

  t = boost::shared_ptr<Tuple>(new Tuple);
  t->add_e()->set_s_val("foo");
  t->set_version(n);
  tuples.push_back(t);
  chain->process(tuples, no_meta);
//  cout << "sendk. N=" << n<< " and k = " << k<<endl;
  return (++n < k) ? 0 : -1;
}



const string SendK::my_type_name("SendK operator");


}

