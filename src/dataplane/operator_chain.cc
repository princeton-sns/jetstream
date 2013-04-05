//
//  operator_chain.cpp
//  JetStream
//
//  Created by Ariel Rabkin on 4/5/13.
//  Copyright (c) 2013 Ariel Rabkin. All rights reserved.
//

#include <glog/logging.h>
#include "operator_chain.h"
#include "js_utils.h"

using namespace ::std;

namespace jetstream {

const string&
OperatorChain::chain_name() {

  if (cached_chain_name.size() == 0) {
    cached_chain_name = "UNDEF";
  }
  return cached_chain_name;
}

void
OperatorChain::start() {

  running = true;
  loopThread = boost::shared_ptr<boost::thread>(new boost::thread(boost::ref(*this)));
}



void
OperatorChain::stop() {
  LOG(INFO) << "Stopping chain.";
  // << typename_as_str() << " operator " << id() << ". Running is " << running;
  if (running) {
    running = false;
    assert (loopThread->get_id() != boost::this_thread::get_id());
    loopThread->join();
  }
}



void
OperatorChain::operator()() {

  while (running) {
     vector<Tuple> data_buf(1000);
     DataplaneMessage maybe_meta;
     for (int i = 0; i < ops.size(); ++i) {
       COperator * op = ops[i];
//       op->process(data_buf, maybe_meta);
     }
  
  }
  
  running = false;
}


}
