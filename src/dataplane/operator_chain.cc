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
  for (int i = 0; i < ops.size(); ++i) {
    ops[i]->start();
  }
//  loopThread = boost::shared_ptr<boost::thread>(new boost::thread(boost::ref(*this)));
}



void
OperatorChain::stop() {
  LOG(INFO) << "Stopping chain.";
  // << typename_as_str() << " operator " << id() << ". Running is " << running;
  if (running) {
    running = false;
    boost::barrier b(1);

    stop_async( boost::bind(&boost::barrier::wait, &b) );
    b.wait();
    
  }
}

void
OperatorChain::stop_async(close_cb_t cb) {
  if (!strand) {
    LOG(INFO) << "Can't stop, chain was never started";
  } else
    strand->wrap(boost::bind(&OperatorChain::do_stop, this, cb));
}

void
OperatorChain::do_stop(close_cb_t cb) {
  for (int i = 0; i < ops.size(); ++i) {
    ops[i]->stop();
  }
  LOG(INFO) << " called stop everywhere; invoking cb";
  cb();
}



void
OperatorChain::process(std::vector<boost::shared_ptr<Tuple> > & data_buf, DataplaneMessage& maybe_meta) {

   for (int i = 1; i < ops.size(); ++i) {
     COperator * op = ops[i].get();
     op->process(this, data_buf, maybe_meta);
   }
}


}
