//
//  operator_chain.cpp
//  JetStream
//
//  Created by Ariel Rabkin on 4/5/13.
//  Copyright (c) 2013 Ariel Rabkin. All rights reserved.
//

#include <glog/logging.h>
#include "operator_chain.h"
#include "chain_ops.h"
#include "js_utils.h"

//using namespace ::std;
using std::string;
using std::vector;
using namespace boost;

namespace jetstream {

const string&
OperatorChain::chain_name() {

  if (cached_chain_name.size() == 0) {
    std::ostringstream buf;
    if (ops.size() ==0)
      buf << "empty chain";
    else {
      buf << ops.size() <<"-element chain starting at ";
      if (ops[0])
        buf << ops[0]->id_as_str();
      else
        buf << "null element";
    }
    cached_chain_name = buf.str();
  }
  return cached_chain_name;
}

void
OperatorChain::start() {

  running = true;
  if (ops.size() > 0 && ops[0]) {
  
    boost::shared_ptr<COperator> first_op = boost::dynamic_pointer_cast<COperator>(ops[0]);
//    LOG_IF(FATAL,!first_op)<< "chain can't start if head op is " << ops[0]->id_as_str();
    if (first_op) {
      first_op->start();
    }
    LOG(INFO) << "Starting " << chain_name();
  }
  
//  for (int i = 1; i < ops.size(); ++i) {
//    ops[i]->start();
//  }
}

OperatorChain::~OperatorChain() {
//  LOG(INFO) << "Deleting operator chain " << this;
}

void
OperatorChain::unblock(bool * stopped) {

//  LOG(INFO) << "calling notify";
  {
    unique_lock<boost::mutex> lock(stopwait_mutex);
    *stopped = true;
  }
  chainStopped.notify_all();
//  LOG(INFO) << "...notified";
}

void
OperatorChain::stop() {
  LOG(INFO) << "Stopping "<< chain_name()<< "; running is " << running;
  // << typename_as_str() << " operator " << id() << ". Running is " << running;
  if (running) {
    running = false;
//    boost::barrier b(2);
    {
      bool stopped = false;
      stop_async( boost::bind(&OperatorChain::unblock, this, &stopped) );
      {
        unique_lock<boost::mutex> lock(stopwait_mutex);
        if (!stopped)
          chainStopped.wait(lock);
      }
    }
    LOG(INFO) << "Stop of " << chain_name() << " complete";
  }
}

void
OperatorChain::stop_async(close_cb_t cb) {
  if (!strand) {
    LOG(WARNING) << "Can't stop gracefully, no strand for "<< chain_name();
    do_stop(cb);
  } else
    strand->dispatch(boost::bind(&OperatorChain::do_stop, this, cb));
}

void
OperatorChain::do_stop(close_cb_t cb) {
  if (ops.size() > 0 && ops[0]) {
    ops[0]->chain_stopping(this);
  }

  for (unsigned i = 1; i < ops.size(); ++i) {
    ops[i]->chain_stopping(this);
  }
//  LOG(INFO) << chain_name() << " called stop everywhere; invoking cb";
  cb();
}


/*
void
OperatorChain::unregister() {

  shared_ptr<ChainMember> src = ops[0];
  shared_ptr<COperator> src_op = dynamic_pointer_cast<COperator>(src);
  if( src_op) {
    src_op->unregister();
  }
}*/


boost::shared_ptr<CongestionMonitor>
OperatorChain::congestion_monitor() {
  return ops[ops.size() -1]->congestion_monitor();
}
  

void
OperatorChain::process(std::vector<boost::shared_ptr<Tuple> > & data_buf, DataplaneMessage& maybe_meta) {

   for (unsigned i = 1; i < ops.size(); ++i) {
     ChainMember * op = ops[i].get();
/*
     int nulls = 0;
     for (int j = 0; j < data_buf.size(); ++j)
      if (!data_buf[j])
        nulls++;
     LOG(INFO) << "Applying operator " << op->id_as_str() << " " << nulls << "empty cells"; */
     op->process(this, data_buf, maybe_meta);
   }
}

void
OperatorChain::clone_from(boost::shared_ptr<OperatorChain> source) {
  for (unsigned i = 0; i < source->ops.size(); ++i) {
    ops.push_back(source->ops[i]);
//    source->ops[i]->add_chain(newchain); //can't do this here because we don't have a shared point
  }
}

void
OperatorChain::upwards_metadata(jetstream::DataplaneMessage& m, jetstream::ChainMember* c) {
  int i = ops.size() -1;
  for (; i >=0; --i) {
    if (ops[i].get() == c)
      break;
  }
  LOG_IF(WARNING, i < 0) << "Got meta from unknown chain source " << c->id_as_str()
            <<":" << m.Utf8DebugString();
  for (; i >=0; --i) {
    if(ops[i])
      ops[i]->meta_from_downstream(m);
  }
}


}
