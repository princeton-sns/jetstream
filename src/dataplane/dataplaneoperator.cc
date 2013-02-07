#include <iostream>
#include "dataplaneoperator.h"
#include "node.h"

#include <glog/logging.h>

using namespace std;
using namespace jetstream;

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
DataPlaneOperator::emit (boost::shared_ptr<Tuple> t)
{
  tuplesEmitted ++;
  if (dest)
    dest->process(t, operID);
  else
    LOG(WARNING) << "Operator: no destination for operator " << operID << endl;
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
    node->stop_operator(operID); 
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

const string DataPlaneOperator::my_type_name("base operator");