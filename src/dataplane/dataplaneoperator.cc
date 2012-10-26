#include <iostream>
#include "dataplaneoperator.h"
#include "node.h"

#include <glog/logging.h>

using namespace std;
using namespace jetstream;

DataPlaneOperator::~DataPlaneOperator() 
{
  VLOG(1) << "destroying " << id();
}


void
DataPlaneOperator::process (boost::shared_ptr<Tuple> t)
{
  assert(t);
  LOG(INFO) << "Operator: base operator process" << endl;
}


void 
DataPlaneOperator::emit (boost::shared_ptr<Tuple> t)
{
  tuplesEmitted ++;
  if (dest)
    dest->process(t);
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

  if (dest != NULL) {
    dest->no_more_tuples();
    dest.reset(); //trigger destruction if no more pointers.
  }
  if (node != NULL) {
    node->stop_operator(operID); 
  }
}


void
OperatorCleanup::cleanup(boost::shared_ptr<DataPlaneOperator> op) {
  iosrv.post( boost::bind(&OperatorCleanup::cleanup_cb, this, op) );
}

void
OperatorCleanup::cleanup_cb(boost::shared_ptr<DataPlaneOperator> op) {
  LOG(INFO) << "destroying operator " << op->id();
  op.reset();
  //do nothing, quietly invokes destructor for op
  //this runs in the ioserv thread pool.
}

const string DataPlaneOperator::my_type_name("base operator");