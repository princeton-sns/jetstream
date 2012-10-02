#include <iostream>
#include "dataplaneoperator.h"
#include "node.h"

#include <glog/logging.h>

using namespace std;
using namespace jetstream;

DataPlaneOperator::~DataPlaneOperator() 
{

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


void
DataPlaneOperator::no_more_tuples () {

  if (dest != NULL) {
    dest->no_more_tuples();
    dest.reset(); //trigger destruction.
  }
  if (node != NULL) {
    node->stop_operator(operID); 
  }
}


const string DataPlaneOperator::my_type_name("base operator");