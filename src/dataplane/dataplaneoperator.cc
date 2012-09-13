#include <iostream>
#include "dataplaneoperator.h"

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
  if (dest)
    dest->process(t);
  else
    LOG(WARNING) << "Operator: no destination for operator " << operID << endl;

  //  cout << "Base Operator emit" << endl;
}

const string DataPlaneOperator::my_type_name("base operator");