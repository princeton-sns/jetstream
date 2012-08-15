#include <iostream>
#include "dataplaneoperator.h"

using namespace jetstream;

DataPlaneOperator::~DataPlaneOperator() 
{

}


void
DataPlaneOperator::process (boost::shared_ptr<Tuple> t)
{
  std::cout << "Base Operator process" << std::endl;
}


void 
DataPlaneOperator::emit (boost::shared_ptr<Tuple> t)
{
  if (dest)
    dest->process(t);
  else
    std::cerr <<"WARN: no dest for operator " << operID << std::endl;
  //  cout << "Base Operator emit" << endl;
}

