#include "dataplaneoperator.h"
#include <iostream>

using namespace std;

jetstream::DataPlaneOperator::~DataPlaneOperator() 
{

}


void 
jetstream::DataPlaneOperator::process(tuple_t t)
{
  cout << "Base Operator process" << endl;
}


void 
jetstream::DataPlaneOperator::emit(tuple_t t)
{
  if (dest)
    dest->process(t);
  else
    cerr << "WARN: no dest for operator "<<operID;
  cout << "Base Operator emit" << endl;
}



