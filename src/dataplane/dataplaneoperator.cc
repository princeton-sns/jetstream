#include "dataplaneoperator.h"
#include <iostream>

using namespace std;

jetstream::DataPlaneOperator::~DataPlaneOperator() 
{

}


void jetstream::DataPlaneOperator::process(shared_ptr<Tuple> t)
{
  cout << "Base Operator process" << endl;
}



void jetstream::DataPlaneOperator::emit(shared_ptr<Tuple> t)
{
  if(dest)
    dest->process(t);
  else
    cerr <<"WARN: no dest for operator "<<operID;
//  cout << "Base Operator emit" << endl;
}

