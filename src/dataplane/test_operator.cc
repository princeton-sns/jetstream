#include "dataplaneoperator.h"
#include<iostream>

#include <gtest/gtest.h>

namespace jetstream {

using namespace std;
  
  
class TestOperator: public jetstream::DataPlaneOperator {
 public:
  virtual void start(map<string,string> config) {
    cout << "test operator 2" << endl;
  }
};




extern "C" {
  DataPlaneOperator *maker(){
   return new TestOperator();
  }
}

}
