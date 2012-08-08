#include "dataplaneoperator.h"
#include<iostream>

namespace jetstream {

using namespace std;
class TestOperator: public jetstream::DataPlaneOperator {
 public:
  void execute(){
    cout << "test operator 2" << endl;
  }
};




extern "C" {
  DataPlaneOperator *maker(){
   return new TestOperator();
  }
}

}
