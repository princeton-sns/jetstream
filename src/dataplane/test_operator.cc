#include "operator_chain.h"
#include <iostream>

#include <gtest/gtest.h>

namespace jetstream {

using namespace std;
  
/**
*  This class is used for testing the dynamic-loading of user-defined operators.
* It should not have any code beyond the TestOperator defined here.
*/

static std::string tname("Test operator");


class TestOperator: public jetstream::COperator {

 public:
  virtual operator_err_t configure(std::map<std::string, std::string> &) {return C_NO_ERR;}
  
  virtual void start() {
    cout << "test operator started" << endl;
  }
  
  virtual void process(OperatorChain * chain, std::vector<boost::shared_ptr<Tuple> > &, DataplaneMessage&) {}
  
  virtual const std::string& typename_as_str() {
      return tname;
  }

  virtual bool is_source() {return true;}  
  
  
};


extern "C" {
  COperator *maker(){
   return new TestOperator();
  }
}

}
