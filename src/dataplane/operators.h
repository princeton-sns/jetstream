

#ifndef JetStream_operators_h
#define JetStream_operators_h

#include "dataplaneoperator.h"
#include <string>


using namespace std;

namespace jetstream {
  
class FileReadOperator: public DataPlaneOperator {
 public:
  virtual void start(map<string,string> config); //starts chugging forward

 protected:
  void loop_while_printing();
  string f_name;
  bool running;
};

}

#endif
