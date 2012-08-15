

#ifndef JetStream_operators_h
#define JetStream_operators_h

#include "dataplaneoperator.h"
#include <string>


using namespace std;

namespace jetstream {
  
/***
 * Operator for reading lines from a file. Expects one parameter, a string named
 * 'file'. Emits tuples with one element, a string corresponding to a line from the
 * file. The carriage return at the end of line is NOT included.
 */
class FileReadOperator: public DataPlaneOperator {
 public:
  virtual void start(map<string,string> config); 
  void operator()(); //a thread that will loop while reading the file

 protected:
  string f_name; //name of file to read
  bool running;
};

}

#endif
