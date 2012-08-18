

#ifndef JetStream_operators_h
#define JetStream_operators_h

#include "dataplaneoperator.h"
#include <string>
#include <boost/regex.hpp>

namespace jetstream {
  
/***
 * Operator for reading lines from a file. Expects one parameter, a string named
 * 'file'. Emits tuples with one element, a string corresponding to a line from the
 * file. The carriage return at the end of line is NOT included.
 */
class FileRead: public DataPlaneOperator {
 public:
  virtual void start(std::map<std::string,std::string> config);
  void operator()(); //a thread that will loop while reading the file

 protected:
  std::string f_name; //name of file to read
  bool running;
};

/***
 * Operator for filtering strings. Expects one parameter, a string named 'pattern'
 * containing a regular expression. Assumes each received tuple has a first element
 * that is a string, and re-emits the tuple if the string matches 'pattern'.
 */

class StringGrep: public DataPlaneOperator {
 public:
  virtual void start(std::map<std::string,std::string> config);
  virtual void process(boost::shared_ptr<Tuple> t);
 protected:
  boost::regex re; // regexp pattern to match tuples against
  int id;//the field on which to filter
};

  
class DummyReceiver: public DataPlaneOperator {
public:
  std::vector<Tuple> tuples;
  virtual void process(boost::shared_ptr<Tuple> t) {
    tuples.push_back(*t);
  }
};
  
  
}

#endif
