#ifndef JetStream_operators_h
#define JetStream_operators_h

#include "dataplaneoperator.h"
#include <string>
#include <iostream>
#include <boost/regex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/random.hpp>


namespace jetstream {
  
/***
 * Operator for reading lines from a file. Expects one parameter, a string named
 * 'file'. Emits tuples with one element, a string corresponding to a line from the
 * file. The carriage return at the end of line is NOT included.
 */
class FileRead: public DataPlaneOperator {
 public:
  //TODO: Make some of these part of DataPlaneOperator API? Or define a base class
  //for source operators?
  FileRead() : running(false) {}
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  virtual void start();
  virtual void stop();
  void operator()();  // A thread that will loop while reading the file
  bool isRunning();
  virtual void process(boost::shared_ptr<Tuple> t);  

  virtual std::string long_description();

 protected:
  std::string f_name; //name of file to read
  boost::shared_ptr<boost::thread> loopThread;
  volatile bool running;

GENERIC_CLNAME
};


/***
 * Operator for filtering strings. Expects one parameter, a string named 'pattern'
 * containing a regular expression. Assumes each received tuple has a first element
 * that is a string, and re-emits the tuple if the string matches 'pattern'.
 */
class StringGrep: public DataPlaneOperator {
 public:
  StringGrep() : fieldID (0) {}
  virtual operator_err_t configure (std::map<std::string,std::string> &config);
  virtual void process (boost::shared_ptr<Tuple> t);
  virtual std::string long_description();

 protected:
  boost::regex re; // regexp pattern to match tuples against
  int fieldID; // the field on which to filter

 GENERIC_CLNAME
};


/**  Parses strings in tuples. 
 *  Takes three params: field_to_parse, pattern, types
 * If 'field_to_parse' = x, then given a tuple (t0,t1...t_x,t_x+1,...), will 
 *  produce (t0,t1...t_y,t_y2,...,t_x+1, ...). In other words, the params before
 * and after 'field_to_parse' are kept, and the field to parse is expanded.
 *
 *  pattern should be a regex with groups in it. The types param should be a string
 * with one char per regex group and corresponds to the type of the group elems.
 * [S = string, I = Int, D = double]
 *
 *  Behavior is un-specified if the regex doesn't match.
 *  NOTE THAT FIELDS ARE NUMBERED FROM ZERO
 */
class GenericParse: public DataPlaneOperator {

 public:
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  virtual void process(boost::shared_ptr<Tuple> t);

 protected:
  boost::regex re; // regexp pattern to match tuples against
  std::string field_types;
  int fld_to_parse;
  
 GENERIC_CLNAME
};

/** Use a typecode [char] to [parse a string into an element. Shared by GenericParse
and by Extend */
void parse_with_types(Element * e, const std::string& s, char typecode);
  

/**
 * Adds constant data to a tuple.
 *   Values should be named "0"..."9".
 *    If you need to add more than ten values, use two ExtendOperators!
 * Values should be parallel to a field, named types, with same syntax as
 * for the GenericParse operator.
 *  The value ${HOSTNAME} is special; it will be replaced with the host name at 
 * configuration time. 
 
*/
class ExtendOperator: public DataPlaneOperator {
 public:
  std::vector< Element > new_data;
  virtual void process (boost::shared_ptr<Tuple> t);
  virtual operator_err_t configure (std::map<std::string,std::string> &config);

  
  virtual ~ExtendOperator() {};

GENERIC_CLNAME
};

/***
 * Given a data stream, allows some fraction of data through.
 * Config options: seed [an int] and fraction [ a float], representing the fraction
 * to drop.  (So fraction == 0 means 'allow all')
 */
class SampleOperator: public DataPlaneOperator {
 public:
  boost::random::mt19937 gen;
  uint32_t threshold; //drop tuples if rand >= threshhold. So thresh = 0 means pass all
  virtual void process (boost::shared_ptr<Tuple> t);
  virtual operator_err_t configure (std::map<std::string,std::string> &config);

  
  virtual ~SampleOperator() {};

GENERIC_CLNAME
};


/**
 Rearranges the order of elements in a tuple

class Rearrange: public DataPlaneOperator {
 public:
  std::vector< int > new_positions;
  virtual void process(boost::shared_ptr<Tuple> t);
  virtual void configure(std::map<std::string,std::string> &config);

  
  virtual ~Rearrange();

GENERIC_CLNAME
};*/

}

#endif