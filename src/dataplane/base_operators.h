#ifndef JetStream_operators_h
#define JetStream_operators_h

#include "dataplaneoperator.h"
#include "threaded_source.h"


#include <string>
#include <iostream>
#include <fstream>
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
class FileRead: public ThreadedSource {
 public:
  //TODO: Make some of these part of DataPlaneOperator API? Or define a base class
  //for source operators?
  FileRead():lineno(0) {}
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  virtual bool emit_1();  // A thread that will loop while reading the file
  virtual void process(boost::shared_ptr<Tuple> t);  

  virtual std::string long_description();

 protected:
  std::string f_name; //name of file to read
  bool skip_empty; // option: skip empty lines
  std::ifstream in_file;
  unsigned lineno;

GENERIC_CLNAME
};


class CSVParse: public DataPlaneOperator {
  public:
    virtual operator_err_t configure (std::map<std::string,std::string> &config);
    virtual void process (boost::shared_ptr<Tuple> t);
    virtual std::string long_description();

   protected:
      std::string types;
      // specified by a string of 0s and 1s separated by spaces
      std::vector<bool> keep_fields;
      int n_fields;
      bool discard_off_size;
      //int fieldID; // the field containing the CSV values, default 0

   GENERIC_CLNAME
};

class CSVParseStrTk: public CSVParse {
  public:
    virtual void process (boost::shared_ptr<Tuple> t);
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


/***
 * Parses strings in tuples. 
 * Takes three params: field_to_parse, pattern, types
 * If 'field_to_parse' = x, then given a tuple (t0,t1...t_x,t_x+1,...), will 
 * produce (t0,t1...t_y,t_y2,...,t_x+1, ...). In other words, the params before
 * and after 'field_to_parse' are kept, and the field to parse is expanded.
 *
 * pattern should be a regex with groups in it. The types param should be a string
 * with one char per regex group and corresponds to the type of the group elems.
 * [S = string, I = Int, D = double]
 *
 * Behavior is un-specified if the regex doesn't match.
 * NOTE THAT FIELDS ARE NUMBERED FROM ZERO
 */
class GenericParse: public DataPlaneOperator {

 public:
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  virtual void process(boost::shared_ptr<Tuple> t);

 protected:
  boost::regex re; // regexp pattern to match tuples against
  std::string field_types;
  int fld_to_parse;
  bool keep_unparsed; // option: copy through or drop unparsed tuple elements
  
 GENERIC_CLNAME
};

/** Use a typecode [char] to parse a string into an element. Shared by
 * GenericParse, Extend, and CSVParse */
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

  void mutate_tuple(Tuple& t);
  virtual void process (boost::shared_ptr<Tuple> t) {
    mutate_tuple(*t);
    emit(t);
  }
  virtual void process_delta (Tuple& oldV, boost::shared_ptr<Tuple> newV, const operator_id_t pred);
  
  virtual operator_err_t configure (std::map<std::string,std::string> &config);

  
  virtual ~ExtendOperator() {};

GENERIC_CLNAME
};

class TimestampOperator: public DataPlaneOperator {
 public:
  enum TimeType {S, MS, US};
  virtual void process (boost::shared_ptr<Tuple> t);
  virtual operator_err_t configure (std::map<std::string,std::string> &config);

  
  virtual ~TimestampOperator() {};

GENERIC_CLNAME

 private:
  TimeType type;
};


// given concurrent callers, sends out an ordered stream
class OrderingOperator: public DataPlaneOperator {
 private:
  boost::mutex lock;
  
 public:

  virtual void process (boost::shared_ptr<Tuple> t) {
    boost::lock_guard<boost::mutex> critical_section (lock);
    emit(t);
  }

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



/***
 * Given a data stream, allows some fraction of data through.
 * Config options: seed [an int] and fraction [ a float], representing the fraction
 * to drop.  (So fraction == 0 means 'allow all')
 */
class HashSampleOperator: public DataPlaneOperator {
 public:
//  boost::random::mt19937 gen;
  uint32_t threshold; //drop tuples if rand >= threshhold. So thresh = 0 means pass all
  virtual void process (boost::shared_ptr<Tuple> t);
  virtual operator_err_t configure (std::map<std::string,std::string> &config);

  
  HashSampleOperator(): hash_field(0), hash_type(' ') {}
  
  virtual ~HashSampleOperator() {}
  
 private:
  int hash_field;
  char hash_type;

GENERIC_CLNAME
};

//rounds time fields
class TRoundingOperator: public DataPlaneOperator {
 public:
  enum InFormat {T, I, D};
  InFormat in_type;
  unsigned int fld_offset;
  int round_to;
  int add_offset;
// could in theory have a fixed offset, so you'd get  result = (original / round_to) * round_to + offset
  virtual void process (boost::shared_ptr<Tuple> t);
  virtual operator_err_t configure (std::map<std::string,std::string> &config);


GENERIC_CLNAME
};

/* Doesn't work currently. popen creates half-duplex pipe we need full duplex
 *
 * look at: http://stackoverflow.com/questions/6171552/popen-simultaneous-read-and-write
 * to fix
 * */
class UnixOperator: public ThreadedSource {
 public:
 
  virtual void stop();
  virtual void process (boost::shared_ptr<Tuple> t);
  virtual operator_err_t configure (std::map<std::string,std::string> &config);
  virtual bool emit_1();
  UnixOperator(): line_count(0) {}
  

private:
  FILE * pipe;
  std::string cmd;
  int line_count;

GENERIC_CLNAME
};


class URLToDomain: public DataPlaneOperator {

public:
  virtual void process (boost::shared_ptr<Tuple> t);
  virtual operator_err_t configure (std::map<std::string,std::string> &config);
private:
  unsigned field_id;

GENERIC_CLNAME
};

class GreaterThan: public DataPlaneOperator {
  //passes tuples that are greater than the filter
public:
  virtual void process (boost::shared_ptr<Tuple> t);
  virtual operator_err_t configure (std::map<std::string,std::string> &config);
private:
  unsigned field_id;
  int bound;

GENERIC_CLNAME
};

class IEqualityFilter: public DataPlaneOperator {

public:
  virtual void process (boost::shared_ptr<Tuple> t);
  virtual operator_err_t configure (std::map<std::string,std::string> &config);
private:
  unsigned field_id;
  int targ;

GENERIC_CLNAME
};

class RatioFilter: public DataPlaneOperator {

public:
  virtual void process (boost::shared_ptr<Tuple> t);
  virtual operator_err_t configure (std::map<std::string,std::string> &config);
private:
  unsigned numer_field_id;
  unsigned denom_field_id;
  double bound;

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
