#ifndef JetStream_operators_h
#define JetStream_operators_h

#include "dataplaneoperator.h"
#include <string>
#include <boost/regex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/shared_ptr.hpp>


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
  virtual void start(std::map<std::string,std::string> config);
  virtual void stop();
  void operator()();  // A thread that will loop while reading the file
  bool isRunning();
  virtual void process(boost::shared_ptr<Tuple> t);  
  
//  virtual const char * get_type() {return "File read";}

 protected:
  std::string f_name; //name of file to read
  boost::shared_ptr<boost::thread> loopThread;
  volatile bool running;


 private:
   const static std::string my_type_name;  
 public:
   virtual const std::string& get_type() {return my_type_name;}
};


/***
 * Operator for emitting a specified number of generic tuples.
 */
class SendK: public DataPlaneOperator {
 public:
  virtual void start(std::map<std::string,std::string> config);
  virtual void stop();
  virtual void process(boost::shared_ptr<Tuple> t);
  void operator()();  // A thread that will loop while reading the file    

    
 protected:
  u_int k; //name of file to read
  boost::shared_ptr<boost::thread> loopThread;
  volatile bool running;
  

 private:
   const static std::string my_type_name;  
 public:
   virtual const std::string& get_type() {return my_type_name;}
};  
  

/***
 * Operator for filtering strings. Expects one parameter, a string named 'pattern'
 * containing a regular expression. Assumes each received tuple has a first element
 * that is a string, and re-emits the tuple if the string matches 'pattern'.
 */
class StringGrep: public DataPlaneOperator {
 public:
  StringGrep() : id (0) {}
  virtual void start(std::map<std::string,std::string> config);
  virtual void process(boost::shared_ptr<Tuple> t);

 protected:
  boost::regex re; // regexp pattern to match tuples against
  int id; // the field on which to filter


 private:
   const static std::string my_type_name;  
 public:
   virtual const std::string& get_type() {return my_type_name;}
};

  
class DummyReceiver: public DataPlaneOperator {
 public:
  std::vector< boost::shared_ptr<Tuple> > tuples;
  virtual void process(boost::shared_ptr<Tuple> t) {
    tuples.push_back(t);
  }
  
  virtual ~DummyReceiver();

 private:
   const static std::string my_type_name;  
 public:
   virtual const std::string& get_type() {return my_type_name;}
};

}

#endif
