#ifndef __JetStream__operator_chain__
#define __JetStream__operator_chain__

#include <vector>
#include <boost/thread.hpp>
#include <string>
#include "jetstream_types.pb.h"
#include "js_utils.h"
#include <boost/asio.hpp>

namespace jetstream {

//class CSrcOperator;
class OperatorChain;
class Node;
typedef std::string operator_err_t;
const operator_err_t C_NO_ERR = "";
typedef boost::function<void ()> close_cb_t;


#define GENERIC_CLNAME  private: \
   const static std::string my_type_name; \
 public: \
   virtual const std::string& typename_as_str() {return my_type_name;}


class ChainMember {

  public:
   virtual void process(OperatorChain * chain, std::vector<boost::shared_ptr<Tuple> > &, DataplaneMessage&) = 0;
   virtual ~ChainMember() {}
   virtual bool is_source() = 0;
   virtual std::string id_as_str() = 0;
   virtual void add_chain(boost::shared_ptr<OperatorChain>) {}
};



class COperator: public ChainMember {

 public:
  virtual void process(OperatorChain * chain, std::vector<boost::shared_ptr<Tuple> > &, DataplaneMessage&) = 0;
  virtual ~COperator() {}
  virtual operator_err_t configure(std::map<std::string,std::string> &config) = 0;
  virtual void start() {}
  virtual void stop() {} //called only on strand
  virtual bool is_source() {return false;}

  operator_id_t & id() {return operID;}
  virtual std::string id_as_str() { return operID.to_string(); }
  virtual const std::string& typename_as_str() = 0; //return a name for the type  
  virtual std::string long_description() {return "";}


  void set_node (Node * n) { node = n; }

 protected:
    operator_id_t operID; // note that id() returns a reference, letting us set this
    Node * node;   
};



class CEachOperator: public COperator {

public:
  virtual void process(OperatorChain * chain,
                       std::vector<boost::shared_ptr<Tuple> > &,
                       DataplaneMessage&);
  
  virtual void process_one(boost::shared_ptr<Tuple> & t)  = 0;

};

class OperatorChain {

protected:
//  CSrcOperator * chain_head;
  std::vector< boost::shared_ptr<ChainMember> > ops;
  volatile bool running;
  std::string cached_chain_name;


public:

  OperatorChain() : running(false), strand(NULL) {}

  boost::asio::strand * strand;
  
  void start();
  void process(std::vector<boost::shared_ptr<Tuple> > &, DataplaneMessage&);
  void meta_from_upstream (DataplaneMessage& m) {
    std::vector<boost::shared_ptr<Tuple> > dummy;
//    DataplaneMessage m2;
//    m2.CopyFrom(m);
    process(dummy, m);
  }

  void upwards_metadata(DataplaneMessage&);

  void stop();
  void stop_async(close_cb_t cb);
  void do_stop(close_cb_t);

  const std::string& chain_name();  
  
    //These methods are for structural changes
  
  void add_member(boost::shared_ptr<ChainMember> op = boost::shared_ptr<ChainMember>() ) {
    //default value is to push back a null pointer, just reserving a space
    ops.push_back(op);
  }

  
  void clone_from(boost::shared_ptr<OperatorChain>);
  
  boost::shared_ptr<ChainMember> member(unsigned i) const {
    return ops[i];
  }

};

}

#endif /* defined(__JetStream__operator_chain__) */
