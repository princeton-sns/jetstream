#ifndef __JetStream__operator_chain__
#define __JetStream__operator_chain__

#include <vector>
#include <boost/thread.hpp>
#include <string>

namespace jetstream {

//class CSrcOperator;
class COperator;


class OperatorChain {

protected:
//  CSrcOperator * chain_head;
  std::vector<COperator*> ops;
  volatile bool running;
  boost::shared_ptr<boost::thread> loopThread;
  std::string cached_chain_name;

public:
  
  void start();
  void stop();
  void operator()();
  const std::string& chain_name();

};

}

#endif /* defined(__JetStream__operator_chain__) */
