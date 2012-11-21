#ifndef __JetStream__variable_sampling__
#define __JetStream__variable_sampling__

#include "base_operators.h"


namespace jetstream {

class VariableSamplingOperator: public SampleOperator {

  //needs to respond to congestion signals

};


class CongestionController: public DataPlaneOperator {

public:
  virtual void process(boost::shared_ptr<Tuple> t) {
    emit(t);
  }


};

}

#endif /* defined(__JetStream__variable_sampling__) */
