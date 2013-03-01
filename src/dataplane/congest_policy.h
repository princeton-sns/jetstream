//
//  congest_policy.h
//  JetStream
//
//  Created by Ariel Rabkin on 1/29/13.
//  Copyright (c) 2013 Ariel Rabkin. All rights reserved.
//

#ifndef JetStream_congest_policy_h
#define JetStream_congest_policy_h

#include "js_utils.h"
#include "congestion_monitor.h"
#include <map>
#include <boost/thread.hpp>


namespace jetstream {

class CongestionPolicy {

  struct OperatorState {
    operator_id_t op;
    int availStepsDown;
    int availStepsUp;
    msec_t last_state_change;
    msec_t last_check;
    
    OperatorState(operator_id_t i) : op(i),availStepsDown(0), availStepsUp(0),
      last_state_change (0), last_check(1) {}
  };

  protected:
    mutable boost::mutex stateLock; //no need for locking if access all within a chain?
    std::vector<OperatorState> statuses;
    boost::shared_ptr<CongestionMonitor> congest;
  
  public:
    //-1 means "lower send rate", +1 means "raise send rate, and "0" means no shift
    int get_step(operator_id_t op, const double* const levels, unsigned levelsLen, unsigned curLevel);
  
    void add_operator(operator_id_t id) {
      statuses.push_back( OperatorState(id));
    }
  
    void set_congest_monitor(boost::shared_ptr<CongestionMonitor> c) {
      congest = c;
    }

};

}

#endif
