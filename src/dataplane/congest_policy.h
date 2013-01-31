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
#include <map>

namespace jetstream {

class CongestionPolicy {
  protected:
//    std::map<operator_id_t, 

  public:
    double get_step(operator_id_t op, double min_ratio, double next_step_down, double next_step_up, double max_ratio);

};

}

#endif
