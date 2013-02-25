#include "congest_policy.h"
#include "js_utils.h"

#include <glog/logging.h>

using namespace ::std;

namespace jetstream {

const unsigned int MIN_MS_BETWEEN_ACTIONS = 50;

int
CongestionPolicy::get_step(operator_id_t op, const double* const levels, unsigned levelsLen, unsigned curLevel) {
  
  if (!congest) {
//    LOG(WARNING) << "no congestion monitor ahead of " << op;
    return 0;
  }
  
  size_t op_pos = 0;
  msec_t last_action = 0;
  for (; op_pos < statuses.size(); ++ op_pos) {
    OperatorState & status = statuses[op_pos];
    if( status.op != op && status.availStepsDown > 0) {
      return 0;
    } else
      last_action = max(last_action, status.last_state_change);
  }
  
  msec_t now = get_msec();
  if ( now - last_action < MIN_MS_BETWEEN_ACTIONS)
    return 0;
  
  double congest_level = congest->capacity_ratio();
  
  VLOG(1) << congest->name() << " congest level was " << congest_level << endl;
  
  unsigned delta = 0;
  if ( congest_level < 0.95) {
    int targ_step = curLevel;
    while ( congest_level / levels[targ_step] * levels[curLevel] < 0.95 && targ_step > 0)
      targ_step --;
    delta =  targ_step - curLevel;
  } else if (congest_level > 1.1  &&  (levelsLen - curLevel ) > 1)
    delta = 1;
  else
    delta = 0;
  
  if (delta != 0)
    LOG(INFO) << "setting degradation level for " <<op << " to " << delta<< ", congestion was " << congest_level;
  return delta;
}



}