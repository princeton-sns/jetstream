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
  if (statuses.size() == 0) //monitor not hooked up
    return 0;
  
  msec_t last_action = 0;
  double congest_level = congest->capacity_ratio();
  
  OperatorState * status = NULL;
  for (size_t op_pos = 0; op_pos < statuses.size(); ++ op_pos) {
    status = &statuses[op_pos];
    if (status->op == op)
      break;
    if( (congest_level < 1 && status->availStepsDown > 0)
      || (congest_level > 1 && status->availStepsUp > 0)) {
      return 0; //other operator has priority to degrade/upgrade
    }
    last_action = max(last_action, status->last_state_change);
  }
  DLOG_IF(FATAL, status->op != op) << "no status record for " << op << " in policy";
  msec_t now = get_msec();
  if (status->op != op || now - last_action < MIN_MS_BETWEEN_ACTIONS)
    return 0;
  
  bool check_since_action  =  (status->last_check > status->last_state_change); // no action since state change
  status->last_check = now;
  if (! check_since_action)
    return 0;
  
  // LOG(INFO)
  VLOG(2) << "policy for " << op << ". Queue " <<congest->name() <<
        " congest level was " << congest_level << endl;
  
  unsigned int targ_step = curLevel;

  if ( congest_level < 0.95) {
    while ( congest_level / levels[targ_step] * levels[curLevel] < 0.95 && targ_step > 0)
      targ_step --;
  } else {
    //jump up one step, if room
     if ( targ_step < (levelsLen -1) && 
       (congest_level / levels[targ_step+1] * levels[curLevel] > 1.1 || (levels[curLevel] == 0))
     )
       targ_step ++;
  }
  
  int delta =  targ_step - curLevel;
  
  if (delta != 0) {
    LOG(INFO) << "setting degradation level for " <<op << " to " << (curLevel+delta)<< ", congestion: " << congest_level << " TS " << now/1000;
    status->last_state_change = now;
  }
  return delta;
}



}
