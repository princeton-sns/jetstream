#include "congest_policy.h"
#include "js_utils.h"

#include <glog/logging.h>

using namespace ::std;

namespace jetstream {

const unsigned int MIN_MS_BETWEEN_ACTIONS = 1000;

int
CongestionPolicy::get_step(operator_id_t op, const double* const levels, unsigned levelsLen, unsigned curLevel) {
//  boost::lock_guard<boost::mutex> lock (stateLock);
  
  if (!congest) {
//    LOG(WARNING) << "no congestion monitor ahead of " << op;
    return 0;
  }
  if (statuses.size() == 0) //monitor not hooked up
    return 0;
  

  unsigned staleness = congest->measurement_staleness_ms();
  double congest_level = congest->capacity_ratio();   //do this unconditionally; triggers congest-monitor update
  
  OperatorState * status = NULL;
  msec_t now = get_msec();
  size_t op_pos;
  for (op_pos = 0; op_pos < statuses.size(); ++ op_pos) {
    status = &statuses[op_pos];

    if ( (status->last_state_change + MIN_MS_BETWEEN_ACTIONS > now - staleness)
     && (congest_level > 0))
      return 0; //did something recently

    if (status->op == op)
      break;
      //some other operator has higher priority and some available steps
    if( (congest_level < 1 && status->availStepsDown > 0)) {
      return 0; //other operator has priority to degrade
    }
  }
  DLOG_IF(FATAL, status->op != op) << "no status record for " << op << " in policy";
  if (status->op != op)
    return 0;
  
      //if we are upgrading, search list in opposite order
  if (congest_level > 1) {
    for (size_t back_op_pos = statuses.size() -1 ; back_op_pos > op_pos ; --back_op_pos) {
      if(statuses[back_op_pos].availStepsUp > 0)
        return 0;
      if (statuses[back_op_pos].last_state_change + MIN_MS_BETWEEN_ACTIONS > now - staleness)
        return 0; //did something recently
    }
  }
  
  bool check_since_action  =  (status->last_check > status->last_state_change); // no action since state change
  status->last_check = now;
  if (! check_since_action)
    return 0;
  
//  LOG(INFO)
  VLOG(2)
      << "policy for " << op << ". Queue " <<congest->name() <<
        " capacity-ratio level was " << congest_level << endl;
  
  unsigned int targ_step = curLevel;

  if ( congest_level < 0.95) {
    while ( congest_level / levels[targ_step] * levels[curLevel] < 0.95 && targ_step > 0)
      targ_step --;
  } else {
    //jump up one step, if room
     if ( targ_step < (levelsLen -1) && 
       (congest_level / levels[targ_step+1] * levels[curLevel] > 1.1
            || (levels[curLevel] == 0) ) ) {
       targ_step ++;
     }
  }
  
  int delta =  targ_step - curLevel;
  
  if (delta != 0) {
    LOG(INFO) << "setting degradation level for " <<op << " to " << (targ_step)
      << "/"<<levelsLen <<  " (operator ratio = " << levels[targ_step]
      << "). Capacity-ratio: " << congest_level << " at " << congest->name() << " TS " << now/1000;
    status->last_state_change = now;
  }
  status->availStepsDown = targ_step;
  status->availStepsUp = levelsLen - targ_step -1;
  return delta;
}

/*
void
CongestionPolicy::set_effect_delay(operator_id_t op, unsigned msecs) {
//  boost::lock_guard<boost::mutex> lock (mutex);

  for (size_t op_pos = 0; op_pos < statuses.size(); ++ op_pos) {
    if (statuses[op_pos].op == op) {
      statuses[op_pos].last_state_change += msecs;
      return;
    }
  }
  LOG(WARNING) << "attempt to adjust effect delay for unknown operator  " << op;
}
*/


}
