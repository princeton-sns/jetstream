#include "congest_policy.h"
#include "js_utils.h"

#include <glog/logging.h>
#include <stdlib.h>

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
  
  msec_t now = get_msec();
  msec_t measurement_time = congest->measurement_time();
  double congest_level = congest->capacity_ratio();   //do this unconditionally; triggers congest-monitor update
  
  OperatorState * status = NULL;
  size_t op_pos;
  for (op_pos = 0; op_pos < statuses.size(); ++ op_pos) {
    status = &statuses[op_pos];

    if ( (status->last_state_change + MIN_MS_BETWEEN_ACTIONS > measurement_time)
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
      if (statuses[back_op_pos].last_state_change + MIN_MS_BETWEEN_ACTIONS > measurement_time)
        return 0; //did something recently
    }
  }
  
  bool check_since_action  =  (status->last_check > status->last_state_change); // no action since state change
  status->last_check = measurement_time;
  if (! check_since_action)
    return 0;
  
//  LOG(INFO)
  VLOG(2)
      << "policy for " << op << ". Queue " <<congest->name() <<
        " capacity-ratio level was " << congest_level << endl;
  
  int targ_step = curLevel;

  if ( congest_level < 0.95) {
    while ( congest_level / levels[targ_step] * levels[curLevel] < 0.95 && targ_step > 0)
      targ_step --;
  } else if (congest_level > 1.1){
    targ_step = should_upgrade(congest_level, levels, levelsLen, curLevel, *status);
    //jump up one step, if room
    
  }
  
  int delta =  targ_step - curLevel;
  
  if (delta != 0) {
    LOG(INFO) << "setting degradation level for " <<op << " to " << (targ_step+1)
      << "/"<<levelsLen <<  " (operator ratio = " << levels[targ_step]
      << "). Capacity-ratio: " << congest_level << " at " << congest->name() << " TS " << now/1000;
    status->last_state_change = now;
  }
  status->availStepsDown = targ_step;
  status->availStepsUp = levelsLen - targ_step -1;
  return delta;
}

static double PROB_TO_WAIT_FOR_UPGRADE = 0.05;

int
CongestionPolicy::should_upgrade(double capacity, 
                                 const double * const levels,
                                 unsigned levelsLen,
                                 unsigned curLevel, 
                                 OperatorState& status) {
  if (curLevel >= levelsLen -1)
      return curLevel; //at max
//  LOG(INFO) << "Considering taking step up. Level =" << curLevel<< " cap-ratio " << capacity;
  if (curLevel == 0 && capacity > 0)
    return 1;
  
  unsigned max_supported_step = curLevel;
  while ((levels[max_supported_step]/ levels[curLevel]  < capacity) &&
    (max_supported_step < levelsLen))
        max_supported_step ++;
  
  unsigned half_supported = (max_supported_step + curLevel + 1) /2;
  if (rand() < RAND_MAX * PROB_TO_WAIT_FOR_UPGRADE)
    return half_supported;
  else
    return curLevel;
//  double next_step_ratio = ;
//  if (next_step_ratio )
//    return curLevel +1;

//  return curLevel;
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
