#ifndef JetStream_topk_source_h
#define JetStream_topk_source_h

#include "dataplaneoperator.h"
#include "experiment_operators.h"

#include <string>
#include <iostream>
// #include <boost/thread/thread.hpp>
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <glog/logging.h>

namespace jetstream {

extern double rand_data[];
extern std::string rand_labels[];
extern int rand_data_len;

class RandSourceOperator: public ThreadedSource {
 private:
  const static int DEFAULT_BATCH_SIZE = 1000;
  int BATCH_SIZE;
 
  double slice_min, slice_max; //the numeric values to choose between
  
  int start_idx; //label such that cumulative sum from [labels[0]...labels[start_idx-1] < slice_min 

  double accum;  //the sum of labels[0]...labels[start_idx]
  int wait_per_batch; //ms

 public:
  virtual operator_err_t configure(std::map<std::string,std::string> &config);

 protected:
  virtual bool emit_1() ;


GENERIC_CLNAME
};  


/**
  Inputs should be "(string, time range, count)" with optional other fields after, not examined
*/
class RandEvalOperator: public DataPlaneOperator {
 public:
  virtual void process (boost::shared_ptr<Tuple> t);
  double cur_deviation() {return max_rel_deviation;} // a number between 0 and 1; 0 represents the biggest distortion, 1 means no distortion
  long data_in_last_window() {return total_last_window;}

  
  RandEvalOperator() : last_ts_seen(0), max_rel_deviation(0), total_in_window(0),total_last_window(0) {}

  virtual std::string long_description();
  
 private:
  std::map<std::string,int> counts_this_period;
  time_t last_ts_seen ;
  double max_rel_deviation;
  long total_in_window, total_last_window;


GENERIC_CLNAME
};

}

#endif
