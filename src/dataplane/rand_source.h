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

extern double s_rand_data[];
extern std::string s_rand_labels[];
//extern int s_rand_data_len;

class RandSourceOperator: public ThreadedSource {
 private:
  const static int DEFAULT_BATCH_SIZE = 500;
  int BATCH_SIZE;
 
  double slice_min, slice_max; //the numeric values to choose between
  
  int rate_per_sec;
  int tuples_this_sec;
  int cur_idx;
  double position_in_slice; // used in sequential mode.

  int start_idx; //label such that cumulative sum from [labels[0]...labels[start_idx-1] < slice_min
  double accum;  //the sum of labels[0]...labels[start_idx]
  int wait_per_batch; //ms
  int next_version_number;

 public:
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  RandSourceOperator(): next_version_number(0) {}

  virtual bool emit_1();

 protected:

  std::vector<double> rand_data;
  std::vector<std::string> rand_labels;


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

  
  RandEvalOperator() : last_ts_seen(0), max_rel_deviation(0), total_in_window(0),
      total_last_window(0), old_data(0), results_out(&std::cout), total_in_distrib(0) {}

  virtual std::string long_description();
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  virtual ~RandEvalOperator();

  std::vector<double> rand_data;
  std::vector<std::string> rand_labels;
  
 private:
  std::map<std::string,int> counts_this_period;
  time_t last_ts_seen ;
  double max_rel_deviation;
  long total_in_window, total_last_window, old_data;
  std::ostream* results_out;

  double total_in_distrib;

GENERIC_CLNAME
};

size_t fillin_s(std::vector<double>&, std::vector<std::string>&);
size_t fillin_zipf(std::vector<double>&, std::vector<std::string>&, int len);


class RandHistOperator: public ThreadedSource {
 private:
//  const static int DEFAULT_BATCH_SIZE = 50;
//  int BATCH_SIZE;
 
  int hist_size;
  unsigned tuples_per_sec;
  unsigned wait_per_batch;
  int next_version_number;
  
  bool schedule;
  unsigned schedule_increment;
  unsigned schedule_wait; 
  msec_t last_schedule_update;
  

 public:
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  RandHistOperator(): hist_size(200), wait_per_batch(1000),next_version_number(0), last_schedule_update(0) {}

  virtual bool emit_1();

 protected:

  std::vector<double> rand_data;
  std::vector<std::string> rand_labels;


GENERIC_CLNAME
};  




}

#endif
