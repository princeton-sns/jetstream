#ifndef JetStream_topk_source_h
#define JetStream_topk_source_h

#include "dataplaneoperator.h"
#include "experiment_operators.h"

#include <string>
#include <iostream>
// #include <boost/thread/thread.hpp>
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

namespace jetstream {

class RandSourceOperator: public ThreadedSource {
 private:
  const static int BATCH_SIZE = 500;
 
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


}

#endif
