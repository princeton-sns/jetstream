
#include "rand_source.h"

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_real_distribution.hpp>
#include <glog/logging.h>
#include <time.h>


using namespace ::std;
using namespace boost;

namespace jetstream {


double data[] = {37.7, 25.7, 19.5, 19.1, 12.9};
string labels[] = {"California", "Texas", "New York", "Florida","Illinois"};

operator_err_t
RandSourceOperator::configure(std::map<std::string,std::string> &config) {

  
  int n = 0;
  
  // stringstream overloads the '!' operator to check the fail or bad bit
  if (!(stringstream(config["n"]) >> n)) {
    return operator_err_t("Invalid number of partitions: '" + config["n"] + "' is not a number.");
  }
  
  int k = 0;
  if (!(stringstream(config["k"]) >> k)) {
    return operator_err_t("Invalid partition for this operator: '" + config["k"] + "' is not a number.");
  }
  if (k >= n) {
    return operator_err_t("parameter k must be less than n");
  }

  double total = 0;
  for(int i =0; i < sizeof(data); ++i) {
    total += data[i];
  }
  double slice_size = total /n;
  slice_min = slice_size * k;
  slice_max = slice_size * (k + 1);

  accum = 0;
  start_idx = 0;

  while ( accum + data[start_idx] < slice_min ) {
    accum += data[start_idx++];
  }

  int rate_per_sec = 1000;

  if ((config["rate"].length() > 0)  && !(stringstream(config["rate"]) >> rate_per_sec)) {
    return operator_err_t("'rate' param should be a number, but '" + config["rate"] + "' is not.");
  }

  wait_per_batch = BATCH_SIZE * 1000 / rate_per_sec;
  
  return NO_ERR;
}



bool RandSourceOperator::emit_1()  {
  boost::shared_ptr<Tuple> t(new Tuple);
  

  boost::mt19937 gen;
  boost::random::uniform_real_distribution<double> rand(slice_min, slice_max);
  
  int tuples = BATCH_SIZE;
  int tuples_sent = 0;
  time_t now = time(NULL);
  while (running && tuples_sent++ < tuples) {
    double d = rand(gen);
    double my_acc = accum;
    int i = start_idx;
    while ( my_acc + data[i] < d) {
      my_acc += data[i++];
    }
    shared_ptr<Tuple> t(new Tuple);
    t->add_e()->set_s_val(labels[i]);
    t->add_e()->set_t_val(now);
    emit(t);
    
    
  }
  boost::this_thread::sleep(boost::posix_time::milliseconds(wait_per_batch));

  return false; //keep running indefinitely
}


const string RandSourceOperator::my_type_name("Random source");



}