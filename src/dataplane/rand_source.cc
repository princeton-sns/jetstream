
#include "rand_source.h"

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_real_distribution.hpp>
#include <glog/logging.h>
#include <time.h>

using namespace ::std;
using namespace boost;

namespace jetstream {

double rand_data[] = {37.7, 25.7, 19.5, 19.1, 12.9, 0.0};
string rand_labels[] = {"California", "Texas", "New York", "Florida","Illinois", "Should never appear; fencepost"};
int rand_data_len = sizeof(rand_data) / sizeof(double);

double init_countup() {
  double d = 0;
  for (int i=0; i < rand_data_len; ++i)
    d += rand_data[i];
  return d;
}

double total_in_distrib = init_countup();




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
  for(int i =0; i < rand_data_len; ++i) {
    total += rand_data[i];
  }
  double slice_size = total /n;
  slice_min = slice_size * k;
  slice_max = slice_size * (k + 1);

  start_idx = 0;
  double low_vals = 0;
  while ( low_vals + rand_data[start_idx] < slice_min ) {
    low_vals += rand_data[start_idx++];
  }
    //at this point accum is how much weight is associated with values <= start_idx
  accum = slice_min - low_vals;
  

  int rate_per_sec = 1000;

  if ((config["rate"].length() > 0)  && !(stringstream(config["rate"]) >> rate_per_sec)) {
    return operator_err_t("'rate' param should be a number, but '" + config["rate"] + "' is not.");
  }
  LOG(INFO) << id() << " will choose numbers between " << slice_min << " and " << slice_max <<". Total = " << total;
 
  BATCH_SIZE = DEFAULT_BATCH_SIZE;
  if (BATCH_SIZE > rate_per_sec )
    BATCH_SIZE = rate_per_sec;
  wait_per_batch = BATCH_SIZE * 1000 / rate_per_sec;
  return NO_ERR;
}

#define USE_SEQ 
//  two choices: a sequential algorithm or an RNG. Take your pick.

bool
RandSourceOperator::emit_1()  {
  boost::shared_ptr<Tuple> t(new Tuple);
  
  int tuples = BATCH_SIZE;
  int tuples_sent = 0;

#ifdef USE_SEQ
  double incr = (slice_max - slice_min) / tuples;
  int i = start_idx;
  double my_acc = accum;
#else
  boost::mt19937 gen;
  boost::random::uniform_real_distribution<double> rand(slice_min, slice_max);
#endif

  time_t now = time(NULL);

  while (running && tuples_sent++ < tuples) {
  
#ifdef USE_SEQ
    if ( my_acc + incr > rand_data[i] ) { // note endpoint does NOT trigger increment here. That is deliberate.
      my_acc = 0;
      i ++;
    } else
      my_acc += incr;
    
#else
    double d = rand(gen);
    double my_acc = accum;
    int i = start_idx;
    while ( my_acc + rand_data[i] < d) {
      my_acc += rand_data[i++];
    }
#endif


    shared_ptr<Tuple> t(new Tuple);
    t->add_e()->set_s_val(rand_labels[i]);
    t->add_e()->set_t_val(now);
    emit(t);
  }
  boost::this_thread::sleep(boost::posix_time::milliseconds(wait_per_batch));

  return false; //keep running indefinitely
}


void
RandEvalOperator::process(boost::shared_ptr<Tuple> t) {
  assert( t->e_size() > 2);
  assert (t->e(1).has_t_val());
  
  time_t tuple_ts = t->e(1).t_val();
  if (last_ts_seen == 0) {
    last_ts_seen = tuple_ts;
/*    for (int i = 0; i < rand_data_len; ++i) {
      cout << "clearing " << labels[i] << "("<<i << "/" << rand_data_len<< ")"<< endl;
      counts_this_period[ labels[i]] = 0;
    }*/
  }
  if (tuple_ts != last_ts_seen) {
      //end of window, need to assess. The current tuple is irrelevant to the window
    cout << "end of window" << endl;
    max_rel_deviation = 1;
    for (int i = 0; i < rand_data_len; ++i) {
      double expected_total = total_in_window * rand_data[i] / total_in_distrib; //todo can normalize in advance
      double real_total =  (double) counts_this_period[ rand_labels[i]];
      double deflection;
      if ( expected_total > real_total)
        deflection = real_total / expected_total;
      else
        deflection = expected_total / real_total;
      
      if (deflection < 0.9) {
        cout << "Expected " << rand_labels[i] << " to be " << expected_total
          << " and got " << real_total << endl;
      }
      
      max_rel_deviation = min(max_rel_deviation, deflection);
    }
    cout << "Total data rate: "<< total_in_window << ". Data evenness was " <<  max_rel_deviation  << endl;
    last_ts_seen = tuple_ts;
    total_last_window = total_in_window;
    total_in_window = 0;
//    for (int i = 0; i < rand_data_len; ++i)
//      counts_this_period[ labels[i]] = 0;
    counts_this_period.clear();
  }
  
    //need to process the data, whether or not window closed
  int count = t->e(2).i_val();
  counts_this_period[t->e(0).s_val()] += count;
  total_in_window += count;
  last_ts_seen = tuple_ts;
}

std::string
RandEvalOperator::long_description() {
//  boost::lock_guard<boost::mutex> lock (mutex);

  ostringstream out;
  out << total_in_window << " tuples. Data evenness was " <<  max_rel_deviation;
  return out.str();

}

const string RandSourceOperator::my_type_name("Random source");
const string RandEvalOperator::my_type_name("Random data quality measurement");



}