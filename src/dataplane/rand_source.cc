
#include "rand_source.h"

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_real_distribution.hpp>
#include <glog/logging.h>
#include <time.h>
#include <fstream>

using namespace ::std;
using namespace boost;

namespace jetstream {

double s_rand_data[] = {37.7, 25.7, 19.5, 19.1, 12.9, 0.0};
string s_rand_labels[] = {"California", "Texas", "New York", "Florida","Illinois", "Should never appear; fencepost"};
int s_rand_data_len = sizeof(s_rand_data) / sizeof(double);


size_t fillin_s(std::vector<double>& rand_data, std::vector<std::string>& rand_labels) {
  rand_data.resize(s_rand_data_len);
  rand_labels.resize(s_rand_data_len);
  for (int i = 0; i < s_rand_data_len; ++ i) {
    rand_data[i] = s_rand_data[i];
    rand_labels[i] = s_rand_labels[i];
  }
  return s_rand_data_len;
}
size_t fillin_zipf(std::vector<double>& rand_data, std::vector<std::string>& rand_labels, int target_size) {
  rand_data.resize(target_size);
  rand_labels.resize(target_size);


  return target_size;
}

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
  
  std::string mode = config["mode"];
  if (mode == "zipf")
    rand_data_len = fillin_zipf(rand_data, rand_labels, 100);
  else
    rand_data_len = fillin_s(rand_data, rand_labels);


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
  js_usleep( 1000 * wait_per_batch);

  return false; //keep running indefinitely
}


operator_err_t
RandEvalOperator::configure(std::map<std::string,std::string> &config) {
  string out_file_name = config["file_out"];
  if ( out_file_name.length() > 0) {
    bool clear_file = (config["append"].length() > 0) && (config["append"] != "false");
    LOG(INFO) << "clear_file is " << clear_file;
    results_out = new ofstream(out_file_name.c_str(), (clear_file ? ios_base::out : ios_base::ate | ios_base::app));
  }
  
  std::string mode = config["mode"];
//  if (mode == "static")
  if (mode == "zipf")
    rand_data_len = fillin_zipf(rand_data, rand_labels, 100);
  else
    rand_data_len = fillin_s(rand_data, rand_labels);

  
  for (int i=0; i < s_rand_data_len; ++i)
    total_in_distrib += rand_data[i];
  
  return NO_ERR;
}



void
RandEvalOperator::process(boost::shared_ptr<Tuple> t) {
  assert( t->e_size() > 2);
  assert (t->e(1).has_t_val());
  
  time_t tuple_ts = t->e(1).t_val();
  if (last_ts_seen == 0) {
    last_ts_seen = tuple_ts;
  }
  if (tuple_ts > last_ts_seen) {
    int window_size_s = tuple_ts - last_ts_seen;
      //end of window, need to assess. The current tuple is irrelevant to the window
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
        *results_out << endl;
        *results_out << "Expected " << rand_labels[i] << " to be " << expected_total
          << " and got " << real_total << endl;
      }
      max_rel_deviation = min(max_rel_deviation, deflection); //TODO can average here instead
    }
    char time_str_buf[80];
    time_t now = time(NULL);
    ctime_r(&now, time_str_buf);
    *results_out <<  time_str_buf << " Data rate: "<< total_in_window/window_size_s << ". Data evenness was " <<
       max_rel_deviation  << " and got " << old_data << " old tuples"  << endl;
    last_ts_seen = tuple_ts;
    total_last_window = total_in_window;
    total_in_window = 0;
    old_data = 0;
    counts_this_period.clear();
  } else if (tuple_ts < last_ts_seen) {
    old_data ++;
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


RandEvalOperator::~RandEvalOperator() {
  *results_out << "ending RandEvalOperator" << endl;
  if (results_out != &std::cout) {
    ((ofstream*)results_out)->close();
    delete results_out;
  }
}

const string RandSourceOperator::my_type_name("Random source");
const string RandEvalOperator::my_type_name("Random data quality measurement");



}