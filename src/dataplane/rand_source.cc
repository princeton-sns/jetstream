
#include "chain_ops.h"
#include "rand_source.h"

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_real_distribution.hpp>
#include <time.h>
#include <fstream>
#include <glog/logging.h>

#include "quantile_est.h"

using namespace ::std;
using namespace boost;

namespace jetstream {

const double s_rand_data[] = {37.7, 25.7, 19.5, 19.1, 12.9, 0.0};
const string s_rand_labels[] = {"California", "Texas", "New York", "Florida","Illinois", "Should never appear; fencepost"};
const int s_rand_data_len = sizeof(s_rand_data) / sizeof(double);



size_t fillin_s(std::vector<double>& rand_data, std::vector<std::string>& rand_labels) {
  rand_data.resize(s_rand_data_len);
  rand_labels.resize(s_rand_data_len);
  for (int i = 0; i < s_rand_data_len; ++ i) {
    rand_data[i] = s_rand_data[i];
    rand_labels[i] = s_rand_labels[i];
  }
  return s_rand_data_len;
}

size_t fillin_zipf (std::vector<double>& rand_data,
                    std::vector<std::string>& rand_labels,
                    int target_size,
                    double zipf_param) {
  rand_data.resize(target_size);
  rand_labels.resize(target_size);
  double BASE_VAL = 100;
  for (int i = 0; i < target_size; ++ i) {
    ostringstream str;
    str << "item_" << i;
    rand_labels[i] = str.str();
    rand_data[i] = BASE_VAL / pow(i+2, zipf_param);
  }
  return target_size;
}

operator_err_t fillin_data (std::vector<double>& rand_data,
                    std::vector<std::string>& rand_labels,
                    operator_config_t config) {
  
  if (config["mode"] == "zipf") {
    double zipf_param = 1.2;
    if (config["zipf_param"].length() > 0)
      zipf_param = boost::lexical_cast<double>(config["zipf_param"]);
    
    int items = 100;
    if (config["items"].length() > 0)
      items = boost::lexical_cast<int>(config["items"]);
    fillin_zipf(rand_data, rand_labels, items, zipf_param);

  } else
    fillin_s(rand_data, rand_labels);
  return NO_ERR;
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
  
  operator_err_t err = fillin_data(rand_data, rand_labels, config);
  if (err != NO_ERR)
    return err;

  double total = 0;
  for(unsigned int i =0; i < rand_data.size(); ++i) {
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
  
  rate_per_sec = 1000;
  cur_idx = start_idx;

  if ((config["rate"].length() > 0)  && !(stringstream(config["rate"]) >> rate_per_sec)) {
    return operator_err_t("'rate' param should be a number, but '" + config["rate"] + "' is not.");
  }
  LOG(INFO) << id() << " will choose numbers between " << slice_min << " and " << slice_max <<". Total = " << total;
 
  BATCH_SIZE = DEFAULT_BATCH_SIZE;
  if (BATCH_SIZE > rate_per_sec )
    BATCH_SIZE = rate_per_sec;  // if rate_per_sec is low, split into smaller batches
  wait_per_batch = BATCH_SIZE * 1000 / rate_per_sec;
  return NO_ERR;
}

/* two choices: a sequential algorithm or an RNG. Take your pick.
  Rand is more realistic, seq is more predictable for testing.
  */
#define USE_SEQ 

int
RandSourceOperator::emit_data()  {
  
  int tuples_sent = 0;

#ifdef USE_SEQ
  double incr = (slice_max - slice_min) / rate_per_sec;
#else
  boost::mt19937 gen;
  boost::random::uniform_real_distribution<double> rand(slice_min, slice_max);
#endif

  time_t now = time(NULL);
  vector<shared_ptr<Tuple> > tuples;
  tuples.reserve(BATCH_SIZE);

  while (tuples_sent++ < BATCH_SIZE) {
    shared_ptr<Tuple> t(new Tuple);

#ifdef USE_SEQ
    if ( position_in_slice > rand_data[cur_idx] ) { // note endpoint does NOT trigger increment here. That is deliberate.
      position_in_slice -= rand_data[cur_idx];
      cur_idx ++;
    }
    if ( cur_idx >= rand_data.size()) {
      cur_idx = start_idx; //wrap around.
      position_in_slice += accum; //subtract the share of rand_data[startIdx] that isn't this Operator's.
    }
    t->add_e()->set_s_val(rand_labels[cur_idx]);
    position_in_slice += incr;
    
#else
    double d = rand(gen);
    double my_acc = accum;
    int i = start_idx;
    while ( my_acc + rand_data[i] < d) {
      my_acc += rand_data[i++];
    }
    t->add_e()->set_s_val(rand_labels[i]);
#endif

    t->add_e()->set_t_val(now);
    t->set_version(next_version_number++);
    tuples.push_back(t);
  }
  DataplaneMessage end_msg;
  end_msg.set_type(DataplaneMessage::END_OF_WINDOW);
  end_msg.set_window_length_ms(wait_per_batch);
  chain->process(tuples, end_msg);
  return wait_per_batch;
}


operator_err_t
RandEvalOperator::configure(std::map<std::string,std::string> &config) {
  string out_file_name = config["file_out"];
  if ( out_file_name.length() > 0) {
    bool clear_file = (config["append"].length() > 0) && (config["append"] != "false");
    LOG(INFO) << "clear_file is " << clear_file;
    results_out = new ofstream(out_file_name.c_str(), (clear_file ? ios_base::out : ios_base::ate | ios_base::app));
  }
  
  operator_err_t err = fillin_data(rand_data, rand_labels, config);
  if (err != NO_ERR)
    return err;
  
  for (unsigned int i=0; i < rand_data.size(); ++i)
    total_in_distrib += rand_data[i];
  
  return NO_ERR;
}



void
RandEvalOperator::process_one(boost::shared_ptr<Tuple>& t) {
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
    for (unsigned int i = 0; i < rand_data.size(); ++i) {
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
RandEvalOperator::long_description() const {
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


operator_err_t
RandHistOperator::configure(std::map<std::string,std::string> &config) {

  ADAPT = false;

  tuples_per_sec = 50;

  unique_vals = 100;
  if ((config["unique_vals"].length() > 0)  && !(stringstream(config["unique_vals"]) >> unique_vals)) {
    return operator_err_t("'unique_vals' param should be a number, but '" + config["unique_vals"] + "' is not.");
  }
  
  if ((config["hist_size"].length() > 0)  && !(stringstream(config["hist_size"]) >> hist_size)) {
    return operator_err_t("'hist_size' param should be a number, but '" + config["hist_size"] + "' is not.");
  }

  if ((config["wait_per_batch"].length() > 0)  && !(stringstream(config["wait_per_batch"]) >> wait_per_batch)) {
    return operator_err_t("'wait_per_batch' param should be a number, but '" + config["wait_per_batch"] + "' is not.");
  }

  if ((config["batches_per_window"].length() > 0)  && !(stringstream(config["batches_per_window"]) >> batches_per_window)) {
    return operator_err_t("'batches_per_window' param should be a number, but '" + config["batches_per_window"] + "' is not.");
  }

  if ((config["schedule_start"].length() > 0)  && !(stringstream(config["schedule_start"]) >> tuples_per_sec)) {
    return operator_err_t("'schedule_start' param should be a number, but '" + config["schedule_start"] + "' is not.");
  }  

  schedule_increment = 10;
  if ((config["schedule_increment"].length() > 0)  && !(stringstream(config["schedule_increment"]) >> schedule_increment)) {
    return operator_err_t("'schedule_increment' param should be a number, but '" + config["schedule_increment"] + "' is not.");
  }

  schedule = false;
  if (schedule_increment != 0) {
    schedule = true;

    schedule_wait = 5000;
    if ((config["schedule_wait"].length() > 0)  && !(stringstream(config["schedule_wait"]) >> schedule_wait)) {
      return operator_err_t("'schedule_wait' param should be a number, but '" + config["schedule_wait"] + "' is not.");
    }
    schedule_max = 10000;
    if ((config["schedule_max"].length() > 0)  && !(stringstream(config["schedule_max"]) >> schedule_max)) {
      return operator_err_t("'schedule_max' param should be a number, but '" + config["schedule_max"] + "' is not.");
    }

    if ((config["window_fudge_factor"].length() > 0)  && !(stringstream(config["window_fudge_factor"]) >> window_fudge_factor)) {
      return operator_err_t("'window_fudge_factor' param should be a number, but '" + config["window_fudge_factor"] + "' is not.");
    }

  }

  unsigned num_levels = 1;
  if ((config["levels"].length() > 0)  && !(stringstream(config["levels"]) >> num_levels)) {
    return operator_err_t("'levels' param should be a number, but '" + config["levels"] + "' is not.");
  }
  for (unsigned i = 0; i < num_levels; ++i)
    levels.push_back(  double(i+1) / (num_levels)  );
  cur_level = num_levels - 1;
  

  
  return NO_ERR;
 
/*  BATCH_SIZE = DEFAULT_BATCH_SIZE;
  if (BATCH_SIZE > rate_per_sec )
    BATCH_SIZE = rate_per_sec;
  wait_per_batch = BATCH_SIZE * 1000 / rate_per_sec;
*/
}

void
RandHistOperator::start() {
  TimerSource::start();
  //boost::shared_ptr<boost::thread>(new boost::thread(boost::bind(&RandHistOperator::generate, this)));
  new boost::thread(boost::bind(&RandHistOperator::generate, this));
  new boost::thread(boost::bind(&RandHistOperator::generate, this));
  new boost::thread(boost::bind(&RandHistOperator::generate, this));

}

void
RandHistOperator::generate() {

  unsigned int level = 9999999;
  LogHistogram * lh = NULL;

  msec_t start_t = get_msec();
  time_t now = start_t / 1000 ;

  unsigned counter = 0;
  unsigned tuples_per_batch = tuples_per_sec * (wait_per_batch/1000);
  while(running)
  {
    if(level != cur_level)
    {
      level = cur_level;
      if (lh != NULL)
        delete lh;
      lh = new LogHistogram(hist_size * (levels[level]));
      for (int i = 0; i < 22; ++i)
        lh->add_item(i*i, i + 10);
    }

    counter++;
    shared_ptr<Tuple> t(new Tuple);
    extend_tuple_time(*t, now);
    extend_tuple(*t, int32_t(counter % unique_vals));
    JSSummary * s = t->add_e()->mutable_summary();
    
    lh->serialize_to(*s);

    {
      boost::unique_lock<boost::mutex> lock(internals);
      t->set_version(next_version_number++);
      queue.push(t);
    }

    if(queue.size() > 2*tuples_per_batch)
    {
       tuples_per_batch = tuples_per_sec * (wait_per_batch/1000);
       js_usleep(100000);
    }
  }
  if(lh != NULL)
    delete lh;
}

int
RandHistOperator::emit_data() {

  msec_t start_t = get_msec();

  unsigned tuples_sent = 0;

  if ( levels.size() > 1)
    adapt();

  msec_t now_msec = get_msec();
  if(schedule && now_msec > (last_schedule_update + schedule_wait)  && tuples_per_sec < schedule_max){
    last_schedule_update = now_msec;
    tuples_per_sec += schedule_increment;
    LOG(INFO) << "Setting tuples per sec " << tuples_per_sec;
  }
  unsigned tuples_per_batch = tuples_per_sec * (wait_per_batch/1000);
  vector< shared_ptr<Tuple> > tuples;
  while (tuples_sent++ < tuples_per_batch) {
    while(queue.size()<1)
    {
      LOG(INFO) << "Waiting on tuples";
      js_usleep(100);
    }
    {
      boost::unique_lock<boost::mutex> lock(internals);
      shared_ptr<Tuple> t = queue.front();
      queue.pop();
      tuples.push_back(t);
    }
  }
  msec_t end_t = get_msec();
  msec_t running_time = (end_t+10) - start_t;
  if (running_time > wait_per_batch / 2) {
    LOG_FIRST_N(WARNING, 10) << "Took " << running_time << " to send; exceeds half of wait-per-batch";
  }
  if (running_time > wait_per_batch)
    LOG(FATAL) << "Generation took too long. Runtime "<< running_time << " should be under "<< wait_per_batch;

  DataplaneMessage end_msg;
  if (batch ++ % batches_per_window == 0) {
    end_msg.set_type(DataplaneMessage::END_OF_WINDOW);
    end_msg.set_window_length_ms(wait_per_batch * batches_per_window-window_fudge_factor);
  }
  chain->process(tuples, end_msg);

  return (wait_per_batch - running_time); //keep running indefinitely
}

void
RandHistOperator::adapt() {
  if (levels.size() > 1) {
    int delta = congest_policy->get_step(id(), levels.data(), levels.size(), cur_level);
    cur_level += delta;
    LOG(INFO)  << "Rand-hist source adjusting itself cur_level="<< cur_level<< " delta="<<delta;
  }
}


const string RandSourceOperator::my_type_name("Random source");
const string RandEvalOperator::my_type_name("Random data quality measurement");
const string RandHistOperator::my_type_name("Random hist source");


}
