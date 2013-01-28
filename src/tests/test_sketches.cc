#include "cm_sketch.h"
#include "js_utils.h"

#include <gtest/gtest.h>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/normal_distribution.hpp>
#include <boost/timer/timer.hpp>
#include <boost/random/exponential_distribution.hpp>

#include <limits>
#include <math.h>


using namespace ::std;
using namespace jetstream;


TEST(CMSketch, AddAndQuery) {

  CMSketch c(4, 8, 2);
  int seq[] = {  2, 7, 4, 8, 12, 100};
  int seq_size = sizeof(seq) / sizeof(int);
  for (int i = 0; i <  seq_size; ++i) {
    c.add_item( reinterpret_cast<char *>(& (seq[i])), sizeof(int), 2 * i + 1);
  }
  for (int i = 0; i < seq_size; ++i) {
    int sval = c.estimate(reinterpret_cast<char *>(& (seq[i])), sizeof(int));
    cout << "got " << sval << " and expected " << 2 * i + 1 << endl;
    ASSERT_GE(sval, 2* i + 1);
  }

}


TEST(CMSketch, Quantile) {
  CMMultiSketch c(5, 10, 3);

//  int seq[100];
  for (int i = 0; i < 20; ++i) {
    c.add_item_h(i, 1);
  }
  
  int collisions = 0;
  for(int i = 0; i < 20; ++i) {
    int est = c.estimate_h(i);
//    cout << "estimate for " << i << " is " << est << endl;
    ASSERT_GE(est, 1);
    collisions += est - 1;
  }

  int r = c.hash_range(0, numeric_limits<uint32_t>::max());
  cout << "maximal range has estimated sum: " << r << ", expected 20" << endl;

  r = c.hash_range(1000, numeric_limits<uint32_t>::max());
  cout << "over-high range has estimated sum: " << r << ", expected 0"<< endl;
  ASSERT_LE(r, collisions);

  
  r = c.hash_range(0, 1);
  cout << "range [0-1] has estimated sum: " << r << ", expected 2"<< endl;
  ASSERT_GE(r, 2);
  
  r =  c.hash_range(4, 8);
  cout << "range [4-8] has estimated sum: " <<r << ", expected 5"<< endl;
  ASSERT_GE(r, 5);
  ASSERT_LE(r, 5 + collisions);
  
  r =  c.hash_range(0, 10);
  cout << "range [0-10] has estimated sum: " <<r<< ", expected 11" << endl;
  ASSERT_GE(r, 11);
  ASSERT_LE(r, 11 + collisions);
  
  r =  c.hash_range(3, 10);
  cout << "range [3-10] has estimated sum: " <<r << ", expected 8"<< endl;
  ASSERT_GE(r, 8);
  ASSERT_LE(r, 8 + collisions);

  
  r =  c.hash_range(0, 10000);
  cout << "range [0-10000] has estimated sum: " <<r<< ", expected 20" << endl;
  ASSERT_GE(r, 20);
  ASSERT_LE(r, 20+ collisions); //should be upper bounded by max
  
  for (int i = 30; i > 10; --i) {
    r =  c.hash_range(0, 1 << i);
    if (r < 20 ||  r > 20 + collisions) {
      cout << "for range 0 - 2 ^ "<< i << " ("<<  (1 << i)<< ")" << endl;
      ASSERT_GE(r, 20);
      ASSERT_LE(r, 20+ collisions); //should be upper bounded by max
    }
  }
  
  int quantile_pts[] = {10, 25, 50, 75, 90};
  int quantile_list_len = sizeof(quantile_pts) / sizeof(int);
  for (int i =0; i < quantile_list_len; ++i) {
    int q = c.quantile( quantile_pts[i] / 100.0) ;
    cout << quantile_pts[i]<<"th percentile is " << q << endl;
  }

}

TEST(CMSketch, MultiInit) {

  cout << "initializing 100 sketches, each 10kb"<< endl;
  for(int i = 0; i < 100; ++i) {
    CMSketch c(8, 10, 2 + i);
  }

}


class StatsSample {
  

  public:
    vector<int> sample_of_data;
  
    void snarf_data(int * data, int size_to_take) {
      sample_of_data.reserve(size_to_take);
      sample_of_data.assign(data, data + size_to_take);
      std::sort (sample_of_data.begin(), sample_of_data.end());
    }
  
    int quantile(double q) {
      assert (q < 1 && q >= 0);
      return sample_of_data[ sample_of_data.size() * q];
    }


};


double update_err(int q, double* mean_error, int64_t* true_quantile, uint32_t est) {
  double err = abs( est - true_quantile[q]);
  mean_error[q] +=  err ;
  return err;
}

//use --gtest_also_run_disabled_tests to run
TEST(DISABLED_CMSketch, SketchVsSample) {
  const unsigned int DATA_SIZE = 1024* 1024 * 8;
  const int TRIALS = 8;
  int* data = new int[DATA_SIZE];
  size_t data_bytes = DATA_SIZE * sizeof(int);

  boost::mt19937 gen;
//  boost::random::uniform_int_distribution<> randsrc(1, DATA_SIZE /2);
//  boost::random::normal_distribution<> randsrc(10000, 1000);
  boost::random::exponential_distribution<> randsrc(0.002);
  
  for (int i=0; i < DATA_SIZE; ++ i)
    data[i] = (uint32_t) randsrc(gen);

  cout << " checking which of sampling versus sketching is better: " << endl;
  
  double quantiles_to_check[] = {0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95};
  int QUANTILES_TO_CHECK = sizeof(quantiles_to_check) /sizeof(double);
  double mean_error_with_sketch[QUANTILES_TO_CHECK];
  double mean_error_with_sampling[QUANTILES_TO_CHECK];
  int64_t true_quantile[QUANTILES_TO_CHECK];

  memset(mean_error_with_sketch, 0, sizeof(mean_error_with_sketch));
  memset(mean_error_with_sampling, 0, sizeof(mean_error_with_sampling));
  StatsSample full_population;
  full_population.snarf_data(data, DATA_SIZE);
  for (int q = 0; q < QUANTILES_TO_CHECK; ++ q) {
     true_quantile[q]= full_population.quantile(quantiles_to_check[q]);
  }
  
  usec_t time_adding_items = 0;
  usec_t time_querying = 0;

  for (int i =0; i < TRIALS; ++i) {
    cout << "Trial " << i << endl;
    CMMultiSketch sketch(9, 6, 2 + i);
    StatsSample sample;
    if (i ==0)
      cout << "sketch size is " << (sketch.size()/1024)<< "kb and data is " << data_bytes/1024 << "kb\n";
    sample.snarf_data(data, min<size_t>( sketch.size() / sizeof(int), DATA_SIZE));

    {
//      boost::timer::auto_cpu_timer t;
      usec_t now = get_usec();
      for (int j =0; j < DATA_SIZE; ++j)
        sketch.add_item_h(data[j], 1);
      time_adding_items += (get_usec() - now);
    }

    usec_t query_start = get_usec();
    for (int q = 0; q < QUANTILES_TO_CHECK; ++ q) {
      double quantile_pt = quantiles_to_check[q];
      
//      cout << " checking quantile " << quantile_pt<< " ("<<q<<"/"<< 5<<")\n";
      double d=  update_err(q, mean_error_with_sketch, true_quantile, sketch.quantile(quantile_pt));
      cout << "  error was " << d << " or " << 100.0 * d /true_quantile[q] << "%\n";
      
      update_err(q, mean_error_with_sampling, true_quantile, sample.quantile(quantile_pt));
    }
    time_querying += (get_usec() - query_start);
  }
  
  for (int q =0; q < QUANTILES_TO_CHECK; ++q) {
    mean_error_with_sketch[q] /= TRIALS;
    mean_error_with_sampling[q] /= TRIALS;

    cout << "\nQuantile " << quantiles_to_check[q] << " ("  << true_quantile[q]<< ")\n";
    cout << "sketch mean error: " << mean_error_with_sketch[q] << " or " <<
        (100.0 * mean_error_with_sketch[q] /true_quantile[q])<< "%"  << endl;
    
    cout << "sample mean error: " << mean_error_with_sampling[q] << " or " <<
        (100.0 * mean_error_with_sampling[q] /true_quantile[q])<< "%"  << endl;
  }
  cout << "Adding data to sketches took " << time_adding_items/TRIALS / 1000 <<
    "ms per sketch; each query took " << time_querying/TRIALS/QUANTILES_TO_CHECK << "us .\n";
  delete[] data;
  
}
