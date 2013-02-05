
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
    c.add_item(i, 1);
  }
  
  int collisions = 0;
  for(int i = 0; i < 20; ++i) {
    int est = c.estimate_point(i);
//    cout << "estimate for " << i << " is " << est << endl;
    ASSERT_GE(est, 1);
    collisions += est - 1;
  }

  int r = c.hash_range(0, numeric_limits<int32_t>::max());
  cout << "maximal range has estimated sum: " << r << ", expected 20" << endl;

  r = c.hash_range(1000, numeric_limits<int32_t>::max());
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

TEST(CMSketch, Merge) {
  const int ITEMS = 20;
  CMSketch s1(6, 8, 3);
  CMMultiSketch s1_multi(6, 8, 3);
  for(int i = 0; i < ITEMS; ++i) {
    s1.add_item_h(i*i, i + 2);
    s1_multi.add_item(i * i, i + 2);
  }

  CMSketch s2(6, 6, 3); //fewer rows
  CMMultiSketch s2_multi(6, 6, 3);
  for(int i = 0; i < ITEMS; ++i) {
    s2.add_item_h(i*i, i + 10);
    s2_multi.add_item(i * i, i + 10);
  }
  
  s1.merge_in(s2);
  s1_multi.merge_in(s2_multi);
  for(int i = 0; i < ITEMS; ++i) {
    ASSERT_GT( (unsigned int) i + 12, s1.estimate_h(i*i));
    ASSERT_GT( (unsigned int) i + 12, s1_multi.estimate_point(i*i));
  }
  ASSERT_EQ(s1.depth(), 6U);
  
  CMSketch s3(6, 8, 4);
  ASSERT_FALSE( s3.can_accept(s1)); //due to different random seeds

//  CMMultiSketch s3_m(6, 8, 4);
//  ASSERT_FALSE( s3_m.can_accept(s1_multi));
}

TEST(CMSketch, SerDe) {
  CMMultiSketch summary(6, 8, 4);
  const int ITEMS = 20;
  for(int i = 0; i < ITEMS; ++i) {
    summary.add_item(i*i, 1);
  }
  
  cout << "top quantile is " << summary.quantile(1) << endl;
  
  JSSummary serialized;
  summary.serialize_to(serialized);
  string s = serialized.SerializeAsString();
  JSCMSketch * s2 = serialized.mutable_sketch();
  CMMultiSketch deserialized(*s2);
  
  for (int i = 0; i < ITEMS; ++i) {
    ASSERT_EQ(deserialized.estimate_point(i*i), summary.estimate_point(i * i));
  }
}


TEST(ReservoirSample, Merge) {

  for (int a_elems = 1; a_elems < 4; ++a_elems) {
    for (int b_elems = 1; b_elems < 4; ++b_elems) {
      ReservoirSample a(800);
      ReservoirSample b(800);
      a.add_item(1000, 400 * a_elems);
      b.add_item(500, 400 * b_elems);
      
      a.merge_in(b);
      int expected_mean = (1000 * a_elems + 500 * b_elems) / (a_elems + b_elems);
      int mean = (int) a.mean();
      if (  abs(expected_mean - mean) > 30) {
        cout << "test run "<< a_elems <<"," << b_elems << ": merged population mean is "
            << mean<< " expected " << expected_mean << endl;
      }
      EXPECT_EQ( a.quantile(0.1), 500); //at least a tenth from b
      EXPECT_EQ( a.quantile(0.9), 1000); //at least a tenth from a
      
      if (a_elems > b_elems) {
        EXPECT_EQ(a.quantile(0.5), 1000);
      } else if (b_elems > a_elems) {
        EXPECT_EQ(a.quantile(0.5), 500);
      }
    }
  }
}

TEST(ReservoirSample, SerDe) {

  ReservoirSample summary(30);
  const int ITEMS = 20;
  for(int i = 0; i < ITEMS; ++i) {
    summary.add_item(i*i, 1);
  }
  
  JSSummary serialized;
  summary.serialize_to(serialized);
  string s = serialized.SerializeAsString();
  JSSample* s2 = serialized.mutable_sample();
  ReservoirSample deserialized(*s2);

  ASSERT_EQ(summary.mean(), deserialized.mean());
  ASSERT_EQ(summary.elements(), deserialized.elements());
}

TEST(LogHistogram, SerDe) {

  LogHistogram summary(30);
  const int ITEMS = 20;
  for(int i = 0; i < ITEMS; ++i) {
    summary.add_item(i*i, 1);
  }
  
  JSSummary serialized;
  summary.serialize_to(serialized);
  string s = serialized.SerializeAsString();
  JSHistogram * s2 = serialized.mutable_histo();
  ASSERT_EQ( (size_t) s2->bucket_vals_size(), summary.bucket_count());
  LogHistogram deserialized(*s2);
  ASSERT_EQ(summary.bucket_count(), deserialized.bucket_count());

  ASSERT_EQ(summary, deserialized);
}

TEST(LogHistogram, Boundaries) {
  const int BUCKETS = 30;
  LogHistogram hist(BUCKETS);
  cout << "asked for " << BUCKETS << " and got " << hist.bucket_count() << endl;
  ASSERT_EQ(0,hist.bucket_min(0));
  ASSERT_EQ(1,hist.bucket_min(1));
  for(unsigned int i = 0; i < hist.bucket_count()-1; ++i) {
    ASSERT_EQ(hist.bucket_max(i),hist.bucket_min(i+1)-1);
  }

  hist.add_item(10, 2);
  ASSERT_EQ(2U, hist.count_in_b(hist.bucket_with(10)));
  
  for (int i = 0; i < 1000; ++i) {
    size_t b = hist.bucket_with(i);
    std::pair<int, int> bucket_bounds = hist.bucket_bounds(b);
    ASSERT_GE(i, bucket_bounds.first);
    ASSERT_LE(i, bucket_bounds.second);
    hist.add_item(i, 1);
  }
  
/*
  for(int b =0; b < hist.bucket_count(); ++b) {
    std::pair<int, int> bucket_bounds = hist.bucket_bounds(b);
    cout << "in "<< bucket_bounds.first << ", " << bucket_bounds.second <<"] "
      << hist.count_in_b(b) << endl;
  }*/
}


template <typename T>
int * make_rand_data(size_t size, T& randsrc) {
  boost::mt19937 gen;
  int* data = new int[size];
  for (unsigned int i=0; i < size; ++ i)
    data[i] = (int) randsrc(gen);
  return data;
}

TEST(LogHistogram, Quantile) {

  const int DATA_SIZE = 1000;
//  int * data = make_rand_data<>(DATA_SIZE, boost::random::uniform_int_distribution<>(1, 1000));
  int * data = new int[DATA_SIZE];
  for (int i = 0; i < DATA_SIZE; ++i)
    data[i] = i;

  SampleEstimation full_population;
  full_population.add_data(data, DATA_SIZE);
  
  double quantiles_to_check[] = {0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95};
  int QUANTILES_TO_CHECK = sizeof(quantiles_to_check) /sizeof(double);
  int true_quantile[QUANTILES_TO_CHECK];
  
  for (int q = 0; q < QUANTILES_TO_CHECK; ++ q) {
     true_quantile[q]= full_population.quantile(quantiles_to_check[q]);
  }

  for(int i = 29; i < 30; ++i) {
    LogHistogram hist(i);
    cout << "testing hist with size " << hist.bucket_count() << endl;
    hist.add_data(data, DATA_SIZE);
    
    for (int q = 0; q < QUANTILES_TO_CHECK; ++ q) {
      std::pair<int,int> range = hist.quantile_range(quantiles_to_check[q]);
      cout << "got q " << quantiles_to_check[q] << " in [" << range.first << ", "
        << range.second <<"] - should be " << true_quantile[q] << endl;
      
      ASSERT_GE(true_quantile[q], range.first);
      ASSERT_LE(true_quantile[q], range.second);
      int quant = hist.quantile(quantiles_to_check[q]);
      ASSERT_GE(quant, range.first);
      ASSERT_LE(quant, range.second);
    }
    
  }
  delete data;
}


double update_err(int q, double* mean_error, int64_t* true_quantile, int est) {
  double err = abs( est - true_quantile[q]);
  mean_error[q] +=  err ;
  return err;
}

//use --gtest_also_run_disabled_tests to run
TEST(DISABLED_CMSketch, SketchVsSample) {
  const unsigned int DATA_SIZE = 1024* 1024 * 8;
  const int TRIALS = 8;
  const int APPROACHES = 3;
  
  
  size_t data_bytes = DATA_SIZE * sizeof(int);

//  boost::random::uniform_int_distribution<> randsrc(1, DATA_SIZE /2);
  boost::random::normal_distribution<> randsrc(10000, 1000);
//  boost::random::exponential_distribution<> randsrc(0.002);
  int * data = make_rand_data<>(DATA_SIZE, randsrc);

  cout << " checking which of sampling versus sketching is better: " << endl;
  
  double quantiles_to_check[] = {0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95};
  int QUANTILES_TO_CHECK = sizeof(quantiles_to_check) /sizeof(double);
    
  double mean_error_with[APPROACHES][QUANTILES_TO_CHECK];
  int64_t true_quantile[QUANTILES_TO_CHECK];

  memset(mean_error_with, 0, sizeof(mean_error_with));
  SampleEstimation full_population;
  full_population.add_data(data, DATA_SIZE);
  for (int q = 0; q < QUANTILES_TO_CHECK; ++ q) {
     true_quantile[q]= full_population.quantile(quantiles_to_check[q]);
  }
  
  usec_t time_adding_items[APPROACHES];
  memset(time_adding_items, 0, sizeof(time_adding_items));
  usec_t time_querying[APPROACHES];
  memset(time_querying, 0, sizeof(time_querying));

  vector<string> labels;
  labels.push_back("sketch");
  labels.push_back("sample");
  labels.push_back("histogram");

  for (int i =0; i < TRIALS; ++i) {
    cout << "Trial " << i << endl;
    QuantileEstimation * estimators[APPROACHES];
    CMMultiSketch sketch(10, 6, 2 + i);
    ReservoirSample sample(sketch.size()/ sizeof(int));
    LogHistogram histo(80);
    
    estimators[0] = &sketch;
    estimators[1] = &sample;
    estimators[2] = &histo;
    
    if (i ==0)
      cout << "sketch size is " << (sketch.size()/1024)<< "kb and data is " << data_bytes/1024 << "kb\n";
    
    for (int a = 0; a < APPROACHES; ++a) {

      usec_t now = get_usec();
      estimators[a]->add_data(data, DATA_SIZE);
      time_adding_items[a] += (get_usec() - now);

      usec_t query_start = get_usec();
      for (int q = 0; q < QUANTILES_TO_CHECK; ++ q) {
        double quantile_pt = quantiles_to_check[q];
//      cout << " checking quantile " << quantile_pt<< " ("<<q<<"/"<< 5<<")\n";
        update_err(q, mean_error_with[a], true_quantile, estimators[a]->quantile(quantile_pt));
      }
      time_querying[a] += (get_usec() - query_start);
    }
//      cout << "  error was " << d << " or " << 100.0 * d /true_quantile[q] << "%\n";
  }
  
    //end queries, now we report results
  
  for (int q =0; q < QUANTILES_TO_CHECK; ++q) {

    cout << "\nQuantile " << quantiles_to_check[q] << " ("  << true_quantile[q]<< ")\n";
    for (int a = 0; a < APPROACHES; ++a) {
      mean_error_with[a][q] /= TRIALS;

      cout << labels[a] << " mean error: " << mean_error_with[a][q] << " or " <<
          (100.0 * mean_error_with[a][q] /true_quantile[q])<< "%"  << endl;
      }
  }
  
  for (int a = 0; a < APPROACHES; ++a) {
    cout << "Adding data to "<<labels[a] <<"  took " << time_adding_items[a]/TRIALS / 1000 <<
    "ms per " <<labels[a] << "; each query took " << time_querying[a]/TRIALS/QUANTILES_TO_CHECK << "us .\n";
  }
  delete[] data;
  
}
