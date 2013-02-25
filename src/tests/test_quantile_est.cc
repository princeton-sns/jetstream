
#include "cm_sketch.h"
#include "js_utils.h"

#include <gtest/gtest.h>

#include <glog/logging.h>

#include <limits>
#include <math.h>
#include <fstream>

using namespace ::std;
using namespace jetstream;


TEST(QuantCMSketch, AddAndQuery) {

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


TEST(QuantCMSketch, Quantile) {
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

TEST(QuantCMSketch, MultiInit) {

  cout << "initializing 100 sketches, each 10kb"<< endl;
  for(int i = 0; i < 100; ++i) {
    CMSketch c(8, 10, 2 + i);
  }
}

TEST(QuantCMSketch, Merge) {
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

TEST(QuantCMSketch, SerDe) {
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


TEST(QuantReservoirSample, Merge) {

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

TEST(QuantReservoirSample, Mean) {
  const int ITEMS = 20;
  {
    ReservoirSample agg(30);

    for (int j =0; j < 2; ++j)
      for(int i = 0; i < ITEMS; ++i) {
        agg.add_item(i+(10*j), 1);
      }
    ASSERT_GT(16.0, agg.mean());
    ASSERT_LT(13.0, agg.mean());
  }
  
  {
    ReservoirSample a1(30), a2(30);
  
    for(int i = 0; i < ITEMS; ++i) {
      a1.add_item(i, 1);
      a2.add_item(i + 20, 1);
    }
    a1.merge_in(a2);
    ASSERT_GT(21.0, a1.mean());
    ASSERT_LT(18.0, a1.mean());
  }
}

TEST(QuantReservoirSample, SerDe) {

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
  ASSERT_EQ(summary.pop_seen(), deserialized.pop_seen());

}

TEST(QuantLogHistogram, SerDe) {

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

TEST(QuantLogHistogram, Boundaries) {
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

TEST(QuantLogHistogram, Buckets) {
  LogHistogram hist(600);
  for (int i = 0; i < 30; ++i) {
    ASSERT_EQ(i, hist.bucket_min(i));
    cout << hist.bucket_min(i) << " ";
  }
  cout << endl;
}

TEST(QuantLogHistogram, Merge) {
  const unsigned ITEMS = 20;
  LogHistogram src_hist(600);
  
  for(unsigned i = 0; i < ITEMS; ++i)
    src_hist.add_item(i*i, i + 2);

  LogHistogram * dests[2];
  dests[0] = new LogHistogram(100);
  dests[1] = new LogHistogram(600);
  
  for (int d = 0; d < 2; ++d) {
    dests[d]->merge_in(src_hist);
    for (unsigned i = 0; i < ITEMS; ++i) {
      ASSERT_LE(i+2, dests[d]->count_in_b( dests[d]->bucket_with(i * i) ));
    }
  }
}

TEST(QuantLogHistogram, MergeSmall) {

  for (int s_size = 2; s_size < 6; ++s_size) {
    for (int d_size = 2; d_size <= s_size; ++d_size) {
      LogHistogram src_hist(s_size);
      LogHistogram dest_hist(d_size);
      
      src_hist.add_item(2, 1);
      dest_hist.merge_in(src_hist);
    }
  }
}

TEST(QuantLogHistogram, Quantile) {

  const int DATA_SIZE = 1000;
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

