//
//  quantile_est.h
//  JetStream
//
//  Created by Ariel Rabkin on 1/28/13.
//  Copyright (c) 2013 Ariel Rabkin. All rights reserved.
//

#ifndef JetStream_quantile_est_h
#define JetStream_quantile_est_h

#include <vector>
#include <assert.h>
#include <limits>
#include <boost/random/mersenne_twister.hpp>


namespace jetstream {

typedef u_int32_t count_val_t; //the estimated counts


class QuantileEstimation {

  public:
    virtual int quantile(double q) =0;
    virtual size_t size() = 0; //
  
    virtual void add_item(int, count_val_t) = 0;
  
    virtual void add_data(int * data, size_t size_to_take) {
      for (unsigned int i =0; i < size_to_take; ++i)
        add_item( data[i], 1);
    }

//    virtual size_t elems_represented() = 0;
    virtual ~QuantileEstimation() {}
};


class SampleEstimation: public QuantileEstimation {

  protected:
    std::vector<int> sample_of_data;
    bool is_sorted;

  public:
  
    SampleEstimation(): is_sorted(false) {}
  
    virtual void add_data(int * data, size_t size_to_take) {
      is_sorted = false;
      sample_of_data.reserve(size_to_take);
      sample_of_data.assign(data, data + size_to_take);
    }
  
    virtual void add_item(int v, count_val_t c);

  
    virtual int quantile(double q);
  
    virtual size_t size() {
      return sample_of_data.size() * sizeof(int);
   }
};

class ReservoirSample: public SampleEstimation {
  protected:
    size_t max_size;
    size_t total_seen;
    inline void add_one(int v);
    boost::mt19937 gen;

  public:
    ReservoirSample(int reserv_size): max_size(reserv_size),total_seen(0) {}
    virtual void add_item(int v, count_val_t c);
    virtual void add_data(int * data, size_t size_to_take);
  
};

// could have a base histogram class here if need be

class LogHistogram : public QuantileEstimation {
/*
  Histogram of data, with a log distribution 
*/
  protected:
    std::vector<count_val_t> buckets;
//    size_t num_buckets;
    std::vector<int> bucket_starts;

    count_val_t total_vals;  
    size_t quantile_bucket(double d); //the bucket holding quantile d
  
  public:
    LogHistogram(size_t buckets);
    virtual int quantile(double q);
    virtual void add_item(int v, count_val_t c);


    virtual size_t size() {
      return buckets.size() * sizeof(count_val_t);
    }
  
    std::pair<int,int> quantile_range(double q) {
      return bucket_bounds(quantile_bucket(q));
    }

    count_val_t count_in_b(size_t b) {
      assert(b < buckets.size());
      return buckets[b];
    }
  
  
    std::pair<int,int> bucket_bounds(size_t b);
    size_t bucket_with(int item);

  
    int bucket_min(size_t bucket_id) {
      return bucket_starts[bucket_id];
    }
    int bucket_max(size_t bucket_id) { //largest element that'll get sorted into bucket_id
      assert(bucket_id < buckets.size());
      if (bucket_id == buckets.size() -1)
        return std::numeric_limits<int>::max();
      return bucket_starts[bucket_id+1] -1;
    }
  
   virtual size_t bucket_count() {
      return bucket_starts.size();
   }
  
};

}

#endif