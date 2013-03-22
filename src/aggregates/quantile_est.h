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
#include "jetstream_types.pb.h"

namespace jetstream {

typedef u_int32_t count_val_t; //the estimated counts


class QuantileEstimation {

  public:
    virtual int quantile(double q) =0;
    virtual size_t size() const = 0; //

    virtual void add_item(int, count_val_t) = 0;

    virtual void add_data(int * data, size_t size_to_take) {
      for (unsigned int i =0; i < size_to_take; ++i)
        add_item( data[i], 1);
    }

    virtual uint64_t pop_seen() const = 0;


    virtual void serialize_to(JSSummary&) const = 0;

//    virtual void clear();

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

    size_t elements() const {
      return sample_of_data.size();
    }

    virtual size_t size() const {
      return sample_of_data.size() * sizeof(int);
    }

    double mean() const;

    virtual uint64_t pop_seen() const {
      return sample_of_data.size();
    }

    virtual void serialize_to(JSSummary&) const;
};

class ReservoirSample: public SampleEstimation {
  protected:
    size_t max_size;
    size_t total_seen;
    inline void add_one(int v);
    boost::mt19937 gen;


    void fillIn(const JSSample&);

  public:
    ReservoirSample(int reserv_size): max_size(reserv_size),total_seen(0) {}
    ReservoirSample(const JSSample& s) {fillIn(s);};
    ReservoirSample(const JSSummary& s) { fillIn(s.sample()) ; }


    virtual void serialize_to(JSSummary& q) const ;

    virtual void add_item(int v, count_val_t c);
    virtual void add_data(int * data, size_t size_to_take);
    bool merge_in(const ReservoirSample& s);
  
    virtual uint64_t pop_seen() const {
      return total_seen;
    }
};

// could have a base histogram class here if need be

class LogHistogram : public QuantileEstimation {
/*
  Histogram of data, with a log distribution
*/
  protected:
    std::vector<count_val_t> buckets;
    std::vector<int> bucket_starts;
    uint64_t total_vals;

    size_t quantile_bucket(double d) const; //the bucket holding quantile d

    virtual void set_bucket_starts(size_t b_count);

    void fillIn(const JSHistogram&);

  public:
    LogHistogram(size_t buckets);
    LogHistogram(const JSHistogram& s): total_vals(0) { fillIn(s); }
    LogHistogram(const JSSummary& s): total_vals(0) { fillIn(s.histo()); }

    LogHistogram(const LogHistogram&); //no public copy constructor
    LogHistogram& operator=(const LogHistogram&);


    virtual int quantile(double q);

    virtual void add_item(int v, count_val_t c);

    virtual size_t size() const {
      return buckets.size() * sizeof(count_val_t);
    }

    virtual uint64_t pop_seen() const {
      return total_vals;
/*      uint64_t total = 0;
      for (size_t i = 0; i < buckets.size(); ++i)
        total+=buckets[i];
      return total;*/
    }


    std::pair<int,int> quantile_range(double q) const {
      return bucket_bounds(quantile_bucket(q));
    }

    count_val_t count_in_b(size_t b) const {
      assert(b < buckets.size());
      return buckets[b];
    }

    bool operator==(const LogHistogram & h) const;

    std::pair<int,int> bucket_bounds(size_t b) const;

    size_t bucket_with(int item) const;

    int bucket_min(size_t bucket_id) const {
      return bucket_starts[bucket_id];
    }

    int bucket_max(size_t bucket_id) const { //largest element that'll get sorted into bucket_id
      assert(bucket_id < buckets.size());
      if (bucket_id == buckets.size() -1)
        return std::numeric_limits<int>::max();
      return bucket_starts[bucket_id+1] -1;
    }

    virtual size_t bucket_count() const {
      return bucket_starts.size();
    }

    virtual void serialize_to(JSSummary&) const;

    bool merge_in(const LogHistogram & rhs);

};

std::ostream& operator<<(std::ostream& out, const LogHistogram&);

template<typename AggregateClass>
bool contains_aggregate(const jetstream::JSSummary  & summary);

template<typename AggregateClass>
void make_aggregate(jetstream::JSSummary  & summary);

template<typename AggregateClass>
bool should_make_into_aggregate(const jetstream::JSSummary  & lhs, const jetstream::JSSummary  & rhs);


template<typename AggregateClass>
void merge_into_aggregate(jetstream::JSSummary  &sum_into, const jetstream::JSSummary  &sum_update) {
  if(contains_aggregate<AggregateClass>(sum_into)) {
    AggregateClass agg_into(sum_into);

    if(contains_aggregate<AggregateClass>(sum_update)) {
      AggregateClass agg_update(sum_update);
      if (agg_into.size() >=agg_update.size())
        agg_into.merge_in(agg_update);
      else {
        AggregateClass dest(sum_update);
        dest.merge_in(agg_into);
        agg_into = dest;
      }
    }
    else {
      for(int i = 0; i < sum_update.items_size(); ++i) {
        agg_into.add_item(sum_update.items(i), 1);
      }
    }

    agg_into.serialize_to(sum_into);
  }
  else if(contains_aggregate<AggregateClass>(sum_update)) {
    AggregateClass agg_update(sum_update);

    for(int i = 0; i < sum_into.items_size(); ++i) {
      agg_update.add_item(sum_into.items(i), 1);
    }

    agg_update.serialize_to(sum_into);
    sum_into.clear_items();
  }
  else
  {
    assert(0); //LOG(FATAL) << "One of the summaries should be an aggregate";
  }
}


template<typename AggregateClass>
void merge_summary(jetstream::JSSummary  &sum_into, const jetstream::JSSummary  &sum_update) {
  if(!contains_aggregate<AggregateClass>(sum_into) && !contains_aggregate<AggregateClass>(sum_update))
  {
    //into and update are both lists
    if(should_make_into_aggregate<AggregateClass>(sum_into, sum_update))
    {
      make_aggregate<AggregateClass>(sum_into);
      merge_into_aggregate<AggregateClass>(sum_into, sum_update);
    }
    else
    {
      sum_into.mutable_items()->MergeFrom(sum_update.items());
    }
  }
  else
  {
    merge_into_aggregate<AggregateClass>(sum_into, sum_update);
  }
}







}

#endif
