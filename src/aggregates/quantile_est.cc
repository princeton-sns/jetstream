#include "quantile_est.h"
#include "stdlib.h"
#include <algorithm>
#include <iostream>
#include <boost/random/uniform_int.hpp>
#include <boost/random/discrete_distribution.hpp>


using namespace ::std;
using namespace boost::random;

namespace jetstream {

int
SampleEstimation::quantile(double q) {
  assert (q < 1 && q >= 0);
  
  if (!is_sorted) {
    is_sorted = true;
    std::sort (sample_of_data.begin(), sample_of_data.end());
  }
  return sample_of_data[ sample_of_data.size() * q];
}


void
SampleEstimation::add_item(int v, count_val_t c) {
  is_sorted = false;
  for (size_t i=0; i < c; ++i)
    sample_of_data.push_back(v);
}

double
SampleEstimation::mean() const {
  int64_t total = 0;
  for (size_t i=0; i < sample_of_data.size(); ++i)
    total+= sample_of_data[i];
  return (double) total /  sample_of_data.size();
}

inline void
ReservoirSample::add_one(int v) {
  if( sample_of_data.size() < max_size)
      sample_of_data.push_back(v);
  else {
    uniform_int_distribution<uint64_t> randsrc(0, total_seen-1); //note that this is different each time
    uint64_t i_to_replace = randsrc(gen);
    if (i_to_replace < max_size)
      sample_of_data[i_to_replace] = v;
  }
  total_seen += 1;  
}

void
ReservoirSample::add_item(int v, count_val_t c) {
  for (count_val_t i =0; i < c; ++i)
    add_one(v);
}


void
ReservoirSample::add_data(int * data, size_t size_to_take) {
  sample_of_data.reserve(size_to_take);
  for (size_t i =0; i < size_to_take; ++i)
    add_one(data[i]);
}


//http://gregable.com/2007/10/reservoir-sampling.html
bool
ReservoirSample::merge_in(const ReservoirSample& rhs) {
/**
  Four cases:  this reservoir not full and rhs not full. No sampling has happened yet.
  Can just add each element in rhs to this.
    Ditto for this reservoir full and rhs not full.
    
    If this reservoir is full, can handle rhs-full and not-full using weighted choice below.
    
    Last case:  This reservoir not-full but RHS is full. 
*/

  if (rhs.elements() < rhs.max_size) { //RHS is unweighted
    for (size_t i = 0; i < rhs.elements(); ++i) {
      add_item(rhs.sample_of_data[i], 1);
    }
    return true;
  } else {
  
    size_t rhs_seen = rhs.total_seen;
    while (total_seen < max_size) {
      uniform_int_distribution<size_t> start_pos_distrib(0, rhs.elements() -1 );
      size_t rhs_idx = start_pos_distrib(gen);
      add_item(rhs.sample_of_data[rhs_idx], 1);
      rhs_seen --;
    }
    assert (total_seen >= max_size);
    //TODO all this code assumes the destination reservoir is full

    double probabilities[2];
    probabilities[0] = (double) total_seen / (total_seen + rhs_seen);
    probabilities[1] = 1.0 - probabilities[0];
    boost::random::discrete_distribution<> which_source(probabilities);

    uniform_int_distribution<size_t> start_pos_distrib(0, rhs.elements() -1 );
    size_t rhs_idx = start_pos_distrib(gen);
    for (size_t i = 0; i < elements(); ++i) {
      int which = which_source(gen);
      if (which == 1) {
        sample_of_data[i] = rhs.sample_of_data[rhs_idx];
        rhs_idx = (rhs_idx + 1) % rhs.elements();
      }  
    }
    return true;
  }
}


LogHistogram::LogHistogram(size_t bt):
  total_vals(0), bucket_target(bt) {
  set_bucket_starts(bucket_target);
  buckets.assign(bucket_starts.size(), 0);
}

void
LogHistogram::set_bucket_starts(size_t bucket_target) {
  const size_t MAX_LAYERS = 10;
  
  size_t incr_per_layer[MAX_LAYERS];

  size_t layers = 3;
  assert(layers < MAX_LAYERS);

  layers = std::max( (size_t)3, bucket_target / 10);

  for(size_t i =0; i < layers; ++i)
    incr_per_layer[i] = 2;
  
  if(bucket_target < 5 * layers)
    bucket_target = 5 * layers;
  size_t precise_layers = std::min(bucket_target/5 -layers, layers);
  for (size_t b = 0; b < precise_layers; ++b)
    incr_per_layer[b] = 1;
  
  
  int exp = 1;
//  cout << "buckets: 0 ";
  bucket_starts.push_back(0);
  if (incr_per_layer[0] == 1)
    bucket_starts.push_back(1);
    
  for(size_t layer = 0; layer < layers; ++layer) {
    for (int i = 2; i <= 10; i+=incr_per_layer[layer]) {
      bucket_starts.push_back(i * exp);
//      cout << i * exp << " ";
    }
    exp *= 10;
  }
//  cout << endl;
}


int
LogHistogram::quantile(double q) {
  int i = quantile_bucket(q);
  return (bucket_max(i) + bucket_min(i)) / 2;
}

std::pair<int,int>
LogHistogram::bucket_bounds(size_t i) const {
  std::pair<int,int> p;
  p.first = bucket_min(i);
  p.second = bucket_max(i);
  return p;
}

size_t
LogHistogram::quantile_bucket(double q) const {
  
  count_val_t cum_sum = 0, target_sum = (count_val_t) (q * total_vals);
  
  for (unsigned int i = 0; i < buckets.size(); ++i) {

    cum_sum += buckets[i];  
    if (cum_sum > target_sum) {
      return i;
    }
  }
  return std::numeric_limits<int>::max();
}

size_t
LogHistogram::bucket_with(int v) const {
  size_t b_hi = buckets.size() -1;
  size_t b_low = 0;
  size_t b = b_low;
  while ( b_hi >= b_low) {
    b = (b_hi + b_low) /2;
    if( bucket_min(b) > v)
      b_hi = b -1;
    else if (bucket_max(b) < v )
      b_low = b+1;
    else
      break; //    v is in bucket b!
  }
  return b;
}

void
LogHistogram::add_item(int v, count_val_t c) {
  total_vals += c;
  
  int b = bucket_with(v);
  //post-condition:  v is in bucket b
  buckets[b] += c;
}

LogHistogram::LogHistogram(const JSHistogram& serialized):
  total_vals(0), bucket_target(serialized.num_buckets()){
  set_bucket_starts( bucket_target);
  buckets.assign(bucket_starts.size(), 0);

  for(int i = 0; i < serialized.bucket_vals_size(); ++i) {
    buckets[i] = serialized.bucket_vals(i);
    total_vals += buckets[i];
  }
}

void
LogHistogram::serialize_to(JSSummary& q) const {
  JSHistogram * serialized_hist = q.mutable_histo();
  serialized_hist->set_num_buckets(bucket_target);
  for(unsigned int b =0; b <  bucket_count(); ++b) {
    serialized_hist->add_bucket_vals(buckets[b]);
  }
}

bool
LogHistogram::merge_in(const LogHistogram & rhs) {
  assert ( rhs.bucket_count() == bucket_count());
  for(unsigned int b =0; b <  bucket_count(); ++b) {
    buckets[b] += rhs.buckets[b];
  }
  
  return true;
}

bool
LogHistogram::operator==(const LogHistogram & rhs) const {
  if (rhs.bucket_count() != bucket_count())
    return false;
  for (size_t i = 0; i < bucket_count(); ++i)
    if (buckets[i] != rhs.buckets[i])
      return false;
  assert(total_vals == rhs.total_vals);
  return true;
}


std::ostream& operator<<(std::ostream& out, const LogHistogram& hist) {
  for(unsigned int b =0; b < hist.bucket_count(); ++b) {
    std::pair<int, int> bucket_bounds = hist.bucket_bounds(b);
    out << "in "<< bucket_bounds.first << ", " << bucket_bounds.second <<"] "
      << hist.count_in_b(b) << endl;
  }
  return out;
}

}
