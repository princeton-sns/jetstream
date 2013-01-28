#include "quantile_est.h"
#include "stdlib.h"
#include <algorithm>
#include <iostream>

using namespace ::std;

namespace jetstream {

int
GrowableSample::quantile(double q){
  assert (q < 1 && q >= 0);
  
  if (!is_sorted) {
    is_sorted = true;
    std::sort (sample_of_data.begin(), sample_of_data.end());
  }
  return sample_of_data[ sample_of_data.size() * q];
}


void
GrowableSample::add_item(int v, count_val_t c) {
  is_sorted = false;
  for (int i=0; i < c; ++i)
    sample_of_data.push_back(v);
}


LogHistogram::LogHistogram(size_t bucket_target):
  total_vals(0) {
  const int MAX_LAYERS = 10;
  
  size_t incr_per_layer[MAX_LAYERS];

  size_t layers = 3;
  assert(layers < MAX_LAYERS);

  layers = std::max( (size_t)3, bucket_target / 10);

  for(int i =0; i < layers; ++i)
    incr_per_layer[i] = 2;
  
  if(bucket_target < 5 * layers)
    bucket_target = 5 * layers;
  size_t precise_layers = std::min(bucket_target/5 -layers, layers);
  for (int b = 0; b < precise_layers; ++b)
    incr_per_layer[b] = 1;
  
  
  int exp = 1;
//  cout << "buckets: 0 ";
  bucket_starts.push_back(0);
  if (incr_per_layer[0] == 1)
    bucket_starts.push_back(1);
    
  for(int layer = 0; layer < layers; ++layer) {
    for (int i = 2; i <= 10; i+=incr_per_layer[layer]) {
      bucket_starts.push_back(i * exp);
//      cout << i * exp << " ";
    }
    exp *= 10;
  }
//  cout << endl;
  
  buckets.assign(bucket_starts.size(), 0);
}

int
LogHistogram::quantile(double q) {
  int i = quantile_bucket(q);
  return (bucket_max(i) + bucket_min(i)) / 2;
}

std::pair<int,int>
LogHistogram::bucket_bounds(size_t i) {
  std::pair<int,int> p;
  p.first = bucket_min(i);
  p.second = bucket_max(i);
  return p;
}

size_t
LogHistogram::quantile_bucket(double q) {
  
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
LogHistogram::bucket_with(int v) {
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

}