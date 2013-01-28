#include "quantile_est.h"
#include "stdlib.h"
#include <algorithm>

namespace jetstream {

int
StatsSample::quantile(double q){
  assert (q < 1 && q >= 0);
  
  if (!is_sorted) {
    is_sorted = true;
    std::sort (sample_of_data.begin(), sample_of_data.end());
  }
  return sample_of_data[ sample_of_data.size() * q];
}


}