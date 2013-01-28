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

namespace jetstream {

class QuantileEstimation {

  public:
    virtual int quantile(double q) =0;
    virtual size_t size() = 0; //
//    virtual size_t elems_represented() = 0;
    virtual ~QuantileEstimation() {}
};




class StatsSample: public QuantileEstimation {

  protected:
    std::vector<int> sample_of_data;
    bool is_sorted;

  public:
  
    StatsSample(): is_sorted(false) {}
  
    void add_data(int * data, size_t size_to_take) {
      is_sorted = false;
      sample_of_data.reserve(size_to_take);
      sample_of_data.assign(data, data + size_to_take);
    }
  
    int quantile(double q); 
  
   size_t size() {
    return sample_of_data.size() * sizeof(int);
   }


};

}

#endif
