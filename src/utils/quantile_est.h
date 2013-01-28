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

typedef u_int32_t count_val_t; //the estimated counts


class QuantileEstimation {

  public:
    virtual int quantile(double q) =0;
    virtual size_t size() = 0; //
  
    virtual void add_data(int, count_val_t) = 0;
  
    virtual void add_data(int * data, size_t size_to_take) {
      for (int i =0; i < size_to_take; ++i)
        add_data( data[i], 1);
    }

//    virtual size_t elems_represented() = 0;
    virtual ~QuantileEstimation() {}
};




class StatsSample: public QuantileEstimation {

  protected:
    std::vector<int> sample_of_data;
    bool is_sorted;

  public:
  
    StatsSample(): is_sorted(false) {}
  
    virtual void add_data(int * data, size_t size_to_take) {
      is_sorted = false;
      sample_of_data.reserve(size_to_take);
      sample_of_data.assign(data, data + size_to_take);
    }
  
    virtual void add_data(int v, count_val_t c);

  
    virtual int quantile(double q);
  
    virtual size_t size() {
      return sample_of_data.size() * sizeof(int);
   }
};

class LogDistHistogram : public QuantileEstimation {
/*
  Histogram of data, with a log distribution 
*/
  protected:
    std::vector<count_val_t> counts;
    size_t buckets;
  
  public:
    LogDistHistogram(int min, int max, size_t buckets);
    virtual int quantile(double q);

    virtual void add_data(int v, count_val_t c);
  
    virtual size_t size() {
    
    }


};

}

#endif
