
#include "cm_sketch.h"
#include "js_utils.h"

#include <gtest/gtest.h>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/discrete_distribution.hpp>

#include <boost/random/normal_distribution.hpp>
#include <boost/timer/timer.hpp>
#include <boost/random/exponential_distribution.hpp>
#include <glog/logging.h>

#include <limits>
#include <math.h>
#include <fstream>



using namespace ::std;
using namespace jetstream;




template <typename T>
int * make_rand_data(size_t size, T& randsrc) {
  boost::mt19937 gen;
  int* data = new int[size];
  for (unsigned int i=0; i < size; ++ i)
    data[i] = (int) randsrc(gen);
  return data;
}



class DataMaker {
  public:
    virtual int * operator()(size_t t) = 0;

    virtual string name() = 0;
  
    virtual size_t size() {return 0;}
};

class UniformData: public DataMaker {
  public:
    virtual int * operator()(size_t t) {
      boost::random::uniform_int_distribution<> randsrc(1, t /2);
      return make_rand_data<>(t, randsrc);
    }

    virtual string name() { return "uniform"; }  
};

class ExpData: public DataMaker {
  double param;
  public:
     ExpData(double p = 0.002): param(p) {}
  
    virtual int * operator()(size_t t) {
      boost::random::exponential_distribution<> randsrc(param);
      return make_rand_data<>(t, randsrc);
    }
    virtual string name() { return "exponential-" + boost::lexical_cast<string>(param); }
};

class NormalData: public DataMaker {
  int mean, stddev;
  
  public:
    NormalData(): mean(10000), stddev(1000) {}
    virtual int * operator()(size_t t) {
      boost::random::normal_distribution<> randsrc(mean, stddev);
      return make_rand_data<>(t, randsrc);
    }
    virtual string name() { return "normal-" + boost::lexical_cast<string>(mean)
     + "-" + boost::lexical_cast<string>(stddev); }
};

class ZipfData: public DataMaker {
  double param;
  size_t max_v;
  public:
    ZipfData(double p, size_t m):param(p), max_v(m) {}
  
    virtual int * operator()(const size_t t) {
      boost::mt19937 gen;
      double * weights = new double[max_v];
      int * data = new int[t];
      
      double total_weight = 0;
//      cout << "zipf weights: ";
      
      for (unsigned i = 0; i < max_v; ++i) {
        weights[i] = pow(i+1, -param);
        total_weight += weights[i];
 //       cout << " " << weights[i];
      }
//      cout << "\ntotal is " << total_weight<< endl;
      
      //build probabilities
      ;//index into data
      unsigned w_idx = 0; //index into weights
      unsigned to_add =  weights[0] * t / total_weight;
      unsigned effective_t = t;
      for (unsigned d_idx = 0; d_idx < t; ++d_idx) {

        data[d_idx] = w_idx + 1; //data is never zero
        --to_add;
        while(to_add == 0) {
          w_idx ++;
          if(w_idx >= max_v) {
            w_idx = 0; // TODO the wraparound here is super ugly
            effective_t = t - d_idx;
          }
          to_add= weights[w_idx] * effective_t / total_weight;
          
          double probabilities[2];
          probabilities[1] = weights[w_idx] * effective_t / total_weight - double(to_add);
          probabilities[0] = 1.0 - probabilities[1];
          boost::random::discrete_distribution<> maybe_more(probabilities);
          
          to_add += maybe_more(gen);            
        }
      }
      delete [] weights;
      return data;
    }
    virtual string name() { return "zipf-" + boost::lexical_cast<string>(param); }
};



class EmpiricalData: public DataMaker {
  string file;
  vector<int> data;
  public:
    EmpiricalData(string s="data.out"): file(s) {}
  
    size_t size() { return data.size();}
  
    virtual int * operator()(size_t t) {
    
      if (data.size() == 0)
        read_data();
     int * ret = new int[data.size()];
     memcpy(ret, data.data(), data.size());
     return ret;
    }
  
    void read_data() {
      data.reserve(1000 * 1000);
      
      ifstream in;
      in.open(file.c_str());
      int i = 0;

      while (in >> i) {
        if ( i > 0)
          data.push_back(i);
      }
      in.close();
      cout << "read " << data.size() << " ints from " << file << endl;
    }
  
  
    virtual string name() { return "empirical-" + file; }
};


class SporadicData: public DataMaker {
    unsigned points, max_val;
    
public:
    SporadicData(): points(200), max_val(1 << 27) {}
    virtual int * operator()(size_t t) {
        int * ret = new int[t];
        boost::mt19937 gen;
        boost::random::uniform_int_distribution<> randsrc(1, max_val);
        
        unsigned int i = 0;
        const int64_t ratio = 32;
        for (int part = 0; part < 2; ++part) {
        
          unsigned end_of_part = unsigned((ratio - 1) * t / ratio + t * part / ratio);
          unsigned off = i;
          for (; i < points; ++ i)
              ret[i] = (int) randsrc(gen);
          for (; i < end_of_part; ++i)
              ret[i] = ret[i % points + off];
        }
        return ret;
    }
    virtual string name() { return "sporadic-" + boost::lexical_cast<string>(points)
        + "-" + boost::lexical_cast<string>(max_val); }
};



TEST(DISABLED_Datagen, ZipfDist) {
  ZipfData d(1.3, 10000);
  const int SIZE = 50;
  int * data = d(SIZE);
  cout << "zipf data:";
  for (int i = 0; i < SIZE; ++i)
    cout << " " << data[i];
  cout << endl;
  delete[] data;

}


double update_err(int q, double* mean_error, int64_t* true_quantile, int est) {
  double err = abs( est - true_quantile[q]);
  mean_error[q] +=  err ;
  return err / true_quantile[q];
}

void compareOnce(ofstream& data_out, const int DATA_SIZE, const int sketch_w,
    DataMaker& maker, bool use_sketch = true) {

  const int TRIALS = 8;

  const int APPROACHES = use_sketch ? 3 : 2;
  
  size_t data_bytes = DATA_SIZE * sizeof(int);

//  boost::random::normal_distribution<> randsrc(10000, 1000);
//  boost::random::exponential_distribution<> randsrc(0.002);
//  boost::random::exponential_distribution<> randsrc(0.02);

  double quantiles_to_check[] = {0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95};
  int QUANTILES_TO_CHECK = sizeof(quantiles_to_check) /sizeof(double);
    
  double mean_error_with[APPROACHES][QUANTILES_TO_CHECK];
  int64_t true_quantile[QUANTILES_TO_CHECK];

  memset(mean_error_with, 0, sizeof(mean_error_with));
  usec_t time_adding_items[APPROACHES];
  memset(time_adding_items, 0, sizeof(time_adding_items));
  usec_t time_querying[APPROACHES];
  memset(time_querying, 0, sizeof(time_querying));

  vector<string> labels;
  labels.push_back("sample");
  labels.push_back("histogram");
  if(use_sketch)
    labels.push_back("sketch");

  for (int i =0; i < TRIALS; ++i) {
    LOG(INFO) << "Trial " << i << endl;

    int * data = maker(DATA_SIZE);
    SampleEstimation full_population;
    full_population.add_data(data, DATA_SIZE);
    for (int q = 0; q < QUANTILES_TO_CHECK; ++ q) {
       true_quantile[q]= full_population.quantile(quantiles_to_check[q]);
    }
    
    
    QuantileEstimation * estimators[APPROACHES];
    int ints_in_summary = 0;
    if (use_sketch) {
      CMMultiSketch sketch(sketch_w, 6, 2 + i);
      estimators[2] = &sketch;
      ints_in_summary = sketch.size()/ sizeof(int);
    } else {
      ints_in_summary  = (1 << (sketch_w + 4)) ;
    }
    ReservoirSample sample(ints_in_summary);
    LogHistogram histo( ints_in_summary );
    
    estimators[0] = &sample;
    estimators[1] = &histo;
    
    int summary_size = ints_in_summary * sizeof(int)/1024;
    
    if (i ==0) {
      cout << "sketch-w " << sketch_w<< ". Size is " << summary_size<<
            "kb and data is " << data_bytes/1024 << "kb from " << maker.name();
      cout << "Histograms have " << histo.bucket_count() << " cells\n";
      data_out << "DATA: " << maker.name() << " sketch/sample size " << summary_size << endl;
    }
    
    for (int a = 0; a < APPROACHES; ++a) {

      usec_t now = get_usec();
      estimators[a]->add_data(data, DATA_SIZE);
      time_adding_items[a] += (get_usec() - now);

      usec_t query_start = get_usec();
      for (int q = 0; q < QUANTILES_TO_CHECK; ++ q) {
        double quantile_pt = quantiles_to_check[q];
//      cout << " checking quantile " << quantile_pt<< " ("<<q<<"/"<< 5<<")\n";
        double err = update_err(q, mean_error_with[a], true_quantile, estimators[a]->quantile(quantile_pt));
        data_out << "rel err for " <<  labels[a]<< " " << summary_size << " "  << quantile_pt << " " << err << endl;
      }
      time_querying[a] += (get_usec() - query_start);
    }
//      cout << "  error was " << d << " or " << 100.0 * d /true_quantile[q] << "%\n";
    delete[] data;

  }
  
    //end queries, now we report results
  
  data_out << "Quantile,True Value,Sketch Err,Sample Err,Histo Err\n"; 
  for (int q =0; q < QUANTILES_TO_CHECK; ++q) {

    cout << "\nQuantile " << quantiles_to_check[q] << " ("  << true_quantile[q]<< ")\n";
    data_out << quantiles_to_check[q] << "," << true_quantile[q];
    for (int a = 0; a < APPROACHES; ++a) {
      mean_error_with[a][q] /= TRIALS;

      cout << labels[a] << " mean error: " << mean_error_with[a][q] << " or " <<
          (100.0 * mean_error_with[a][q] /true_quantile[q])<< "%"  << endl;
      data_out<< "," << mean_error_with[a][q];
    }
    data_out << endl;
  }
  
  for (int a = 0; a < APPROACHES; ++a) {
    cout << "Adding data to "<<labels[a] <<"  took " << time_adding_items[a]/TRIALS / 1000 <<
    "ms per " <<labels[a] << "; each query took " << time_querying[a]/TRIALS/QUANTILES_TO_CHECK << "us .\n";
  }
}


//use --gtest_also_run_disabled_tests to run
TEST(DISABLED_CMSketch, SketchVsSample) {
  const unsigned int DATA_SIZE = 1024* 1024 * 8;
  const int SKETCH_W = 10;
  ofstream data_out;
  data_out.open("quant_est_comparison.out");
  ZipfData u(1.2, 100* 1000);
  compareOnce(data_out, DATA_SIZE, SKETCH_W, u);
  data_out.close();
}

TEST(DISABLED_CMSketch, MultiComp) {

  unsigned int DATA_SIZE = 1024* 1024 * 8; //ints, not bytes
  ofstream data_out;
  data_out.open("quant_est_comparison.out");
  
  vector<DataMaker *> distribs;
  

  EmpiricalData * empirical_sizes = new EmpiricalData("c_sizes.out");
  distribs.push_back(empirical_sizes);
  distribs.push_back(new EmpiricalData("c_times.out"));
  for (int i = 0; i < 2; ++i)
    ((EmpiricalData*)distribs[i])->read_data();

  cout << "empirical data size is " << empirical_sizes->size() << " ints" <<endl;
  assert (empirical_sizes->size()  > 0);

  distribs.push_back ( new UniformData);
  distribs.push_back ( new ExpData);
  distribs.push_back (new NormalData);

  distribs.push_back( new SporadicData);
  distribs.push_back(new ZipfData(1.2, 100 * 1000));
  
  for (unsigned int d = 0; d < distribs.size(); ++d) {
    DataMaker * distrib = distribs[d];
    size_t s = distrib->size();
    if (s == 0)
      s = DATA_SIZE;
    for (int sketchw = 4; sketchw < 9; sketchw += 1) {
      compareOnce(data_out, s, sketchw, *(distrib), false);
    }
  }
  
  data_out.close();
}

