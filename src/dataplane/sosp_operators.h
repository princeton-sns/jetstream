#ifndef __JetStream__sosp_operators__
#define __JetStream__sosp_operators__


#include "chain_ops.h"
#include <boost/filesystem.hpp>
#include "quantile_est.h"

using boost::filesystem::directory_iterator;

namespace jetstream {

/**
Reads every file in directory, ordered alphabetically by file name.
Each file is emitted as a single tuple, with the file name (just the file, not the
dir) and then a blob with the contents.
*/
class BlobReader: public TimerSource {

 public:
  BlobReader(): cur_path(0) {}
  
  virtual int emit_data();
  virtual operator_err_t configure(std::map<std::string,std::string> &config);


  private:
    static const unsigned MAX_READ_SIZE = 10 * (1 <<20); // 10 MB
    std::string dirname;
    std::string prefix;
    std::vector<boost::filesystem::path> paths;
    unsigned cur_path;
    unsigned ms_per_file;  //read interval
GENERIC_CLNAME
};  


class ImageSampler: public CEachOperator {

 public:
  virtual void process_one(boost::shared_ptr<Tuple>& t);
  virtual operator_err_t configure(std::map<std::string,std::string> &config);


GENERIC_CLNAME
};  




class ImageQualityReporter: public COperator {

 public:
  ImageQualityReporter():
    period_ms(5000),  ts_field(2), chains(0), bytes_this_period(0), latencies_this_period(500),
    latencies_total(1000), logging_filename("image_quality.out"){}

  virtual void process ( OperatorChain * c,
                         std::vector<boost::shared_ptr<Tuple> > & tuples,
                        DataplaneMessage& msg);

  virtual operator_err_t configure(std::map<std::string,std::string> &config);

  virtual void add_chain(boost::shared_ptr<OperatorChain>);
  virtual void chain_stopping(OperatorChain * );
  virtual bool is_source() {return true;} //so we get a chain of our own, even without incoming connections
      // Needed to make sure we get stopped properly; chains == threads, and we have a strand.


  void emit_stats();

 private:
  unsigned period_ms;
  unsigned ts_field;
  volatile unsigned chains;
  volatile bool running;
  boost::shared_ptr<boost::asio::deadline_timer> timer;
  boost::mutex mutex;

  long long bytes_this_period;
  
  LogHistogram latencies_this_period, latencies_total;
  
  std::string logging_filename;
  std::ostream * out_stream;
  std::map<OperatorChain *, unsigned> chain_indexes;
  inline unsigned get_chain_index(OperatorChain *);
  std::vector<long> bytes_per_src_in_period;
  std::vector<long long> bytes_per_src_total;
  

GENERIC_CLNAME
};  




class SeqToRatio: public CEachOperator {
  //logs the total counts going past
 public:

  SeqToRatio(): total_field(0),total_val(0), respcode_field(0), url_field(0) {}
  virtual void process_one(boost::shared_ptr<Tuple>& t);
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
//  virtual void meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred);


 private:
  unsigned total_field;
  double total_val;
  unsigned respcode_field;
  unsigned url_field;
  std::string cur_url;
  boost::shared_ptr<Tuple> targ_el;
  
GENERIC_CLNAME
};  




}


#endif /* defined(__JetStream__sosp_operators__) */
