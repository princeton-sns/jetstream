#ifndef __JetStream__sosp_operators__
#define __JetStream__sosp_operators__


#include "chain_ops.h"

namespace jetstream {


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
