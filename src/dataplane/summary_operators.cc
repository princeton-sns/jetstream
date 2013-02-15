#include "summary_operators.h"
#include "quantile_est.h"
#include "cm_sketch.h"
#include "js_utils.h"
#include <glog/logging.h>


using namespace std;
using namespace boost;

using namespace boost::posix_time;


namespace jetstream {



void
QuantileOperator::process(boost::shared_ptr<Tuple> t) {
  if (t->e(field).has_summary()) {
    const JSSummary& s= t->e(field).summary();
    QuantileEstimation * est;
    if (s.has_histo()) {
      est = new LogHistogram(s.histo());
    } else if (s.has_sample()) {
      est = new ReservoirSample(s.sample());
    } else if (s.has_sketch()) {
      est = new CMMultiSketch(s.sketch());
    } else {
      LOG(FATAL) << " got a summary with no specific summary in it";
    }
    int q_result = est->quantile(q);
    t->mutable_e(field)->set_i_val(q_result);
    t->mutable_e(field)->clear_summary();
    delete est;
    emit(t);
  } else {
    LOG(FATAL) << "no summary in field " << field << " of "<< fmt(*t);
  }
}


operator_err_t QuantileOperator::configure(std::map<std::string,std::string> &config) {

  if( !(istringstream(config["q"]) >> q) || (q <= 0) || (q >= 1))
    return operator_err_t("q must be between 0 and 1; got " + config["q"]);
  
  if( !(istringstream(config["field"]) >> field))
    return operator_err_t("must specify a field; got " + config["field"]);
  return NO_ERR;
}



const string QuantileOperator::my_type_name("Quantile");


}