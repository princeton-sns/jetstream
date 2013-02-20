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


void SummaryToCount::process(boost::shared_ptr<Tuple> t) {
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
    size_t result = est->pop_seen();
//    LOG(INFO) << " incoming histo had " << result << " total values:";
 //   LOG(INFO) <<  *((LogHistogram*)(est));

    t->add_e()->set_i_val(result);
    delete est;
    emit(t);
  } else
    LOG(FATAL) << "no summary in field " << field << " of "<< fmt(*t);
}

operator_err_t
SummaryToCount::configure(std::map<std::string,std::string> &config) {
  if( !(istringstream(config["field"]) >> field))
    return operator_err_t("must specify a field; got " + config["field"]);
  return NO_ERR;
}


void
ToSummary::process(boost::shared_ptr<Tuple> t) {
  if ( (  unsigned(t->e_size()) <= field) || !t->e(field).has_i_val())
    return;
  
  int i = t->e(field).i_val();

  LogHistogram l(s_size);
  assert(i >= 0);
  l.add_item(i, 1);
  JSSummary * s = t->mutable_e(field)->mutable_summary();
  l.serialize_to(*s);
  t->mutable_e(field)->clear_i_val();
  emit(t);
}

operator_err_t ToSummary::configure(std::map<std::string,std::string> &config) {
  if( !(istringstream(config["field"]) >> field))
    return operator_err_t("must specify a field; got " + config["field"]);
  if( !(istringstream(config["size"]) >> s_size))
    return operator_err_t("must specify a summary size; got " + config["size"]);

  return NO_ERR;
}



const string QuantileOperator::my_type_name("Quantile");
const string ToSummary::my_type_name("to-summary");
const string SummaryToCount::my_type_name("to-count");

}