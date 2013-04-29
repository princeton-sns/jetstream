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
QuantileOperator::process_one(boost::shared_ptr<Tuple> & t) {
  int q_result;
  if (t->e(field).has_summary()) {
    const JSSummary& s= t->e(field).summary();
    if (s.has_histo()) {
      LogHistogram est(s.histo());
      q_result = est.quantile(q);
    } else if (s.has_sample()) {
      ReservoirSample est(s.sample());
      q_result = est.quantile(q);
    } else if (s.has_sketch()) {
      CMMultiSketch est(s.sketch());
      q_result = est.quantile(q);
    } else if (s.items_size() > 0) {
      JSSummary *ms= t->mutable_e(field)->mutable_summary();
      std::sort(ms->mutable_items()->begin(), ms->mutable_items()->end());
      q_result = s.items(s.items_size()*q);
    }
    else {
      LOG(FATAL) << " got a summary with no specific summary in it";
    }
    t->mutable_e(field)->set_i_val(q_result);
    t->mutable_e(field)->clear_summary();
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


void
SummaryToCount::process_one (boost::shared_ptr<Tuple>& t) {

  if (t->e(field).has_summary()) {
    const JSSummary& s= t->e(field).summary();
    QuantileEstimation * est;
    if (s.has_histo()) {
      est = new LogHistogram(s.histo());
    } else if (s.has_sample()) {
      est = new ReservoirSample(s.sample());
    } else if (s.has_sketch()) {
      est = new CMMultiSketch(s.sketch());
    } else if (s.items_size() > 0) {
      t->add_e()->set_i_val(s.items_size());
      return;
    } else {
      LOG(FATAL) << " got a summary with no specific summary in it";
    }
    size_t result = est->pop_seen();
//    LOG(INFO) << " incoming histo had " << result << " total values:";
 //   LOG(INFO) <<  *((LogHistogram*)(est));

    t->add_e()->set_i_val(result);
  } else
    LOG(FATAL) << "no summary in field " << field << " of "<< fmt(*t);
}

operator_err_t
SummaryToCount::configure(std::map<std::string,std::string> &config) {
  if(config["field"].length() == 0 || !(istringstream(config["field"]) >> field))
    return operator_err_t("must specify a field; got " + config["field"]);
  return NO_ERR;
}


void
ToSummary::process_one (boost::shared_ptr<Tuple>& t) {

  
  if ( (  unsigned(t->e_size()) <= field) || !t->e(field).has_i_val()) {
    t.reset();
    return;
  }

  int ival = t->e(field).i_val();
  if ( ival >= 0) {
    JSSummary * s = t->mutable_e(field)->mutable_summary();
    s->add_items(ival);
    t->mutable_e(field)->clear_i_val();
    s->set_future_hist_size(s_size);
    /*LogHistogram l(s_size);
    l.add_item(i, 1);
    JSSummary * s = t->mutable_e(field)->mutable_summary();
    l.serialize_to(*s);
    t->mutable_e(field)->clear_i_val();*/
  }
}

operator_err_t ToSummary::configure(std::map<std::string,std::string> &config) {
  if( !(istringstream(config["field"]) >> field))
    return operator_err_t("must specify a field; got " + config["field"]);
  if( !(istringstream(config["size"]) >> s_size))
    return operator_err_t("must specify a summary size; got " + config["size"]);

  return NO_ERR;
}


void
DegradeSummary::start() {
  if (!congest_policy)
    congest_policy = boost::shared_ptr<CongestionPolicy>(new CongestionPolicy); //null policy
}

void
DegradeSummary::process_one(boost::shared_ptr<Tuple> & t) {

  cur_step += congest_policy->get_step(id(), steps.data(), steps.size(), cur_step);

  if (t->e(field).has_summary()) {
    const JSSummary& s= t->e(field).summary();
    if (s.has_histo()) {
      LogHistogram old_h(s.histo());
      LogHistogram* newH = new LogHistogram( unsigned(old_h.bucket_count() * steps[cur_step]));
//      cout << "step " << cur_step << ". old size: " << old_h.bucket_count() << ", new size: " << newH->bucket_count() << endl;
      newH->merge_in(old_h);
      t->mutable_e(field)->clear_summary();
      newH->serialize_to(  *(t->mutable_e(field)->mutable_summary()) );
    } else {
      LOG(FATAL) << " got a summary I don't know how to degrade";
    }
  } else
    LOG(FATAL) << "no summary in field " << field << " of "<< fmt(*t);
}

operator_err_t
DegradeSummary::configure(std::map<std::string,std::string> &config) {
  if( !(istringstream(config["field"]) >> field))
    return operator_err_t("must specify a field; got " + config["field"]);


  unsigned step_count = 10;
  if ((config["step_count"].length() > 0)  && !(stringstream(config["step_count"]) >> step_count)) {
      return operator_err_t("'step_count' param should be a number, but '" + config["step_count"] + "' is not.");
  }
  double min_ratio = 0.1;
  if ((config["min_ratio"].length() > 0)  && !(stringstream(config["min_ratio"]) >> min_ratio)) {
      return operator_err_t("'min_ratio' param should be a number, but '" + config["min_ratio"] + "' is not.");
  }

  double step =  (1.0 - min_ratio) / (step_count -1);
  double ratio = min_ratio;
  for (uint i = 0; i < step_count-1; ++i) {
    steps.push_back(ratio);
    ratio += step;
  }
  steps.push_back(1.0);
  cur_step = step_count -1;
//  cout << "steps:" << steps[0] << "," << steps[1] << "..." << steps[8]<< "," << steps[9] << endl;
  return NO_ERR;
}



const string QuantileOperator::my_type_name("Quantile");
const string ToSummary::my_type_name("to-summary");
const string SummaryToCount::my_type_name("to-count");
const string DegradeSummary::my_type_name("Degrade summary");


}
