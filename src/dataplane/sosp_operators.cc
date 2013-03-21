#include "sosp_operators.h"


#include <glog/logging.h>


using namespace ::std;
using namespace boost;

namespace jetstream {



void
SeqToRatio::process(boost::shared_ptr<Tuple> t) {

  if ( cur_url.size() == 0) {
    cur_url = t->e(url_field).s_val();
  }
  else if ( cur_url != t->e(url_field).s_val()) {
    if (targ_el) {
      Element * ratio = targ_el->add_e();
      ratio->set_d_val( numeric(targ_el, total_field) / total_val);
      emit(targ_el);
    }
    // else shouldn't happen; there should have been at least match
    cur_url = "";
    total_val = 0;
  }  

  total_val += numeric(t, total_field);
  int response_code = t->e(respcode_field).i_val();

  if (response_code == 404 || !targ_el) {
    targ_el = t;  
  }  
}


operator_err_t
SeqToRatio::configure(std::map<std::string,std::string> &config) {

  if ( !(istringstream(config["total_field"]) >> total_field)) {
    return operator_err_t("must specify an int as total_field; got " + config["total_field"] +  " instead");
  }
  
  if ( !(istringstream(config["respcode_field"]) >> respcode_field)) {
    return operator_err_t("must specify an int as respcode_field; got " + config["respcode_field"] +  " instead");
  }
  
  if ( !(istringstream(config["url_field"]) >> url_field)) {
    return operator_err_t("must specify an int as url_field; got " + config["url_field"] +  " instead");
  }  

  return NO_ERR;
}


/*
void
SeqToRatio::meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred) {
  if ( msg.type() == DataplaneMessage::END_OF_WINDOW) {
//    boost::lock_guard<boost::mutex> lock (mutex);
    window_for[pred] = msg.window_length_ms();
  }
  DataPlaneOperator::meta_from_upstream(msg, pred); //delegate to base class
}
*/


const string SeqToRatio::my_type_name("Seq to Ratio");


}