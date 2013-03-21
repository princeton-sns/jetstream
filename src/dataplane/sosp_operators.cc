#include "sosp_operators.h"


#include <glog/logging.h>


using namespace ::std;
using namespace boost;

namespace jetstream {



void
SeqToRatio::process(boost::shared_ptr<Tuple> t) {

  if ( cur_url.size() == 0) {
    cur_url = t->e(url_offset).s_val();
  }
  else if ( cur_url != t->e(url_offset).s_val()) {
    Element * ratio = targ_el->add_e();
    ratio->set_d_val( numeric(targ_el, total_field_off) / total_val);
    emit(targ_el);
    cur_url = "";
  } else {
  
    total_val += numeric(t, total_field_off);
    int response_code = t->e(respcode_offset).i_val();

    if (response_code == 404) {
      targ_el = t;  
    }
  }
  
}


operator_err_t
SeqToRatio::configure(std::map<std::string,std::string> &config) {

  if ( !(istringstream(config["total_field_off"]) >> total_field_off)) {
    return operator_err_t("must specify an int as total_field_off; got " + config["total_field_off"] +  " instead");
  }
  
  if ( !(istringstream(config["respcode_offset"]) >> respcode_offset)) {
    return operator_err_t("must specify an int as respcode_offset; got " + config["respcode_offset"] +  " instead");
  }
  
  if ( !(istringstream(config["url_offset"]) >> url_offset)) {
    return operator_err_t("must specify an int as url_offset; got " + config["url_offset"] +  " instead");
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