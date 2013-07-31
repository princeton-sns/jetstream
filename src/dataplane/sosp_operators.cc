#include "sosp_operators.h"


#include <glog/logging.h>


using namespace ::std;
using namespace boost;

namespace jetstream {



void
SeqToRatio::process_one(boost::shared_ptr<Tuple>& t) {

  bool emitted = false;
  if ( cur_url.size() == 0) {  //no URLs seen before
    cur_url = t->e(url_field).s_val();
  } 
  else if ( cur_url != t->e(url_field).s_val()) {   //saw a change in URL
    if (targ_el) {     
      Element * ratio = targ_el->add_e();
      ratio->set_d_val(jetstream::numeric(*targ_el, total_field) / total_val);
      t = targ_el; //emit
      emitted = true;
    }
    // else shouldn't happen; there should have been at least match
    cur_url = "";
    total_val = 0;
  }  

  total_val += jetstream::numeric(*t, total_field);
  int response_code = t->e(respcode_field).i_val();

  if (response_code == 200 || !targ_el) {
    targ_el = t;
  }
  if (!emitted)
    t.reset();
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


  
int
BlobReader::emit_data()  {

  boost::filesystem::path& p = paths[cur_path];
  if (boost::filesystem::exists(p) && boost::filesystem::is_regular(p))
    LOG(INFO) << "reading from " << p;
  else
    LOG(INFO) << "omitting non-file " << p;
    
  cur_path++;
  if (cur_path >= paths.size())
    cur_path = 0;
  return ms_per_file;
}

operator_err_t
BlobReader::configure(std::map<std::string,std::string> &config) {
  if (!config.count("dirname")) {
    return operator_err_t("must specify a string as dirname; got " + config["dirname"] +  " instead");
  } else
    dirname = config["dirname"];
  
  prefix = config["prefix"];
  
  boost::filesystem::path p(dirname);
  if (!boost::filesystem::exists(p))
    return operator_err_t("No such directory " + p.string());
  if (!boost::filesystem::is_directory(p))
    return operator_err_t(p.string() + " is not a directory");

  boost::filesystem::directory_iterator iter(p);
  while(iter != directory_iterator()) {
    boost::filesystem::path f = *iter;
    if (f.filename().string().find(prefix) == 0)
      paths.push_back(f);
    iter++;
  }
  sort(paths.begin(), paths.end());
  LOG(INFO) << "Blob reader " << id() << " will iterate over " << paths.size() << " files in " << dirname;
  
  if ( !(istringstream(config["ms_per_file"]) >> ms_per_file)) {
    return operator_err_t("must specify an int as ms_per_file; got " + config["ms_per_file"] +  " instead");
  }

  return NO_ERR;
}


void
ImageSampler::process_one(boost::shared_ptr<Tuple>& t) {

}

operator_err_t
ImageSampler::configure(std::map<std::string,std::string> &config) {

  return NO_ERR;
}


const string SeqToRatio::my_type_name("Seq to Ratio");
const string BlobReader::my_type_name("Blob Reader");
const string ImageSampler::my_type_name("Image Downsampler");




}
