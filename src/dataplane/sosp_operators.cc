#include "sosp_operators.h"


#include <glog/logging.h>
#include <boost/filesystem/fstream.hpp>
#include "node.h"

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
  if (boost::filesystem::exists(p) && boost::filesystem::is_regular(p)) {
    VLOG(1) << "reading from " << p;
    uintmax_t len = boost::filesystem::file_size(p);
    if (len < MAX_READ_SIZE) {
      char* data_buf = new char[len];
      boost::filesystem::ifstream in(p);
      in.read(data_buf, len);
      len = in.gcount();
      
      boost::shared_ptr<Tuple> t = boost::shared_ptr<Tuple>(new Tuple);
      extend_tuple(*t, p.string());
      Element * e = t->add_e();
      e->set_blob(data_buf, len);
      
      std::vector<boost::shared_ptr<Tuple> > buf;
      buf.push_back(t);
      chain->process(buf);
      
      delete[] data_buf;
    
    } else
      LOG(WARNING) << "Omitting " << p << "because too big";
    
  } else
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
    if (boost::filesystem::exists(f) && boost::filesystem::is_regular(f) &&
          (f.filename().string().find(prefix) == 0))
      paths.push_back(f);
    iter++;
  }
  sort(paths.begin(), paths.end());
  LOG(INFO) << "Blob reader " << id() << " will iterate over " << paths.size() << " files in " << dirname;
  if (paths.size() == 0)
      return "Can't scan an empty directory";
  
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

void
ImageQualityReporter::process_one(boost::shared_ptr<Tuple>& t) {

//  LOG(INFO) << "reporter got a tuple";
  {
    boost::unique_lock<boost::mutex> l(mutex);
    bytes_this_period += t->ByteSize();
    int latency_ms = int(get_usec()/1000 - t->e(ts_field).d_val());
    if (latency_ms > 0)
      latencies_this_period.add_item(latency_ms, 1);
    else if (latency_ms < 0)
      LOG(INFO) << "no measured latency, clocks are skewed by " << latency_ms;
  }
}

void
ImageQualityReporter::emit_stats() {

  {
    boost::unique_lock<boost::mutex> l(mutex);
    LOG(INFO) << "IMGREPORT: " << bytes_this_period << " bytes and "
        << latencies_this_period.pop_seen()  << " total images. Median latency "
        << latencies_this_period.quantile(0.5) << " and 95th percentile is "
        << latencies_this_period.quantile(0.95);
    
    latencies_this_period.clear();
    bytes_this_period = 0;
  }
  if (running) {
    timer->expires_from_now(boost::posix_time::seconds(2));
    timer->async_wait(boost::bind(&ImageQualityReporter::emit_stats, this));
  }

}

operator_err_t
ImageQualityReporter::configure(std::map<std::string,std::string> &config) {

  return NO_ERR;
}

void
ImageQualityReporter::chain_stopping(OperatorChain * ) {
  if (chains == 0) {
    LOG(INFO) << "Stopping image quality reporter";
    running = false;
    timer->cancel();
  } else
    chains --;
}

void
ImageQualityReporter::add_chain(boost::shared_ptr<OperatorChain>) {
  if (++chains == 1) { //added first chain
    running = true;
    LOG(INFO) << "reporter started";
    timer = node->get_timer();
    timer->expires_from_now(boost::posix_time::seconds(2));
    timer->async_wait(boost::bind(&ImageQualityReporter::emit_stats, this));
  }
}

const string SeqToRatio::my_type_name("Seq to Ratio");
const string BlobReader::my_type_name("Blob Reader");
const string ImageSampler::my_type_name("Image Downsampler");
const string ImageQualityReporter::my_type_name("Image quality reporter");



}
