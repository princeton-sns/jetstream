#include "sosp_operators.h"


#include <glog/logging.h>
#include <boost/filesystem/fstream.hpp>
#include "node.h"
#include <algorithm>
#include <numeric>


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

  std::vector<boost::shared_ptr<Tuple> > buf;
  
  for (unsigned i = 0; i < files_per_window; ++i) {
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
        
        buf.push_back(t);
        delete[] data_buf;
      
      } else
        LOG(WARNING) << "Omitting " << p << "because too big";
      
    } else
      LOG(INFO) << "omitting non-file " << p;
    cur_path++;
    if (cur_path >= paths.size())
      cur_path = 0;
  }
  
    
  GenericQCongestionMonitor * mon = dynamic_cast<GenericQCongestionMonitor*>(chain->congestion_monitor().get());
  if ( mon ) {
//    int queue_len = mon->queue_length();
//    LOG(INFO) << "Qlen: " << queue_len;
    LOG_EVERY_N(INFO, 1)  << "(every 1) "<< mon->long_description();
  }
  
  DataplaneMessage window_end;
  window_end.set_type(DataplaneMessage::END_OF_WINDOW);
  window_end.set_window_length_ms(ms_per_window);
  chain->process(buf, window_end);
        
  
  return ms_per_window;
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
  
  if ( !(istringstream(config["files_per_window"]) >> files_per_window)) {
    return operator_err_t("must specify an int as files_per_window; got " + config["files_per_window"] +  " instead");
  }
  
  ms_per_window = 1000;
  if (config.count("ms_per_window") && !(istringstream(config["ms_per_window"]) >> ms_per_window)) {
    return operator_err_t("must specify an int as ms_per_window; got " + config["ms_per_window"] +  " instead");
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

inline unsigned
ImageQualityReporter::get_chain_index(OperatorChain * c) {
  map<OperatorChain *, unsigned>::iterator index_iter = chain_indexes.find(c);
  if (index_iter == chain_indexes.end()) {
    unsigned new_offset = bytes_per_src_in_period.size();
    chain_indexes[c] = new_offset;
    bytes_per_src_in_period.push_back(0);
    bytes_per_src_total.push_back(0);
    verylate_by_chain.push_back(0);
    
    (*out_stream) << "INDEX " << new_offset << " " << c->member(0)->id_as_str() << endl;
    
    return new_offset;
  } else
    return index_iter->second;
}

static const int VERYLATE_THRESH = 5 * 1000; //in milliseconds
static const double GLOBAL_QUANT = 0.999;
static const int PERIOD_SECS = 8; //want to see "prolonged" spikes in quantiles.

void
ImageQualityReporter::process ( OperatorChain * c,
                          std::vector<boost::shared_ptr<Tuple> > & tuples,
                          DataplaneMessage& msg) {
  
  for (unsigned i =0 ; i < tuples.size(); ++i ) {
    if(tuples[i]) {
      const Tuple& t = *(tuples[i]);
    
      boost::unique_lock<boost::mutex> l(mutex);
      bytes_this_period += t.ByteSize();
      
      unsigned chain_id = get_chain_index(c);
      bytes_per_src_in_period[chain_id] +=  t.ByteSize();
      bytes_per_src_total[chain_id] +=  t.ByteSize();
      
      int latency_ms = int(get_msec() - t.e(ts_field).d_val());
      
      max_latency = max<long>(max_latency, latency_ms);
      
      if (latency_ms >= 0) {
        latencies_this_period.add_item(latency_ms, 1);
        latencies_total.add_item(latency_ms, 1);
        
        if (latency_ms > VERYLATE_THRESH) {
          verylate_by_chain[chain_id] += 1;
        }
      }
      else if (latency_ms < 0)
        LOG(INFO) << "no measured latency, clocks are skewed by " << latency_ms;
      //NOTE: Lock gets released here
    }
  }
}

inline double get_stddev(const vector<long long>& v) {
  double sum = std::accumulate(v.begin(), v.end(), 0.0);
  double mean = sum / v.size();
  std::vector<double> diff(v.size());
  std::transform(v.begin(), v.end(), diff.begin(),
                 std::bind2nd(std::minus<double>(), mean));
  double sq_sum = std::inner_product(diff.begin(), diff.end(), diff.begin(), 0.0);
  return std::sqrt(sq_sum / v.size());  
}

template <typename T>
void print_vec( ostream * out_stream , const string& label, vector<T> v) {
  (*out_stream) << label << v[0];
  for (unsigned i = 1; i < v.size(); ++i)
     (*out_stream) << " " << v[i];
  (*out_stream) << endl;
}

void
ImageQualityReporter::emit_stats() {

  if (!running)
    return;

  uint64_t bytes;
  long median, total, this_95th, global_quant, period_max;
  vector<long long> counts_by_node;
  vector<long> verylate_by_chain_copy;
  double src_stddev;
  
  {
    boost::unique_lock<boost::mutex> l(mutex);
    bytes = bytes_this_period;
    total = latencies_this_period.pop_seen();
    median =latencies_this_period.quantile(0.5);
    this_95th = latencies_this_period.quantile(0.95);
    global_quant = latencies_total.quantile(GLOBAL_QUANT);
    period_max = max_latency;
    src_stddev = get_stddev(bytes_per_src_total);
    
    latencies_this_period.clear();
    bytes_per_src_in_period.assign(bytes_per_src_in_period.size(), 0);
    bytes_this_period = 0;
    max_latency = 0;
    counts_by_node = bytes_per_src_total;
    verylate_by_chain_copy = verylate_by_chain;
  }
/*
  LOG(INFO) << "IMGREPORT: " << bytes << " bytes and "
      << total  << " total images. Median latency "
      << median << " and 95th percentile is "
      << this_95th << ". Overall " << (100*GLOBAL_QUANT) << "th percentile is " << global_quant;
*/
  msec_t ts = get_msec();
  (*out_stream) << ts << " "<< bytes << " bytes. " << total << " images. " << median
                << " (median) " << this_95th  << " (95th) " << global_quant <<
                 " (global-"<< (100*GLOBAL_QUANT) <<") " << src_stddev<< " (src_dev;global) "
                 << period_max << " (max)" << endl;
  
  if (counts_by_node.size() > 0) {
    print_vec(out_stream, "BYNODE: ", counts_by_node);

  }
  
  if (period_max > VERYLATE_THRESH) {
    print_vec(out_stream, "VERYLATE: ", verylate_by_chain_copy);
  }
  

  timer->expires_from_now(boost::posix_time::seconds(PERIOD_SECS));
  timer->async_wait(boost::bind(&ImageQualityReporter::emit_stats, this));

}

operator_err_t
ImageQualityReporter::configure(std::map<std::string,std::string> &config) {

  return NO_ERR;
}

void
ImageQualityReporter::chain_stopping(OperatorChain * ) {

  LOG(INFO) << "Stopping chain with image quality reporter; chain count is " << chains;
  if (chains == 1) {
    running = false;
    timer->cancel();
    out_stream ->flush();
    LOG(INFO) << "stopped";
  } else
    chains --;
}

void
ImageQualityReporter::add_chain(boost::shared_ptr<OperatorChain>) {
  if (++chains == 1) { //added first chain
    running = true;
    LOG(INFO) << "reporter started";
    out_stream = new std::ofstream(logging_filename.c_str());
    timer = node->get_timer();
    timer->expires_from_now(boost::posix_time::seconds(PERIOD_SECS));
    timer->async_wait(boost::bind(&ImageQualityReporter::emit_stats, this));
  }
}

const string SeqToRatio::my_type_name("Seq to Ratio");
const string BlobReader::my_type_name("Blob Reader");
const string ImageSampler::my_type_name("Image Downsampler");
const string ImageQualityReporter::my_type_name("Image quality reporter");



}
