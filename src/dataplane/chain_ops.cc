#include "chain_ops.h"
#include <algorithm>
#include "node.h"
#include "base_operators.h"

using namespace ::std;
using namespace boost;

namespace jetstream {



void
TimerSource::process(OperatorChain * chain, std::vector<boost::shared_ptr<Tuple> > & d, DataplaneMessage&) {
  return;
}


void
TimerSource::start() {
  LOG(INFO) << "time source starting";
  running = true;
  timer = node->get_timer();
  st = node->get_new_strand();
  chain->strand = st.get();
  timer->expires_from_now(boost::posix_time::seconds(0));
  timer->async_wait(st->wrap(boost::bind(&TimerSource::emit_wrapper, this)));
}

void
TimerSource::emit_wrapper() {
  LOG(INFO) << "time source emit wrapper; running is " << running;
  if (running) {
    bool stop = emit_data();
    if (!stop) {
      timer->expires_from_now(boost::posix_time::seconds(1));
      timer->async_wait(st->wrap(boost::bind(&TimerSource::emit_wrapper, this)));
    } else {
      LOG(INFO)<< "EOF; should tear down";
    
    }
  }
}


void
TimerSource::stop() {
  bool was_running = running;
  running = false;
  if (was_running) {
    timer->cancel();
  }
}


const int LINES_PER_EMIT = 20;

bool
CFileRead::emit_data() {

  if (!in_file.is_open()) {
    in_file.open (f_name.c_str());
    if (in_file.fail()) {
      LOG(WARNING) << "could not open file " << f_name.c_str() << endl;
      return true; //stop
    }
  }
  
  vector<shared_ptr<Tuple> > tuples;
  tuples.reserve(LINES_PER_EMIT);
  DataplaneMessage no_meta;
//  LOG(INFO) << "starting loop, " << tuples.size() << " tuples";
  
  for (int i = 0; i < LINES_PER_EMIT; ++i) {
    // ios::good checks for failures in addition to eof
    if (!in_file.good()) {
      cout << "hit eof, stopping" << endl;
      break;
    }
    string line;

    getline(in_file, line);
    if (skip_empty && line.length() == 0) {
      continue;
    }
    shared_ptr<Tuple> t( new Tuple);
    Element * e = t->add_e();
    e->set_s_val(line);
    t->set_version(lineno++);
    tuples.push_back(t);
  }


  LOG(INFO) << "Calling chain::process, " << tuples.size() << " tuples";
  chain->process(tuples, no_meta);
  LOG(INFO) << "Returned from chain::process";
  
  return !in_file.good();
}


std::string
CFileRead::long_description() {
  std::ostringstream buf;
  buf << "reading" << f_name;
  return buf.str();
}


operator_err_t
CFileRead::configure(map<string,string> &config) {
  f_name = config["file"];
  if (f_name.length() == 0) {
    LOG(WARNING) << "no file to read, bailing" << endl;
    return operator_err_t("option 'file' not specified");
  }

  boost::algorithm::to_lower(config["skip_empty"]);
  // TODO which values of config["skip_empty"] convert to which boolean
  // values?
  istringstream(config["skip_empty"]) >> std::boolalpha >> skip_empty;

  return C_NO_ERR;
}

void
CDummyReceiver::process( OperatorChain * chain,
                         std::vector< boost::shared_ptr<Tuple> > & in_t,
                         DataplaneMessage&) {
  if(store) {
    size_t cur_sz = tuples.size();
    tuples.reserve(cur_sz + in_t.size());
    for (int i = 0; i < in_t.size(); ++i)
      tuples.push_back(in_t[i]);
  }
}




void
CExtendOperator::mutate_tuple (Tuple& t) {
  for (u_int i = 0; i < new_data.size(); ++i) {
    Element * e = t.add_e();
    e->CopyFrom(new_data[i]);
  }
}

/*
void
CExtendOperator::process_delta (Tuple& oldV, boost::shared_ptr<Tuple> newV, const operator_id_t pred) {
  mutate_tuple(oldV);
  mutate_tuple(*newV);
  emit(oldV, newV);
} */


operator_err_t
CExtendOperator::configure (std::map<std::string,std::string> &config) {

  string field_types = boost::to_upper_copy(config["types"]);
  static boost::regex re("[SDI]+");

  if (!regex_match(field_types, re)) {
    LOG(WARNING) << "Invalid types for regex fields; got " << field_types;
    return operator_err_t("Invalid types for regex fields; got " + field_types);
    //should return failure here?
  }

  string first_key = "0";
  string last_key = ":";
  map<string, string>::iterator it = config.find(first_key);
  map<string, string>::iterator end = config.upper_bound(last_key);

  u_int i;
  for (i = 0;  i < field_types.size() && it != end; ++i, ++it) {
    string s = it->second;
    Element e;
    if (s == "${HOSTNAME}") {
      assert(field_types[i] == 'S');
      e.set_s_val( boost::asio::ip::host_name());
    }
    else {
      parse_with_types(&e, s, field_types[i]);
    }
    new_data.push_back(e);
  }
  if (i < field_types.size()) {
    LOG(WARNING) << "too many type specifiers for operator";
    return operator_err_t("too many type specifiers for operator");
  }
  if ( it != end ) {
    LOG(WARNING) << "not enough type specifiers for operator";
    return operator_err_t("not enough type specifiers for operator");
  }
  return NO_ERR;
}




const string CFileRead::my_type_name("CFileRead operator");
const string CDummyReceiver::my_type_name("CDummyReceiver operator");
const string CExtendOperator::my_type_name("Extend operator");


}

