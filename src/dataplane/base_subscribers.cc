#include <glog/logging.h>
#include <time.h>


#include "base_subscribers.h"
#include "js_utils.h"
#include "node.h"

using namespace std;
using namespace boost;
using namespace jetstream::cube;

jetstream::cube::Subscriber::Action QueueSubscriber::action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) {
  return returnAction;
}

void QueueSubscriber::insert_callback(boost::shared_ptr<jetstream::Tuple> const &update, boost::shared_ptr<jetstream::Tuple> const &new_value) {
  insert_q.push_back(new_value);
}

void QueueSubscriber::update_callback(boost::shared_ptr<jetstream::Tuple> const &update,boost::shared_ptr<jetstream::Tuple> const &new_value, boost::shared_ptr<jetstream::Tuple> const &old_value) {
  update_q.push_back(new_value);
}

jetstream::cube::Subscriber::Action UnionSubscriber::action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) {
  return SEND;
}

void UnionSubscriber::insert_callback(boost::shared_ptr<jetstream::Tuple> const &update, boost::shared_ptr<jetstream::Tuple> const &new_value) {
  emit(update);
}

void UnionSubscriber::update_callback(boost::shared_ptr<jetstream::Tuple> const &update,boost::shared_ptr<jetstream::Tuple> const &new_value, boost::shared_ptr<jetstream::Tuple> const &old_value) {
  LOG(FATAL)<<"Should never be used";  
}


namespace jetstream {

cube::Subscriber::Action
TimeBasedSubscriber::action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) {
  return NO_SEND;
}

void
TimeBasedSubscriber::insert_callback(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value) {
	; 
}

void
TimeBasedSubscriber::update_callback(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value, 
                                 boost::shared_ptr<jetstream::Tuple> const &old_value) {
  ;
}

operator_err_t
TimeBasedSubscriber::configure(std::map<std::string,std::string> &config) {
  
  if (config.find("window_size") != config.end())
    windowSizeMs = boost::lexical_cast<time_t>(config["window_size"]);
  else
    windowSizeMs = 1000;

  time_t start_ts = time(NULL); //now
  if (config.find("start_ts") != config.end())
    start_ts = boost::lexical_cast<time_t>(config["start_ts"]);
  
  string serialized_slice = config["slice_tuple"];
  min.ParseFromString(serialized_slice);
  max.CopyFrom(min);
  min.mutable_e(ts_field)->set_t_val(start_ts);
  
  //max source ts will be set later
  
  

  string name = config["cube_name"];
  if (name.length() == 0)
    return operator_err_t("Must specify option cube_name");
  
  cube = node->get_cube(name);
  if (cube == NULL) {
    return operator_err_t("No such cube " + name);
  }
  return NO_ERR;
}


void
TimeBasedSubscriber::start() {
  running = true;
  loopThread = shared_ptr<boost::thread>(new boost::thread(boost::ref(*this)));
}

void TimeBasedSubscriber::operator()() {

    int newMax = time(NULL) - (windowOffsetMs + 999) / 1000; //can be cautious here since it's just first window
    max.mutable_e(ts_field)->set_t_val(newMax);

    
	 while (running)  {
  //sleep
    boost::this_thread::sleep(boost::posix_time::milliseconds(windowSizeMs));
    time_t now = time(NULL);
    LOG(INFO) << "doing query";
    cube::CubeIterator it = cube->slice_query(min, max);
    while ( it != cube->end()) {
      emit(*it);
      it++;      
    }
    int last_query_end = max.e(ts_field).t_val();
    min.mutable_e(ts_field)->set_t_val(  last_query_end );
    newMax = now - (windowOffsetMs + 999) / 1000; //TODO could instead offset from highest-ts-seen
    max.mutable_e(ts_field)->set_t_val(newMax);
	}

}


const string TimeBasedSubscriber::my_type_name("Timer-based subscriber");



} //end namespace