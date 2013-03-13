#include "subscriber.h"
#include <glog/logging.h>
#include <boost/bind.hpp>
#include "querier.h"

using namespace std;
using namespace jetstream::cube;

void Subscriber::process (boost::shared_ptr<jetstream::Tuple> t) {
  LOG(FATAL)<<"Cube Subscriber should never process";
}

const string Subscriber::my_type_name("Subscriber");

void Subscriber::insert_callback(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value) {
  exec.submit(boost::bind(&Subscriber::post_insert, this, update, new_value));  
}

void Subscriber::update_callback(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value, 
                                 boost::shared_ptr<jetstream::Tuple> const &old_value) {
  exec.submit(boost::bind(&Subscriber::post_update, this, update, new_value, old_value));  
}

size_t Subscriber::queue_length() {
  return exec.outstanding_tasks();
}

void Subscriber::no_more_tuples () {
  if(cube) {
    cube->remove_subscriber(id());
  }
  DataPlaneOperator::no_more_tuples();
}

namespace jetstream {

void get_rollup_level_array(const string & as_str, std::vector<unsigned>& rollup_levels) {

  std::stringstream ss(as_str);
  std::string item;

  while(std::getline(ss, item, ',')) {
    unsigned int level = boost::lexical_cast<unsigned int>(item);
    rollup_levels.push_back(level);
  }
}

operator_err_t
Querier::configure(std::map<std::string,std::string> &config, operator_id_t _id) {
  id = _id;

  string serialized_slice = config["slice_tuple"];
  min.ParseFromString(serialized_slice);
  max.CopyFrom(min);

  num_results = 0;

  if (config.find("num_results") != config.end())
    num_results = boost::lexical_cast<int32_t>(config["num_results"]);

  if (config.find("sort_order") != config.end()) {
    std::stringstream ss(config["sort_order"]);
    std::string item;

    while(std::getline(ss, item, ',')) {
      sort_order.push_back(item);
    }
  }

  if (config.find("rollup_levels") != config.end()) {
    get_rollup_level_array(config["rollup_levels"], rollup_levels);
  }

  return NO_ERR;
}

template<typename T>
std::string fmt_list(const T& l) {
  ostringstream out;
  typename T::const_iterator i;
  out << "(";

  for( i = l.begin(); i != l.end(); ++i)
    out << *i << " ";

  out << ")";
  return out.str();
}

cube::CubeIterator Querier::do_query() {

  if (rollup_levels.size() > 0) {
    VLOG(1) << id << " doing rollup query. Range is " << fmt(min) << " to " << fmt(max) <<
            " and rollup levels are " << fmt_list(rollup_levels);

    cube->do_rollup(rollup_levels, min, max);
    VLOG(1) << "rollup done; now doing query";
    return cube->rollup_slice_query(rollup_levels, min, max, true, sort_order, num_results);
  }
  else {
    VLOG(1) << id << " doing query; range is " << fmt(min) << " to " << fmt(max);

    return cube->slice_query(min, max, true, sort_order, num_results);
  }
}

void
Querier::set_rollup_level(int fieldID, unsigned r_level) {
  if(rollup_levels.size() == 0) {
    for(int i =0; i < min.e_size(); ++i)
      rollup_levels.push_back(DataCube::LEAF_LEVEL);
  }

  assert ((unsigned int) fieldID < rollup_levels.size());

  rollup_levels[fieldID] = r_level;

  if (cube->is_unrolled(rollup_levels)) {
    rollup_levels.clear();
  }

}

void
Querier::set_rollup_levels(DataplaneMessage& m) {
  for( unsigned i = 0; i < rollup_levels.size(); ++i)
    m.add_rollup_levels(rollup_levels[i]);
}

}
