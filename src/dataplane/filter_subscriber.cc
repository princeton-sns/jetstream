
#include "filter_subscriber.h"
#include "cube.h"

using namespace jetstream;
using namespace ::std;


jetstream::cube::Subscriber::Action
FilterSubscriber::action_on_tuple(OperatorChain * c, boost::shared_ptr<const jetstream::Tuple> const update) {
  return SEND;
}

void
FilterSubscriber::post_insert ( boost::shared_ptr<jetstream::Tuple> const &update,
                                boost::shared_ptr<jetstream::Tuple> const &new_value) {
  //update is the tuple that just came in, which is what we pass through.
    //new val is the thing being flushed, which we don't.

  if (cube_field > 0) {
    int filtered_val = 0;
    if (update->e(cube_field).has_i_val())
      filtered_val = update->e(cube_field).i_val();
    else if (update->e(cube_field).has_d_val())
      filtered_val = update->e(cube_field).d_val();
    else {
      LOG(FATAL) << "Subscriber " << id() << " on cube " << cube->id_as_str()
       << " expected field " <<  cube_field << " to be an int or double."
       << " Tuple was " << fmt(*update);
    }
    if (filtered_val >= filter_bound) {
      boost::shared_ptr<jetstream::Tuple> new_update(new Tuple);
      new_update->CopyFrom(*update);
      std::vector< shared_ptr<Tuple> > v;
      v.push_back(new_update);
      chain->process(v);
    }
  } else {
    boost::shared_ptr<jetstream::Tuple> new_update(new Tuple);
    new_update->CopyFrom(*update);
    std::vector< shared_ptr<Tuple> > v;
    v.push_back(new_update);
    chain->process(v);
  }
}

void
FilterSubscriber::post_update(  boost::shared_ptr<jetstream::Tuple> const &update,
                                boost::shared_ptr<jetstream::Tuple> const &new_value,
                                boost::shared_ptr<jetstream::Tuple> const &old_value) {
  LOG(FATAL)<<"Should never be used";
}



operator_err_t
FilterSubscriber::configure(std::map<std::string,std::string> &config) {

  if (config.find("cube_field") != config.end()) {
    if ( !(istringstream(config["cube_field"]) >> cube_field)) {
      return operator_err_t("must specify an int as cube_field; got " + config["cube_field"] +  " instead");
    }
    
    if ( !(istringstream(config["level_in_field"]) >> level_in_field)) {
      return operator_err_t("must specify an int as level_in_field; got " + config["level_in_field"] +  " instead");
    }
  }

  return NO_ERR;
}


void
FilterSubscriber::process( OperatorChain * chain,
                           std::vector<boost::shared_ptr<Tuple> > & tuples,
                           DataplaneMessage& m) {
  for (unsigned i = 0; i < tuples.size(); ++i) {
    boost::shared_ptr<Tuple> t = tuples[i];

    if (t->e(level_in_field).has_i_val())
      filter_bound = t->e(level_in_field).i_val();
    else if (t->e(level_in_field).has_d_val())
      filter_bound = t->e(level_in_field).d_val();
    else {
      LOG(FATAL) << "expected field " <<  level_in_field << " to be an int or double";
    }
  }
}