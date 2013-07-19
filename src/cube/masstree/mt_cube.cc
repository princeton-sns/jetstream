
#include "mt_cube.h"
#include "mt_iter.h"
#include <glog/logging.h>
#include <iostream>

using namespace ::std;

namespace jetstream {
namespace cube {


MasstreeCube::MasstreeCube (jetstream::CubeSchema const _schema,
               string _name,
               bool overwrite_if_present, const NodeConfig &conf)
      : DataCubeImpl(_schema, _name, conf){
  
  
  }


void
MasstreeCube::create() {

}


void MasstreeCube::destroy() {
  tree.clear();
}


inline void
MasstreeCube::extend_with_dims_from(Tuple * target, const Tuple& t) const {
  for (int i = 0; i < dimensions.size(); ++i) {
    Element * e = target->add_e();
    vector<size_t> idx = dimension_offset( dimensions[i]->get_name() );
    e->CopyFrom(t.e(  idx[0] ));
  }
}

inline void
MasstreeCube::extend_with_aggs(Tuple * target, AggregateBuffer * from_store) const {
  char * buf = from_store->data;
  char * buf_end = from_store->data + from_store->sz;
  for (unsigned i = 0; i < num_aggregates(); ++i) {
    buf += aggregates[i]->add_to_tuple(buf, target);
    LOG_IF(FATAL, buf >= buf_end) << "buffer overrun in MT aggregate processing, on agggregate " <<
        i<< "(" << aggregates[i]->get_name() <<")";
  }
}

void
MasstreeCube::save_tuple(jetstream::Tuple const &t, bool need_new_value, bool need_old_value,
                            boost::shared_ptr<jetstream::Tuple> &new_tuple,boost::shared_ptr<jetstream::Tuple> &old_tuple) {
  vector<unsigned int> levels;
  for(size_t i=0; i<dimensions.size(); i++) {
    levels.push_back(dimensions[i]->leaf_level());
  }

  DimensionKey k = get_dimension_key(t, levels);
  AggregateBuffer * aggs = tree.get(k.c_str());
  
  if (!aggs) {
    unsigned sz = 0;
    for (unsigned i = 0; i < num_aggregates(); ++i)
      sz += aggregates[i]->size(t);
    size_t alloc_sz = sizeof(AggregateBuffer) + sz -4;
    aggs =  (AggregateBuffer *) malloc(alloc_sz); //already reserved size four

      //FIXME: should do something aggregate-specific here
    memset(aggs->data, 0, sz);
    aggs->sz = alloc_sz;
    tree.set(k.c_str(), aggs);
  }
  
  if (need_old_value) {
    old_tuple = boost::shared_ptr<Tuple>(new Tuple);
    extend_with_dims_from(old_tuple.get(), t);
    extend_with_aggs(old_tuple.get(), aggs);
  }
  
  char * buf = aggs->data;
  char * buf_end = aggs->data + aggs->sz;
  for (unsigned i = 0; i < num_aggregates(); ++i) {
    buf += aggregates[i]->merge(buf, t);
    LOG_IF(FATAL, buf >= buf_end) << "buffer overrun in MT aggregate processing, on agggregate " <<
        i<< "(" << aggregates[i]->get_name() <<")";
  }
  cout << "At store, aggs->data[0] is " << ((int*) aggs)[1] << endl;
  if ( need_new_value) {
    new_tuple = boost::shared_ptr<Tuple>(new Tuple);
    extend_with_dims_from(new_tuple.get(), t);
    extend_with_aggs(new_tuple.get(), aggs);
  }
}


void
MasstreeCube::save_tuple_batch(const std::vector<boost::shared_ptr<jetstream::Tuple> > &tuple_store,
       const std::vector<bool> &need_new_value_store, const std::vector<bool> &need_old_value_store,
       std::vector<boost::shared_ptr<jetstream::Tuple> > &new_tuple_store, std::vector<boost::shared_ptr<jetstream::Tuple> > &old_tuple_store) {
    for (unsigned i = tuple_store.size(); i < tuple_store.size(); ++i) {
        save_tuple( *(tuple_store[i]), need_new_value_store[i], need_old_value_store[i],
          new_tuple_store[i], old_tuple_store[i]);    
    }
}


void
MasstreeCube::save_tuple_batch(const std::vector<boost::shared_ptr<jetstream::Tuple> > &tuple_store,
       const std::vector<boost::shared_ptr<std::vector<unsigned int> > > &levels_store,
       const std::vector<bool> &need_new_value_store, const std::vector<bool> &need_old_value_store,
       std::vector<boost::shared_ptr<jetstream::Tuple> > &new_tuple_store, std::vector<boost::shared_ptr<jetstream::Tuple> > &old_tuple_store) {
  
}


boost::shared_ptr<jetstream::Tuple>
MasstreeCube::get_cell_value(jetstream::Tuple const &t, std::vector<unsigned int> const &levels, bool final) const {

  DimensionKey k = get_dimension_key(t, levels);
  cout << "lookup key is " << k <<endl;
  AggregateBuffer * from_store = tree.get(k.c_str());

  if (from_store) {
    boost::shared_ptr<Tuple> ret(new Tuple);
    extend_with_dims_from(ret.get(), t);
    extend_with_aggs(ret.get(), from_store);
    return ret;
  } else {
    boost::shared_ptr<Tuple> ret;
    return ret;
  }
}

cube::CubeIterator
MasstreeCube::slice_query(jetstream::Tuple const &min, jetstream::Tuple const& max,
                bool final, std::list<std::string> const &sort, size_t limit) const  {
  boost::shared_ptr<MasstreeCubeIteratorImpl> iter;
  return CubeIterator(iter);
}


 cube::CubeIterator
MasstreeCube::slice_and_rollup( std::vector<unsigned int> const &levels,
                                                 jetstream::Tuple const &min,
                                                 jetstream::Tuple const& max,
                                                 std::list<std::string> const &sort,
                                                 size_t limit) const {
  boost::shared_ptr<MasstreeCubeIteratorImpl> iter;
  return CubeIterator(iter);  
}
      

cube::CubeIterator
MasstreeCube::rollup_slice_query(std::vector<unsigned int> const &levels,
        jetstream::Tuple const &min, jetstream::Tuple const &max, bool final,
        std::list<std::string> const &sort, size_t limit) const {
 
    return slice_and_rollup(levels, min, max, sort, limit);
}

void
MasstreeCube::do_rollup(std::vector<unsigned int> const &levels,jetstream::Tuple const &min, jetstream::Tuple const& max) {
  //deliberate no-op
}


jetstream::cube::CubeIterator
MasstreeCube::end() const {
  boost::shared_ptr<jetstream::cube::MasstreeCubeIteratorImpl> impl = MasstreeCubeIteratorImpl::end();
  return CubeIterator(impl);
}


}
}