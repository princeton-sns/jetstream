
#include "mt_cube.h"
#include "mt_iter.h"


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

}

void MasstreeCube::clear_contents() {

}


void
MasstreeCube::save_tuple(jetstream::Tuple const &t, bool need_new_value, bool need_old_value,
                            boost::shared_ptr<jetstream::Tuple> &new_tuple,boost::shared_ptr<jetstream::Tuple> &old_tuple) {
  
  }


void
MasstreeCube::save_tuple_batch(const std::vector<boost::shared_ptr<jetstream::Tuple> > &tuple_store,
       const std::vector<bool> &need_new_value_store, const std::vector<bool> &need_old_value_store,
       std::vector<boost::shared_ptr<jetstream::Tuple> > &new_tuple_store, std::vector<boost::shared_ptr<jetstream::Tuple> > &old_tuple_store) {
    
    }

void
MasstreeCube::save_tuple_batch(const std::vector<boost::shared_ptr<jetstream::Tuple> > &tuple_store,
       const std::vector<boost::shared_ptr<std::vector<unsigned int> > > &levels_store,
       const std::vector<bool> &need_new_value_store, const std::vector<bool> &need_old_value_store,
       std::vector<boost::shared_ptr<jetstream::Tuple> > &new_tuple_store, std::vector<boost::shared_ptr<jetstream::Tuple> > &old_tuple_store) {
  
}


boost::shared_ptr<jetstream::Tuple>
MasstreeCube::get_cell_value(jetstream::Tuple const &t, std::vector<unsigned int> const &levels, bool final) const {

  boost::shared_ptr<Tuple> tup;
  return tup;
}

cube::CubeIterator
MasstreeCube::slice_query(jetstream::Tuple const &min, jetstream::Tuple const& max,
                bool final, std::list<std::string> const &sort, size_t limit) const  {
  
  }


 cube::CubeIterator
MasstreeCube::slice_and_rollup( std::vector<unsigned int> const &levels,
                                                 jetstream::Tuple const &min,
                                                 jetstream::Tuple const& max,
                                                 std::list<std::string> const &sort,
                                                 size_t limit) const {
   
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
//  boost::shared_ptr<jetstream::cube::MysqlCubeIteratorImpl> impl = MysqlCubeIteratorImpl::end();
//  return CubeIterator(impl);
}

size_t
MasstreeCube::num_leaf_cells() const {
  return 0;
}


}
}