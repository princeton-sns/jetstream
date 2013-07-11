#ifndef __JetStream__mt_cube__
#define __JetStream__mt_cube__

#include "cube.h"
#include "cube_impl.h"
//#include "../cube_iterator.h"
#include "mysql/dimension.h"
#include "mysql/aggregate.h"

namespace jetstream {
namespace cube {

class MasstreeCube: // public DataCube {
    public DataCubeImpl<MysqlDimension, MysqlAggregate>, public boost::enable_shared_from_this<MasstreeCube> {

  public:

    MasstreeCube (jetstream::CubeSchema const _schema,
               std::string _name,
               bool overwrite_if_present, const NodeConfig &conf = NodeConfig());



    virtual void create();
    virtual void destroy();
    virtual void clear_contents();


    virtual void save_tuple(jetstream::Tuple const &t, bool need_new_value, bool need_old_value,
                            boost::shared_ptr<jetstream::Tuple> &new_tuple,boost::shared_ptr<jetstream::Tuple> &old_tuple);


  virtual void save_tuple_batch(const std::vector<boost::shared_ptr<jetstream::Tuple> > &tuple_store,
       const std::vector<bool> &need_new_value_store, const std::vector<bool> &need_old_value_store,
       std::vector<boost::shared_ptr<jetstream::Tuple> > &new_tuple_store, std::vector<boost::shared_ptr<jetstream::Tuple> > &old_tuple_store)  ;

  virtual void save_tuple_batch(const std::vector<boost::shared_ptr<jetstream::Tuple> > &tuple_store,
       const std::vector<boost::shared_ptr<std::vector<unsigned int> > > &levels_store,
       const std::vector<bool> &need_new_value_store, const std::vector<bool> &need_old_value_store,
       std::vector<boost::shared_ptr<jetstream::Tuple> > &new_tuple_store, std::vector<boost::shared_ptr<jetstream::Tuple> > &old_tuple_store);


    virtual boost::shared_ptr<jetstream::Tuple>
    get_cell_value(jetstream::Tuple const &t, std::vector<unsigned int> const &levels, bool final = true) const;

    virtual cube::CubeIterator
    slice_query(jetstream::Tuple const &min, jetstream::Tuple const& max,
                bool final = true, std::list<std::string> const &sort = std::list<std::string>(),
                size_t limit = 0) const ;


    virtual cube::CubeIterator slice_and_rollup( std::vector<unsigned int> const &levels,
                                                 jetstream::Tuple const &min,
                                                 jetstream::Tuple const& max,
                                                 std::list<std::string> const &sort = std::list<std::string>(),
                                                 size_t limit = 0) ;
      

    virtual cube::CubeIterator rollup_slice_query(std::vector<unsigned int> const &levels,
        jetstream::Tuple const &min, jetstream::Tuple const &max, bool final = true,
        std::list<std::string> const &sort = std::list<std::string>(), size_t limit = 0) const;

    virtual void
    do_rollup(std::vector<unsigned int> const &levels,jetstream::Tuple const &min, jetstream::Tuple const& max) ;


    virtual jetstream::cube::CubeIterator end() const ;
    virtual size_t num_leaf_cells() const ;


};
}
}
#endif /* defined(__JetStream__mt_cube__) */
