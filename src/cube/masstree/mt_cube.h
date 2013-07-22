#ifndef __JetStream__mt_cube__
#define __JetStream__mt_cube__

#include "cube.h"
#include "cube_impl.h"
#include "../mysql/dimension.h"
#include "mt_aggregate.h"
#include "js_mt_shim.h"

namespace jetstream {
namespace cube {

struct AggregateBuffer {
  unsigned sz; //size of this buffer
  char data[4]; // potentially many more.
};

class MasstreeCube: // public DataCube {
    public DataCubeImpl<MysqlDimension, MasstreeAggregate>, public boost::enable_shared_from_this<MasstreeCube> {

  public:

    MasstreeCube (jetstream::CubeSchema const _schema,
               std::string _name,
               bool overwrite_if_present, const NodeConfig &conf = NodeConfig());

    virtual ~MasstreeCube();

    virtual void create();

    virtual void destroy();

    virtual void clear_contents() {
      tree.clear();
    }


    virtual void save_tuple(jetstream::Tuple const &t, bool need_new_value, bool need_old_value,
                            boost::shared_ptr<jetstream::Tuple> &new_tuple,boost::shared_ptr<jetstream::Tuple> &old_tuple);


  virtual void save_tuple_batch(const std::vector<boost::shared_ptr<jetstream::Tuple> > &tuple_store,
       const std::vector<bool> &need_new_value_store, const std::vector<bool> &need_old_value_store,
       std::vector<boost::shared_ptr<jetstream::Tuple> > &new_tuple_store, std::vector<boost::shared_ptr<jetstream::Tuple> > &old_tuple_store)  ;

  virtual void save_tuple_batch(const std::vector<boost::shared_ptr<jetstream::Tuple> > &tuple_store,
                                const std::vector<boost::shared_ptr<std::vector<unsigned int> > > &levels_store,
                                const std::vector<bool> &need_new_value_store,
                                const std::vector<bool> &need_old_value_store,
                                std::vector<boost::shared_ptr<jetstream::Tuple> > &new_tuple_store,
                                std::vector<boost::shared_ptr<jetstream::Tuple> > &old_tuple_store);


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
                                                 size_t limit = 0) const;
      

    virtual cube::CubeIterator rollup_slice_query(std::vector<unsigned int> const &levels,
        jetstream::Tuple const &min, jetstream::Tuple const &max, bool final = true,
        std::list<std::string> const &sort = std::list<std::string>(), size_t limit = 0) const;

    virtual void
    do_rollup(std::vector<unsigned int> const &levels,jetstream::Tuple const &min, jetstream::Tuple const& max) ;


    virtual jetstream::cube::CubeIterator end() const ;
    virtual size_t num_leaf_cells() const {
      return tree.elements();
    }
    
    virtual void do_process(OperatorChain * chain, boost::shared_ptr<Tuple> t, DimensionKey key, boost::shared_ptr<std::vector<unsigned int> > levels, boost::shared_ptr<cube::TupleBatch> &tupleBatcher, ProcessCallable * proc);

  private:
    JSMasstreePtr<AggregateBuffer> tree;
    inline void extend_with_dims_from(Tuple * target, const Tuple& t) const;
    inline void extend_with_aggs(Tuple * ret, AggregateBuffer * from_store) const;

};
}
}
#endif /* defined(__JetStream__mt_cube__) */
