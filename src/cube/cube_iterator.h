#ifndef CUBE_ITERATOR_LV674898
#define CUBE_ITERATOR_LV674898

#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>
#include "jetstream_types.pb.h"
#include "cube_iterator_impl.h"
#include <boost/iterator/iterator_facade.hpp>

namespace jetstream {
namespace cube {


class CubeIterator
  : public boost::iterator_facade<
  CubeIterator
  , jetstream::Tuple
  , boost::forward_traversal_tag
  , boost::shared_ptr<jetstream::Tuple>
    > {
  public:
    CubeIterator(boost::shared_ptr<CubeIteratorImpl> impl): impl(impl) {}

    size_t numCells();

  protected:
    friend class boost::iterator_core_access;

    void increment();
    bool equal(CubeIterator const& other) const ;
    boost::shared_ptr<jetstream::Tuple> dereference() const;
    boost::shared_ptr<CubeIteratorImpl> impl;
};

}
}
#endif /* end of include guard: CUBE_ITERATOR_LV674898 */
