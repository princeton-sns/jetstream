#ifndef CUBE_ITERATOR_IMPL_LV674898
#define CUBE_ITERATOR_IMPL_LV674898

#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>
#include "jetstream_types.pb.h"
#include <boost/iterator/iterator_facade.hpp>

namespace jetstream {
namespace cube {


class CubeIteratorImpl
{
 public:
    CubeIteratorImpl() {}
    virtual ~CubeIteratorImpl() {}
    virtual size_t numCells() = 0;
 
    virtual void increment() = 0;
    virtual bool equal(CubeIteratorImpl const& other) const = 0 ;
    virtual boost::shared_ptr<jetstream::Tuple> dereference() const = 0;
};

}
}
#endif /* end of include guard: CUBE_ITERATOR_IMPL_LV674898 */
