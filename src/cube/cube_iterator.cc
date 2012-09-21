#include "cube_iterator.h"

using namespace std;
using namespace jetstream::cube;

size_t CubeIterator::numCells() {
  return impl->numCells();
}

void CubeIterator::increment() {
  return impl->increment();
}

bool CubeIterator::equal(CubeIterator const& other) const {
  return impl->equal(*(other.impl));
}

boost::shared_ptr<jetstream::Tuple>
CubeIterator::dereference() const {
  return impl->dereference();
};
