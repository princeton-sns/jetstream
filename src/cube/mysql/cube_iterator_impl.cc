#include "cube_iterator_impl.h"

using namespace std;
using namespace jetstream;
using namespace jetstream::cube;

boost::shared_ptr<jetstream::cube::MysqlCubeIteratorImpl> const jetstream::cube::MysqlCubeIteratorImpl::impl_end = boost::make_shared<MysqlCubeIteratorImpl>();


size_t MysqlCubeIteratorImpl::numCells() {
  return num_cells;
}

void  MysqlCubeIteratorImpl::increment() {
  if(res && !res->next()) {
    boost::shared_ptr<sql::ResultSet> rs;
    res=rs;
  }
}

bool MysqlCubeIteratorImpl::equal(CubeIteratorImpl const& other) const  {
  try {
    MysqlCubeIteratorImpl const& otherm = dynamic_cast<MysqlCubeIteratorImpl const&>(other);

    if(!res) {
      return !otherm.res;
    }

    return (this->res == otherm.res && this->res->getRow() == otherm.res->getRow());

  }
  catch (const std::bad_cast& e) {
    return false;
  }
}

boost::shared_ptr<jetstream::Tuple> MysqlCubeIteratorImpl::dereference() const {
  if(!res) {
    boost::shared_ptr<jetstream::Tuple> tup;
    return tup;
  }

  return cube->make_tuple_from_result_set(res, final, rollup);
}

boost::shared_ptr<MysqlCubeIteratorImpl> MysqlCubeIteratorImpl::end() {
  return MysqlCubeIteratorImpl::impl_end;
}

