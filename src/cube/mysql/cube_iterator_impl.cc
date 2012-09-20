#include "cube_iterator_impl.h"
using namespace jetstream;

boost::shared_ptr<jetstream::cube::MysqlCubeIteratorImpl> const jetstream::cube::MysqlCubeIteratorImpl::impl_end = boost::make_shared<MysqlCubeIteratorImpl>();
