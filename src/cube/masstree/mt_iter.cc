//
//  mt_iter.cpp
//  JetStream
//
//  Created by Ariel Rabkin on 7/11/13.
//  Copyright (c) 2013 Ariel Rabkin. All rights reserved.
//

#include "mt_iter.h"

namespace jetstream {
namespace cube {

boost::shared_ptr<jetstream::cube::MasstreeCubeIteratorImpl> const jetstream::cube::MasstreeCubeIteratorImpl::impl_end = boost::make_shared<MasstreeCubeIteratorImpl>();


boost::shared_ptr<MasstreeCubeIteratorImpl> MasstreeCubeIteratorImpl::end() {
  return MasstreeCubeIteratorImpl::impl_end;
}


void MasstreeCubeIteratorImpl::increment() {

}
  
bool MasstreeCubeIteratorImpl::equal(CubeIteratorImpl const& other) const {
  return false;
}

boost::shared_ptr<jetstream::Tuple> MasstreeCubeIteratorImpl::dereference() const {
  boost::shared_ptr<jetstream::Tuple> v;
  return v;
}

}
}