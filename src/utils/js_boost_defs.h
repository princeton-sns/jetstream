#ifndef _JS_BOOST_DEFS_H_
#define _JS_BOOST_DEFS_H_

#include <stdio.h> 
#include <stdlib.h> 
#include <sys/types.h>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include "js_defs.h"

namespace jetstream {

typedef boost::function<void (int32_t) > bfunc_void_int32;
typedef boost::function<void (boost::system::error_code) > bfunc_void_err;
typedef boost::function<void (boost::asio::ip::tcp::endpoint,
			      const boost::system::error_code &) > bfunc_void_endpoint;

}

#endif /* _JS_DEFS_H_ */
