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

typedef boost::function<void () > cb_void_t;
typedef boost::function<void (int32_t) > cb_int32_t;
typedef boost::function<void (boost::system::error_code) > cb_err_t;
typedef boost::function<void (boost::asio::ip::tcp::endpoint,
			      const boost::system::error_code &) > cb_endpoint_t;

inline void no_op_v() {}

}

#endif /* _JS_DEFS_H_ */
