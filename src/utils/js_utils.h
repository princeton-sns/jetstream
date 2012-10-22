#ifndef _JS_UTILS_H_
#define _JS_UTILS_H_

#include <string>
#include "js_defs.h"
#include "js_boost_defs.h"
#include "jetstream_types.pb.h"

namespace jetstream {

void         jetstream_init ();
void         refresh_time ();
sec_t        get_sec ();
usec_t       get_usec ();
timespec     get_time ();
// std::string  get_strtime ();

}

std::string fmt(const jetstream::Tuple& t);

#endif /* _JS_UTILS_H_ */
