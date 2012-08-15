#ifndef _JS_UTILS_H_
#define _JS_UTILS_H_

#include "js_defs.h"
#include "js_boost_defs.h"

namespace jetstream {

void         jetstream_init ();
void         refresh_time ();
sec_t        get_sec ();
usec_t       get_usec ();
timespec     get_time ();
// std::string  get_strtime ();

}

#endif /* _JS_UTILS_H_ */
