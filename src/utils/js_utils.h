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


std::string fmt(const jetstream::Tuple& t);



inline void extend_tuple(jetstream::Tuple& t, int32_t i) {
  t.add_e()->set_i_val(i);
}
inline void extend_tuple(jetstream::Tuple& t, double d) {
  t.add_e()->set_d_val(d);
}
inline void extend_tuple(jetstream::Tuple& t, const std::string& s) {
  t.add_e()->set_s_val(s);
}
inline void extend_tuple_time(jetstream::Tuple& t, int32_t time) {
  t.add_e()->set_t_val(time);
}


}

#endif /* _JS_UTILS_H_ */
