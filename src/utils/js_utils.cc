#include "js_utils.h"
#include <iostream>

#include <time.h>

#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif



using namespace jetstream;

timespec jetstream::tsnow;

void
jetstream::jetstream_init () {
  refresh_time();
}

void
jetstream::refresh_time () {
#ifdef __MACH__ 
  clock_serv_t cclock;
  mach_timespec_t mts;
  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
  clock_get_time(cclock, &mts);
  mach_port_deallocate(mach_task_self(), cclock);
  tsnow.tv_sec  = mts.tv_sec;
  tsnow.tv_nsec = mts.tv_nsec;
#else
  clock_gettime (CLOCK_REALTIME, &tsnow);
#endif
}

sec_t
jetstream::get_sec () {
  return tsnow.tv_sec;
}

usec_t
jetstream::get_usec () {
  refresh_time();
  return ((tsnow.tv_sec * (usec_t) 1000000) + (tsnow.tv_nsec / 1000));
}

timespec
jetstream::get_time () {
  refresh_time();
  return tsnow;
}

/*
std::string
jetstream::get_strtime () {
  timespec ts = get_time();
  string s2 = str( format("%2% %1%") % 36 % 77 );
  str buf = strbuf("%d.%06d", int (ts.tv_sec), int (ts.tv_nsec / 1000));
  return buf;
}
*/

namespace jetstream {

inline void add_one_el(std::ostringstream& buf, const Element& el) {
  if (el.has_s_val())
    buf << el.s_val();
  else if (el.has_d_val())
    buf << el.d_val();
  else if (el.has_i_val())
    buf << el.i_val();
  else if (el.has_t_val()) {
    time_t t = (time_t)el.t_val();
    struct tm parsed_time;
    gmtime_r(&t, &parsed_time);
    
    char tmbuf[80];
    strftime(tmbuf, sizeof(tmbuf), "%H:%M:%S", &parsed_time);
    buf << tmbuf;
  } else
    buf << "UNDEF";
}

std::string fmt(const jetstream::Tuple& t) {
  std::ostringstream buf;
  buf << "(";
  if (t.e_size() > 0) {
    add_one_el(buf, t.e(0));
  }
  for (int i =1; i < t.e_size(); ++i) {
    buf << ",";
    add_one_el(buf, t.e(i));
  }
  buf<< ")";
  return buf.str();
}

}
