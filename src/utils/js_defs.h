#ifndef _JS_DEFS_H_
#define _JS_DEFS_H_

#include <stdio.h> 
#include <stdlib.h> 
#include <sys/types.h>
#include <sys/time.h>
#include <assert.h>
#include <string>
#include "js_version.h"

namespace jetstream {

typedef u_int64_t nsec_t;
typedef u_int64_t usec_t;
typedef u_int64_t msec_t;
typedef u_int32_t sec_t;
typedef u_int16_t port_t;

static const port_t  dns_port                = 53;
static const port_t  http_port               = 80;

static const u_long  year_in_sec             = 31536000;
static const u_long  week_in_sec             = 604800;
static const u_long  day_in_sec              = 86400;
static const u_long  msec_in_sec             = 1000;
static const u_long  usec_in_sec             = 1000000;
static const u_long  nsec_in_sec             = (usec_in_sec * 1000);

static const u_int32_t MAX_UINT32            = 4294967295u;
static const u_int16_t MAX_UINT16            = 65535;

// Global variables defined here
extern timespec tsnow;

uint32_t jenkins_one_at_a_time_hash(const char *key, size_t len);

};

#endif /* _JS_DEFS_H_ */
