#ifndef _JS_CONST_H_
#define _JS_CONST_H_

#include <stdio.h> 
#include <stdlib.h> 
#include <sys/types.h>
#include <sys/time.h>
#include <assert.h>

typedef u_int64_t nsec_t;
typedef u_int64_t usec_t;
typedef u_int32_t msec_t;
typedef u_int32_t sec_t;

static const u_int   dns_port                = 53;
static const u_int   http_port               = 80;

static const u_long  year_in_sec             = 31536000;
static const u_long  week_in_sec             = 604800;
static const u_long  day_in_sec              = 86400;
static const u_long  msec_in_sec             = 1000;
static const u_long  usec_in_sec             = 1000000;
static const u_long  nsec_in_sec             = (usec_in_sec * 1000);

// Global variables defined here
extern timespec tsnow;

#endif /* _JS_CONST_H_ */
