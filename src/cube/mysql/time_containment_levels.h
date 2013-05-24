
#ifndef TIME_CONT_LEVS_H
#define TIME_CONT_LEVS_H
const unsigned int  DTC_SECS_PER_LEVEL[] = {INT_MAX, 3600, 1800, 600, 300, 60, 30, 10, 5, 1};
const unsigned int DTC_LEVEL_COUNT = sizeof(DTC_SECS_PER_LEVEL)/sizeof(unsigned int);

#endif