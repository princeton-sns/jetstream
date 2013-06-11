
#ifndef TIME_CONT_LEVS_H
#define TIME_CONT_LEVS_H
const unsigned int  DTC_SECS_PER_LEVEL[] = {INT_MAX, 3600, 1800, 600, 300, 60, 30, 10, 5, 1};
    //level 0 is fully rolled up
    //level 5 is by-minute
    //level 8 is every-five-secs
    //level 9 is every-second
const unsigned int DTC_LEVEL_COUNT = sizeof(DTC_SECS_PER_LEVEL)/sizeof(unsigned int);

#endif