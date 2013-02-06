#ifndef JetStream_timeteller_h
#define JetStream_timeteller_h

#include <time.h>
#include <boost/numeric/conversion/cast.hpp>

class TimeTeller {
  public:
    virtual time_t now() { return time(NULL); };
    virtual ~TimeTeller() {}
};

class TimeSimulator : public TimeTeller {
  // This class is used to simulate time. It is very simple, and is intended as
  // a drop-in for time(NULL) - granularity is to the nearest second, and it
  // only knows time in terms of the UNIX epoch. You can specify an epoch time to
  // start from, and a positive integer rate to speed up the "simulation".
  public:
    TimeSimulator() : real_start(time(NULL)), sim_start(time(NULL)), rate(1) {}

    TimeSimulator(time_t sim_start, int rate)
      : real_start(time(NULL)), sim_start(sim_start), rate(rate) { 
        if (VLOG_IS_ON(2)) {
          // verbose logging: print start time
          time_t t = now();
          struct tm parsed_time;
          gmtime_r(&t, &parsed_time);
          char tmbuf[80];
          strftime(tmbuf, sizeof(tmbuf), "%d-%m-%y %H:%M:%S", &parsed_time);

          VLOG(2) << "This time simulator starts at time: " << tmbuf << std::endl;
        }
      }

    virtual time_t now() {
      return sim_start +
        boost::numeric_cast<time_t>(rate * (time(NULL) - real_start));
    }

  private:
    const time_t real_start;
    const time_t sim_start;
    const int rate;
};


#endif
