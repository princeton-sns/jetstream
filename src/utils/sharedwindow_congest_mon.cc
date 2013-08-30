

#include "sharedwindow_congest_mon.h"

using namespace ::std;
namespace jetstream {

static SharedWindowMonitor default_shared_monitor;


WindowMonFacade::WindowMonFacade(const std::string& name, double smoothing_coeff):
  WindowCongestionMonitor(name, smoothing_coeff) {
  underlying = &default_shared_monitor;

}


double
WindowMonFacade::capacity_ratio() {

  double my_ratio = WindowCongestionMonitor::capacity_ratio();
  return underlying->update_from_mon(this, my_ratio);
}
  
//void WindowMonFacade::end_of_window(int window_ms, msec_t start_time) {}


const static double TARGET_QUANT = 0.10; //NOTE: list is sorted
  //least to greatest, so lower TARGET_QUANT means more cautious reporting.

double
SharedWindowMonitor::update_from_mon(WindowMonFacade * src, double congest) {
  boost::unique_lock<boost::mutex> lock(internals);
  msec_t now = get_msec();
  measurements_in_period.push_back(congest);
  if ( now - last_period_end >= 3000 ) {
    last_period_end = now;
    std::sort (measurements_in_period.begin(), measurements_in_period.end());
    ratio_this_period = measurements_in_period[ TARGET_QUANT * measurements_in_period.size()];
    LOG(INFO) << "SharedCongest: " << measurements_in_period.size()
      << " measurements, " << TARGET_QUANT<< "-quant was " <<  ratio_this_period;
    measurements_in_period.clear();
  }

  return ratio_this_period;
}

} //end namespace