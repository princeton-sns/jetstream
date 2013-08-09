#
# Uses traffic shaping to simulate wifi link on mobile device. Based on real traces
# collected by Rob Kiefer for Serval/Tango project. Rob walked through Princeton campus
# while streaming media from SNS server in CS department to a multi-homed mobile phone
# cable of switching between wifi and cell links.
#

from optparse import OptionParser
import sys
import time
import subprocess
#import numpy

# DELETE??
#logger = logging.getLogger('JetStream')
#logger.setLevel(logging.INFO)
#ch = logging.StreamHandler()
#ch.setLevel(logging.INFO)
#formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
#ch.setFormatter(formatter)

PORT_DEFAULT = 8540
IFACE_DEFAULT = "eth0"
INTERVAL_DEFAULT = 1

def errorExit(error):
  print "\n" + error + "\n"
  sys.exit(1)

def print_dist_stats(vals):
  # Compute CDF
  # Print out 5/95, 10/90, 1/99 percentiles, median, mode
  pass

def traffic_shape_clear(iface):
  subprocess.call("tc qdisc del dev %s root" % (iface), shell=True)

def traffic_shape(iface, port, bwidth):
  subprocess.call("tc qdisc add dev %s root handle 1:0 htb default 10" % iface, shell=True)
  subprocess.call("tc class add dev %s parent 1:0 classid 1:10 htb rate %s ceil %s prio 0" % (iface, str(bwidth) + "bps", str(bwidth) + "bps"), shell=True)
  subprocess.call("iptables -A OUTPUT -t mangle -p tcp --sport %d -j MARK --set-mark 10" % port, shell=True)
  subprocess.call("service iptables save", shell=True)
  subprocess.call("tc filter add dev %s parent 1:0 prio 0 protocol ip handle 10 fw flowid 1:10" % iface, shell=True)

def main():
  parser = OptionParser()
  parser.add_option("-f", "--file_name", dest="fname", help="name of input trace file")
  parser.add_option("-t", "--shape_interval", dest="interval", help="traffic shaping interval (seconds)", default=INTERVAL_DEFAULT)
  parser.add_option("-p", "--port", dest="port", help="port to apply traffic shaping", default=PORT_DEFAULT)
  parser.add_option("-i", "--interface", dest="iface", help="interface to apply traffic shaping", default=IFACE_DEFAULT)
  parser.add_option("-s", "--stats", dest="show_stats", help="show traffic shaping statistics", action="store_true", default=False)
  #SID: REPLACE WITH JS LOGGING?
  parser.add_option("-v", "--verbose", dest="verbose", help="verbose output", action="store_true", default=False)
  (options, args) = parser.parse_args()

  if options.fname == "":
    errorExit("Must specify a trace file.")
  interval = int(options.interval)
  port = int(options.port)

  # Clear any prior rules before and after running
  traffic_shape_clear(options.iface)

  # Read trace file and simulate link bandwidth conditions over tumbling intervals
  # File format: [timestamp flow_id iface up_bytes down_bytes wifi_signal ...]
  traceFile = open(str(options.fname), 'r')
  count = 0
  upBytes = downBytes = 0
  bwidthVals = []
  for line in traceFile:
    linkStats = line.split()
    upBytes += int(linkStats[3])
    downBytes += int(linkStats[4])
    count += 1
    # Compute bandwidth over this interval and shape traffic accordingly
    if count == interval:
      # Currently we only use down bytes converted to bps
      bwidth = int(downBytes * 8.0 / interval)
      bwidthVals.append(bwidth)
      print "Setting bandwidth to %d bps" % bwidth
      traffic_shape(options.iface, port, bwidth)
      upBytes = downBytes = 0
      count = 0
      time.sleep(interval)

  # Print statistics about the bandwidth distribution
  if options.stats:
   print "Bandwidth over %d-second intervals" % (interval)
   print_dist_stats(bwidthVals)
  
  traffic_shape_clear()


if __name__ == '__main__':
    main()

