#
# Uses traffic shaping to simulate wifi link on mobile device. Based on real traces
# collected by Rob Kiefer for Serval/Tango project. Rob walked through Princeton campus
# while streaming media from SNS server in CS department to a multi-homed mobile phone
# cable of switching between wifi and cell links.
#

from optparse import OptionParser
import sys
import time
import signal
import subprocess
import random
import uuid
import math

PORT_DEFAULT = 3451
IFACE_DEFAULT = "eth0"
INTERVAL_DEFAULT = 10
TIME_DEFAULT = -1
MIN_BWIDTH_DEFAULT = 1000

def errorExit(error):
  print "\n" + error + "\n"
  sys.exit(1)

USE_SCIPY = False

if USE_SCIPY:
  import scipy as sp
  from scipy import stats

  def print_dist_stats(vals):
    vals.sort()
    n, minMax, mean, var, skew, kurt = stats.describe(vals)
    print "Number of intervals: %d" % n
    print "Average: %0.3f" % mean
    print "Minimum: %f, Maximum: %0.3f" % (minMax[0], minMax[1])
    print "Standard deviation: %0.3f" % math.sqrt(var)
  
    # Print out some percentiles
    perc = [1, 5, 10, 50, 90, 95, 99]
    percScores = ["%0.3f" % stats.scoreatpercentile(vals, p) for p in perc]
    print "Percentiles " + str(perc) + ": " + str(percScores)

    # Other stats?
else:
  def print_dist_stats(vals):
    vals.sort()
    n, min, max, mean =  len(vals), vals[0], vals[-1], float(sum(vals))/len(vals)
    print "Number of intervals: %d" % n
    print "Average: %0.3f" % mean
    print "Minimum: %f, Maximum: %0.3f" % (min, max)
    perc = [1, 5, 10, 50, 90, 95, 99]
    percScores = ["%0.3f" % vals[ int(n * p /100.0)] for p in perc]
    print "Percentiles " + str(perc) + ": " + str(percScores)
    

def traffic_shape_start(iface, port):
  print "Starting traffic shaping rules on interface %s, port %d..." % (iface, port)
  # The default htb class is 0, which causes unclassified traffic to be dequeued at hardware speed
  subprocess.call("tc qdisc add dev %s root handle 1:0 htb" % iface, shell=True, stderr=subprocess.STDOUT)
  subprocess.call("iptables -A OUTPUT -t mangle -p tcp --dport %d -j MARK --set-mark 10" % port, shell=True, stderr=subprocess.STDOUT)
  #subprocess.call("service iptables save", shell=True, stderr=subprocess.STDOUT)
  subprocess.call("tc filter add dev %s parent 1:0 prio 0 protocol ip handle 10 fw flowid 1:10" % iface, shell=True, stderr=subprocess.STDOUT)

  #SID: NEED BETTER ERROR HANDLING
  #except subprocess.CalledProcessError as e:
  #  errorExit("Error: The following command failed: " + e.cmd + ";\n" + "output was: " + e.output)


def exit_gracefully(signo, frame):
  traffic_shape_clear(IFACE)
  sys.exit(0)

def traffic_shape_clear(iface):
  print "%s: Clearing prior traffic shaping rules on interface %s..." % (time.ctime(), iface)
  subprocess.call("tc qdisc del dev %s root" % (iface), shell=True, stderr=subprocess.STDOUT)
  subprocess.call("iptables -t mangle -F", shell=True, stderr=subprocess.STDOUT)
  subprocess.call("iptables -t mangle -X", shell=True, stderr=subprocess.STDOUT)
  
def traffic_shape(iface, bwidth):
  # Rate must be non-zero
  bwidth = max(bwidth, 1)
  cmd = "tc class replace dev %s parent 1:0 classid 1:10 htb rate %s ceil %s prio 0" % (iface, str(bwidth) + "bps", str(bwidth) + "bps")
  subprocess.call(cmd, shell=True, stderr=subprocess.STDOUT)

  #except subprocess.CalledProcessError as e:
  #  errorExit("Error: The following command failed: " + e.cmd + ";\n" + "output was: " + e.output)


def main():
  global IFACE
  parser = OptionParser()
  parser.add_option("-f", "--file_name", dest="fname", help="input trace file of bandwidth allocations", default="")
  parser.add_option("-i", "--interval", dest="interval", help="traffic shaping interval (sec) [default = %d]" % (INTERVAL_DEFAULT), default=INTERVAL_DEFAULT)
  parser.add_option("-t", "--time", dest="time", help="simulation time (sec, -1=infinite) [default = %d]" % (TIME_DEFAULT), default=TIME_DEFAULT)
  parser.add_option("-p", "--port", dest="port", help="remote port to apply traffic shaping [default = %d]" % (PORT_DEFAULT), default=PORT_DEFAULT)
  parser.add_option("-e", "--interface", dest="iface", help="interface to apply traffic shaping [default = %s]" % (IFACE_DEFAULT), default=IFACE_DEFAULT)
  parser.add_option("-b", "--fixed_bwidth", dest="fbwidth", help="fixed bandwidth allocation (Kbytes/sec)", default=0)
  parser.add_option("-m", "--min_bwidth", dest="mbwidth", help="minimum bandwidth allocation (Kbytes/sec) [default = %d]" % (MIN_BWIDTH_DEFAULT), default=MIN_BWIDTH_DEFAULT)
  parser.add_option("-x", "--clear", dest="clear", help="clear any prior rules", action="store_true", default=False)
  parser.add_option("-s", "--stats", dest="stats", help="show traffic shaping statistics", action="store_true", default=False)
  parser.add_option( "--scale", dest="scale", help="multiply values by x", default=1.0)

  parser.add_option("-v", "--verbose", dest="verbose", help="verbose output", action="store_true", default=False)
  (options, args) = parser.parse_args()

  if options.clear:
    traffic_shape_clear(options.iface)
    return

  print "%s: Script starting." % time.ctime()

  IFACE = options.iface  #used by signal handler
  signal.signal(signal.SIGINT, exit_gracefully)

  fbwidth = int(options.fbwidth)
  if (options.fname == "") and (fbwidth == 0):
    print "No options specified, so clearing traffic shaping"
    traffic_shape_clear(options.iface)  
    sys.exit(0)
#    errorExit("Error: Must specify a trace file or a fixed bandwidth.")
  interval = int(options.interval)
  simTime = int(options.time)
  port = int(options.port)
  mbwidth = int(options.mbwidth)

  random.seed(uuid.uuid4())

  bwidthVals = []
  if options.fname != "":
    # Read trace file and calculate link bandwidth over tumbling intervals
    # File format: [timestamp flow_id iface up_bytes down_bytes wifi_signal ...]
    traceFile = open(str(options.fname), 'r')
    upBytes = downBytes = 0
    count = 0
    for line in traceFile:
      linkStats = line.split()
      upBytes += int(linkStats[3])
      downBytes += int(linkStats[4])
      count += 1
      # Compute bandwidth over this interval and shape traffic accordingly
      if count == interval:
        # Currently we use only down bytes in bytes/sec
        bwidthVals.append(int(downBytes*1.0 / interval))
        upBytes = downBytes = 0
        count = 0
  else:
    # Use the specified (fixed) bandwidth
    assert fbwidth > 0
    bwidthVals.append(fbwidth * 1024)
        
  bwidthVals = [x * options.scale for x in bwidthVals]
  print bwidthVals

  # Print statistics about the bandwidth distribution
  if options.stats:
   print "\nLink bandwidth statistics (bytes/sec, %d-second intervals)" % (interval)
   print_dist_stats([x for x in bwidthVals])
  
  # Clear prior rules before and after running
  traffic_shape_clear(options.iface)
  traffic_shape_start(options.iface, port)

  # Run the simulation
  tCount = len(bwidthVals)
  count = 0
  # Pick a random starting point if there are multiple intervals
  if tCount > 1:
    count = random.randint(0, tCount - 1)
  print "%s: Simulating link bandwidth..." % time.ctime()
  while True:
    if (simTime >= 0) and (count * interval >= simTime):
      break
    # Never set bandwidth to less than the minimum
    bwidth = max(bwidthVals[count % tCount], mbwidth)
    if options.verbose:
      print "Setting bandwidth to %d bps" % (bwidth)
    traffic_shape(options.iface, bwidth)
    time.sleep(interval)
    count += 1

  traffic_shape_clear(options.iface)


if __name__ == '__main__':
    main()

