#!/bin/bash

FILE=$1
grep 'degradation' $FILE | tr '/' ' ' | cut -d ' ' -f 11,24 > /tmp/to_plot
gnuplot <<HERE
plot '/tmp/to_plot' using 2:1 with linespoints
HERE