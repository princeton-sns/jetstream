#!/bin/bash

nohup python /jetstream/js/adapt_experiments/wifi_tc_simulate.py  2>&1 $@ &
echo "Shaping."