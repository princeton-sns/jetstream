#!/bin/bash

nohup python /jetstream/js/adapt_experiments/wifi_tc_simulate.py > /jetstream/shape.out  2>&1 $@ &
echo "Shaping."