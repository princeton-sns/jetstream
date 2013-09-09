#!/bin/bash

PID_DIR=/jetstream

kill `cat ${PID_DIR}/shaping.pid`

nohup python /jetstream/js/adapt_experiments/wifi_tc_simulate.py > /jetstream/shape.out  2>&1 $@ &
PID=$!
echo $PID > ${PID_DIR}/shaping.pid
echo "started; pid is ${PID}"
sleep 1
