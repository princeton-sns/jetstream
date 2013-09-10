#!/bin/bash

PID_DIR=/jetstream


if [ -f ${PID_DIR}/shaping.pid ]; then
echo "stopping previous run"
kill `cat ${PID_DIR}/shaping.pid`
fi

nohup python /jetstream/js/adapt_experiments/wifi_tc_simulate.py > /jetstream/shape.out  2>&1 $@ &
PID=$!
echo $PID > ${PID_DIR}/shaping.pid
echo "started; pid is ${PID}"
sleep 1
