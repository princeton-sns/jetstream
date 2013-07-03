#!/bin/bash

VICCI=`hostname | grep 'vicci'`
if [ x$VICCI != x ]; then
PID_DIR=/jetstream
else
PID_DIR=/disk/local/asr_js_logs
fi
kill `cat ${PID_DIR}/jsnode.pid`

# killall jsnoded # a more extreme version
