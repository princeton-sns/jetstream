#!/bin/bash

VICCI=`hostname | grep 'vicci'`
if [ x$VICCI != x ]; then 
LOGNAME=/jetstream/log.out
PID_DIR=/jetstream
JS_DIR=/jetstream/js/build
CFG="-C /jetstream/js/config/vicci.conf"
else
JS_DIR=`dirname $0`/..
export LD_LIBRARY_PATH=${JS_DIR}/lib
CFG="-C ${JS_DIR}/config/sns48.conf"
PID_DIR=/disk/local/asr_js_logs
LOGNAME=/disk/local/asr_js_logs/`hostname`-jslog
fi

echo "Killing any previous run"
kill `cat ${PID_DIR}/jsnode.pid`

echo "logging to ${LOGNAME}; libpath is ${LD_LIBRARY_PATH}"
CMD="${JS_DIR}/jsnoded --start ${CFG}"
echo "running ${CMD}"
nohup ${CMD} $@ >$LOGNAME 2>&1 &
PID=$!
echo $PID > ${PID_DIR}/jsnode.pid
echo "started; pid is ${PID}"
sleep 1
#cat $LOGNAME
