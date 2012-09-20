#!/bin/bash
JS_DIR=`dirname $0`/..
export LD_LIBRARY_PATH=${JS_DIR}/lib
CFG="-C ${JS_DIR}/config/sns48.conf"
PID_DIR=/tmp

nohup ${JS_DIR}/jsnoded --start ${CFG} $@ &
PID=$!
echo $PID > ${PID_DIR}/jsnode.pid
