#!/bin/bash
PID_DIR=/tmp
kill `cat ${PID_DIR}/jsnode.pid`

# killall jsnoded # a more extreme version
