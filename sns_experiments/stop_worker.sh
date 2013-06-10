#!/bin/bash

PID_DIR=/disk/local/asr_js_logs
kill `cat ${PID_DIR}/jsnode.pid`

# killall jsnoded # a more extreme version
