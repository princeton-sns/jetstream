#!/bin/bash
JS=`dirname $0`/..
./build/jsnoded -C config/vicci.conf --start 2>&1 | tee $1.nodeoutput.log 

